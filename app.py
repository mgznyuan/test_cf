# --- Imports ---
import geopandas as gpd
import duckdb
import numpy as np
import pandas as pd
import gc
import os
import traceback
import re
# import pyarrow.parquet as pq # Needed only if using load_parquet_from_b2
import io # Required for reading bytes data into pyarrow/pandas
import json # Required for parsing GeoJSON
import boto3 # Import boto3 for B2 access
from botocore.client import Config # For B2 S3 config
from functools import wraps  # For login decorator
from flask import (Flask, render_template, request, redirect,
                     url_for, session, flash, jsonify)

# --- Flask App Initialization ---
app = Flask(__name__)

# --- Authentication Setup ---
# 1. Load Secret Key (Required for sessions)
app.secret_key = os.environ.get('FLASK_SECRET_KEY')
if not app.secret_key:
    raise ValueError("No FLASK_SECRET_KEY set for Flask application")

# 2. Load the Correct Passcode from Environment Variable
CORRECT_PASSCODE = os.environ.get('APP_PASSCODE')
if not CORRECT_PASSCODE:
    print("WARNING: No APP_PASSCODE environment variable set. Authentication will not work.")
    # Optionally raise error: raise ValueError("No APP_PASSCODE set")

# 3. Login Required Decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function


# --- B2 Configuration (Loaded from Environment Variables/Secrets) ---
B2_ENDPOINT_URL = os.environ.get('B2_ENDPOINT_URL')
B2_KEY_ID = os.environ.get('B2_KEY_ID')
B2_APP_KEY = os.environ.get('B2_APP_KEY')
B2_BUCKET_NAME = os.environ.get('B2_BUCKET_NAME')

# --- Define B2 OBJECT KEYS (Update these with your exact filenames/paths in B2) ---
GEOJSON_OBJECT_KEY = 'data_residential.geojson'
PARQUET_OBJECT_KEY = 'full_data.parquet'
# Example if in a 'data' folder:
# GEOJSON_OBJECT_KEY = 'data/data_residential.geojson'
# PARQUET_OBJECT_KEY = 'data/full_data.parquet'


# --- Global State Variables ---
# Use these to hold data loaded from B2
global_gdf = None # Will hold GeoDataFrame loaded from GeoJSON
parquet_df = None # Will hold DataFrame loaded from Parquet
# Set to track generated column names
generated_index_columns = set()
# List to track columns confirmed available in the loaded GeoJSON (updated on load and generation)
available_geojson_columns = []
# List to store columns confirmed needed by frontend AND present in GeoJSON (set on load)
verified_frontend_cols = []
# List to store JS variable names whose _zscore_o cols exist (set on load)
available_residential_vars_js = []
# Mapping from JS var names to backend base names (set on load)
variable_name_map_js_to_backend = {}
# S3 Client instance
s3_client = None

# --- Helper Functions ---
def report_memory(stage=""):
    """Simple memory reporting for global_gdf and parquet_df."""
    mem_usage_gdf = 0
    mem_usage_pq = 0
    if global_gdf is not None:
        try:
            mem_usage_gdf = global_gdf.memory_usage(index=True, deep=True).sum() / (1024**2)
        except Exception as e:
            print(f"Could not report memory usage for GDF: {e}")
    if parquet_df is not None:
        try:
            mem_usage_pq = parquet_df.memory_usage(deep=True).sum() / (1024**2)
        except Exception as e:
            print(f"Could not report memory usage for Parquet DF: {e}")
    print(f"Memory Usage ({stage}): GDF ~ {mem_usage_gdf:.2f} MB | Parquet DF ~ {mem_usage_pq:.2f} MB | Total ~ {mem_usage_gdf + mem_usage_pq:.2f} MB")


def clean_col_name(name):
    """Cleans variable names for backend use (removes spaces)."""
    return name.replace(' ', '')

def check_gdf():
    """Checks if global_gdf is loaded."""
    if global_gdf is None:
        raise RuntimeError("Base GeoDataFrame (global_gdf) not loaded. Cannot proceed.")

def get_columns_for_frontend():
    """
    Determines which columns currently exist in global_gdf and should be sent.
    """
    global global_gdf, verified_frontend_cols, generated_index_columns
    if global_gdf is None: return []

    current_gdf_cols = set(global_gdf.columns)

    # Start with essential and verified columns known to be needed
    cols_to_send = set(verified_frontend_cols)
    cols_to_send.add('Origin_tract')
    cols_to_send.add('geometry')
    cols_to_send.add('COUNTYFP')
    cols_to_send.add('population_x_o')
    cols_to_send.add('race')

    # Add any dynamically generated index columns that exist
    for idx_col in generated_index_columns:
        if idx_col in current_gdf_cols:
             cols_to_send.add(idx_col)

    # Filter against actual columns currently in the dataframe for safety
    final_cols = sorted(list(cols_to_send.intersection(current_gdf_cols)))

    # print(f"DEBUG get_columns_for_frontend: Sending columns ({len(final_cols)}): {final_cols}")
    if 'geometry' not in final_cols:
         print("CRITICAL WARNING in get_columns_for_frontend: 'geometry' column missing!")
    return final_cols

# --- S3 Client Initialization ---
def initialize_s3_client():
    """Initializes the Boto3 S3 client for B2."""
    global s3_client
    if not all([B2_ENDPOINT_URL, B2_KEY_ID, B2_APP_KEY]):
        print("WARNING: Missing one or more B2 environment variables (ENDPOINT_URL, KEY_ID, APP_KEY). S3 client not initialized.")
        s3_client = None
        return False
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=B2_ENDPOINT_URL,
            aws_access_key_id=B2_KEY_ID,
            aws_secret_access_key=B2_APP_KEY,
            config=Config(signature_version='s3v4')
        )
        print("S3 client initialized successfully for B2.")
        return True
    except Exception as e:
        print(f"Error initializing S3 client: {e}")
        s3_client = None
        return False

# --- Data Loading Functions from B2 ---
def load_geojson_from_b2():
    """Loads GeoJSON data from B2 into the global_gdf."""
    global global_gdf, s3_client, available_geojson_columns, verified_frontend_cols
    if not s3_client: return False # Check if client is initialized
    if not B2_BUCKET_NAME: print("Error: B2_BUCKET_NAME not configured."); return False

    print(f"Attempting to load GeoJSON key '{GEOJSON_OBJECT_KEY}' from B2 bucket '{B2_BUCKET_NAME}'...")
    try:
        response = s3_client.get_object(Bucket=B2_BUCKET_NAME, Key=GEOJSON_OBJECT_KEY)
        # Load directly into GeoDataFrame from bytes
        gdf_loaded = gpd.read_file(io.BytesIO(response['Body'].read()))
        print(f"Successfully loaded and parsed GeoJSON from B2 key: {GEOJSON_OBJECT_KEY}")

        # --- Process Loaded GeoDataFrame ---
        initial_available_columns = gdf_loaded.columns.tolist()
        print(f"Columns found in GeoJSON ({len(initial_available_columns)}): {initial_available_columns}")

        # Essential Column Check
        for col in ['Origin_tract', 'geometry']:
            if col not in initial_available_columns:
                print(f"FATAL ERROR: Essential column '{col}' missing from loaded GeoJSON. Cannot proceed.")
                return False # Failed to load essential data

        # Robust Origin_tract Conversion (important!)
        try:
            numeric_tracts_gdf = pd.to_numeric(gdf_loaded['Origin_tract'], errors='coerce')
            if numeric_tracts_gdf.isna().any(): print(f"WARNING: Some Origin_tract values in GeoJSON were non-numeric.")
            # Try nullable Int64 first, fallback to standard int/float if needed
            try: int_tracts_gdf = numeric_tracts_gdf.astype(pd.Int64Dtype())
            except TypeError: int_tracts_gdf = numeric_tracts_gdf.astype('float64').astype('Int64') # Fallback conversion
            gdf_loaded['Origin_tract'] = int_tracts_gdf.astype(str).str.strip()
            print(f"DEBUG: Post-load Origin_tract GDF sample (string): {gdf_loaded['Origin_tract'].head()}")
        except Exception as e_tract:
             print(f"ERROR processing Origin_tract in GeoJSON: {e_tract}"); return False

        # Verify required frontend columns
        missing_frontend_cols = []
        temp_verified_frontend_cols = []
        for col in required_frontend_cols_in_geojson: # Use the predefined list
            if col not in initial_available_columns:
                print(f"WARNING: Frontend column '{col}' NOT FOUND in GeoJSON.")
                missing_frontend_cols.append(col)
            else:
                temp_verified_frontend_cols.append(col)
        print(f"Verified frontend columns present: {temp_verified_frontend_cols}")
        if missing_frontend_cols: print(f"MISSING required frontend columns: {missing_frontend_cols}")

        # Optimize Data Types
        print("Optimizing GeoDataFrame data types...")
        for col in gdf_loaded.columns:
            if col in ['geometry', 'Origin_tract']: continue
            col_dtype = gdf_loaded[col].dtype
            try:
                if pd.api.types.is_numeric_dtype(col_dtype):
                    gdf_loaded[col] = pd.to_numeric(gdf_loaded[col], downcast='float') # Downcast float/int
                elif pd.api.types.is_object_dtype(col_dtype): # Object type
                     # Try converting to numeric if possible, otherwise ignore
                     numeric_test = pd.to_numeric(gdf_loaded[col], errors='coerce')
                     if numeric_test.notna().sum() > 0.9 * len(gdf_loaded[col]): # If mostly numeric
                          gdf_loaded[col] = numeric_test.astype('float32') # Convert to float32
                     # Can add category conversion for low-cardinality strings later if needed
            except Exception as e_opt:
                print(f"Warning: Could not optimize column '{col}': {e_opt}")
        print("GeoDataFrame optimization attempt finished.")

        # --- Assign to global variables ---
        global_gdf = gdf_loaded
        available_geojson_columns = global_gdf.columns.tolist() # Update based on final gdf
        verified_frontend_cols = temp_verified_frontend_cols # Set global list

        # Identify available variables for residential index (based on _zscore_o columns)
        global available_residential_vars_js
        temp_available_residential_vars_js = []
        print("Checking available _zscore_o columns for residential indices...")
        for js_var, backend_cleaned_name in variable_name_map_js_to_backend.items():
             zscore_col = f"{backend_cleaned_name}_zscore_o"
             if zscore_col in available_geojson_columns:
                  temp_available_residential_vars_js.append(js_var)
        available_residential_vars_js = temp_available_residential_vars_js
        print(f"JS variable names usable for residential index: {available_residential_vars_js}")

        report_memory("After GeoJSON Load")
        return True # Success

    except Exception as e:
        print(f"Error loading/processing GeoJSON data from B2 key '{GEOJSON_OBJECT_KEY}': {e}")
        traceback.print_exc()
        global_gdf = None # Ensure it's None on failure
        return False # Failure


def load_parquet_from_b2():
    """Loads Parquet data from B2 into the global parquet_df."""
    global parquet_df, s3_client
    # Requires pyarrow and pandas to be installed
    # Make sure pyarrow is imported: import pyarrow.parquet as pq
    # Make sure pandas is imported: import pandas as pd
    # Make sure io is imported: import io

    if not s3_client: return False
    if not B2_BUCKET_NAME: print("Error: B2_BUCKET_NAME not configured."); return False

    print(f"Attempting to load Parquet key '{PARQUET_OBJECT_KEY}' from B2 bucket '{B2_BUCKET_NAME}'...")
    try:
        response = s3_client.get_object(Bucket=B2_BUCKET_NAME, Key=PARQUET_OBJECT_KEY)
        buffer = io.BytesIO(response['Body'].read())
        print(f"Reading Parquet table from buffer (size: {buffer.getbuffer().nbytes} bytes)...")
        # Read using pyarrow first
        import pyarrow.parquet as pq
        parquet_table = pq.read_table(buffer)
        # Convert to Pandas DataFrame
        import pandas as pd
        parquet_df_loaded = parquet_table.to_pandas()
        print(f"Successfully loaded Parquet data from B2 key: {PARQUET_OBJECT_KEY}. Shape: {parquet_df_loaded.shape}")

        # --- Process Loaded DataFrame ---
        # Robust Origin_tract Conversion (important!)
        if 'Origin_tract' in parquet_df_loaded.columns:
             try:
                 numeric_tracts_pq = pd.to_numeric(parquet_df_loaded['Origin_tract'], errors='coerce')
                 if numeric_tracts_pq.isna().any(): print(f"WARNING: Some Origin_tract values in Parquet were non-numeric.")
                 try: int_tracts_pq = numeric_tracts_pq.astype(pd.Int64Dtype())
                 except TypeError: int_tracts_pq = numeric_tracts_pq.astype('float64').astype('Int64') # Fallback
                 parquet_df_loaded['Origin_tract'] = int_tracts_pq.astype(str).str.strip()
                 print(f"DEBUG: Post-load Origin_tract Parquet sample (string): {parquet_df_loaded['Origin_tract'].head()}")
             except Exception as e_tract_pq:
                  print(f"ERROR processing Origin_tract in Parquet: {e_tract_pq}"); return False
        else:
             print("FATAL ERROR: Parquet file missing 'Origin_tract' column.")
             return False

        # Optimize Data Types (Example for perc_visit)
        if 'perc_visit' in parquet_df_loaded.columns:
             try: parquet_df_loaded['perc_visit'] = pd.to_numeric(parquet_df_loaded['perc_visit'], downcast='float')
             except Exception as e_opt_pq: print(f"Warning: Could not optimize 'perc_visit': {e_opt_pq}")
        # Add optimization for other numeric columns (_zscore_d) if needed

        # Assign to global variable
        parquet_df = parquet_df_loaded
        report_memory("After Parquet Load")
        return True # Success

    except ImportError:
         print("Error loading Parquet: 'pyarrow' or 'pandas' library not found.")
         parquet_df = None
         return False
    except Exception as e:
        print(f"Error loading/processing Parquet data from B2 key '{PARQUET_OBJECT_KEY}': {e}")
        traceback.print_exc()
        parquet_df = None # Ensure it's None on failure
        return False # Failure

# --- Variable Definitions (Frontend Needs - Executed once at startup) ---
# Define these before loading data as they are used in checks
valid_index_variables_for_selection = sorted(list(set([
    # List all JS names used in dropdowns (match script.js)
    'no_high_school_ed', 'no_high_school_rate', 'no_car_rate', 'total_no_work_rate',
    'poverty_rate', 'renter_rate', 'total_no_ins_rate', 'sdwalk_length_m',
    'bik_length_m', 'park_area', 'sidewalk_per_cap', 'park_per_cap',
    'bike_per_cap', 'healthy_retailer', 'pharma', 'clinic', 'healthy_ret_cap',
    'pharma_cap', 'clinic_cap', 'PRE1960PCT', 'OZONE', 'PM25', 'PNPL', 'PRMP',
    'PTSDF', 'DSLPM', 'unhealthy_ret_cap', 'liq_tab_cap', 'food_retailer_cap',
])))
for var in valid_index_variables_for_selection:
     variable_name_map_js_to_backend[var] = clean_col_name(var)

required_frontend_cols_in_geojson = sorted(list(set([
    # List all GeoJSON columns the frontend might display directly
    'Origin_tract', 'geometry', 'COUNTYFP', 'population_x_o', 'race',
    'no_high_school_rate_o', 'no_car_rate_o', 'total_no_work_rate_o',
    'poverty_rate_o', 'renter_rate_o', 'total_no_ins_rate_o',
    'sdwalk_length_m_o', 'bik_length_m_o', 'park_area_o', 'sidewalk_per_cap_o',
    'park_per_cap_o', 'bike_per_cap_o', 'healthy_retailer_o', 'pharma_o', 'clinic_o',
    'healthy_ret_cap_o', 'pharma_cap_o', 'clinic_cap_o', 'unhealthy_ret_cap_o',
    'liq_tab_cap_o', 'food_retailer_cap_o',
    'PRE1960PCT_o', 'OZONE_o', 'PM25_o', 'PNPL_o', 'PRMP_o', 'PTSDF_o', 'DSLPM_o', # Original env vars
    'PRE1960PCT_zscore_o', 'OZONE_zscore_o', 'PM25_zscore_o', 'PNPL_zscore_o', # Z-score env vars (if needed for display)
    'PRMP_zscore_o', 'PTSDF_zscore_o', 'DSLPM_zscore_o', # Z-score env vars (if needed for display)
    'Obesity', 'Diabetes', 'High Blood Pressure', 'Coronary Heart Disease',
    'High Cholesterol', 'Depression', 'Stroke', 'Annual Checkup', 'Physical Inactivity',
    'ndi_o', 'uei_o', 'hoi_o'
])))


# --- Initialize S3 Client and Load Data on Startup ---
print("--- Initializing S3 Client & Loading Data ---")
if initialize_s3_client():
    # Load data only if client initialization succeeded
    geojson_loaded_ok = load_geojson_from_b2()
    parquet_loaded_ok = load_parquet_from_b2()
    if not geojson_loaded_ok or not parquet_loaded_ok:
         print("CRITICAL WARNING: Failed to load one or both essential data files on startup.")
         # Decide if the app should continue or exit/show error
else:
    print("CRITICAL WARNING: S3 Client initialization failed. Data cannot be loaded.")
print("--- Data loading attempt finished ---")


# --- Flask Routes ---

@app.route('/')
@login_required
def index():
    """Serves the main HTML page."""
    # Optionally check if data loaded before rendering
    if global_gdf is None or parquet_df is None:
         flash("Error: Essential application data failed to load.", "danger")
         # Maybe render a simple error template or redirect?
         # return render_template('error.html', message="Data Load Error"), 500
    return render_template("index.html")

# Login/Logout Routes
@app.route('/login', methods=['GET', 'POST'])
def login():
    # ... (Keep your existing login logic) ...
    if request.method == 'POST':
        entered_passcode = request.form.get('passcode')
        # Debug prints can be removed later
        print(f"--- Login Attempt ---"); print(f"Entered: {repr(entered_passcode)}"); print(f"Expected: {repr(CORRECT_PASSCODE)}")
        if entered_passcode and entered_passcode == CORRECT_PASSCODE:
            session['logged_in'] = True; session.permanent = True
            flash('You were successfully logged in!', 'success'); next_page = request.args.get('next')
            return redirect(next_page or url_for('index'))
        else:
            flash('Invalid passcode. Please try again.', 'danger')
    return render_template('login.html')

@app.route('/logout')
def logout():
    # ... (Keep your existing logout logic) ...
    session.pop('logged_in', None)
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))


@app.route('/geojson')
@login_required
def geojson():
    """Serves the current GeoJSON data needed by the frontend."""
    global global_gdf
    print("Request received for /geojson")
    if global_gdf is None:
         print("Error in /geojson: global_gdf is None.")
         return jsonify({"error": "Map data not loaded on server."}), 503 # Service Unavailable

    try:
        cols_to_send = get_columns_for_frontend()
        # print(f"Sending GeoJSON with columns ({len(cols_to_send)}): {cols_to_send}") # Verbose log

        if 'geometry' not in cols_to_send:
             print("ERROR: No 'geometry' column found in columns to send.")
             return jsonify({"error": "Internal error preparing map data (geometry missing)."}), 500

        # Create a VIEW (or copy() if mutations elsewhere are problematic) for sending
        # Using a view is generally more memory efficient if gdf isn't modified during send
        gdf_to_send = global_gdf[cols_to_send]

        # Convert to GeoJSON interface and return
        # Ensure CRS is set correctly before conversion if necessary (B2 loaded data might lose CRS)
        if gdf_to_send.crs is None:
             print("Warning: CRS not set on gdf_to_send, assuming EPSG:4326 for GeoJSON output.")
             # If you know the original CRS, set it here: gdf_to_send.set_crs(epsg=YOUR_ORIGINAL_EPSG, inplace=True)
        # Convert to WGS84 (EPSG:4326) which is standard for GeoJSON
        geo_interface = gdf_to_send.to_crs(epsg=4326).__geo_interface__
        print("GeoJSON conversion successful, sending response.")
        return jsonify(geo_interface)

    except Exception as e:
        print(f"Error in /geojson: {e}")
        traceback.print_exc()
        return jsonify({"error": f"Error preparing GeoJSON for display: {str(e)}"}), 500

@app.route('/get_index_fields')
@login_required
def get_index_fields():
    """Returns the list of variables selectable for index creation."""
    # Return the JS variable names defined at the top
    return jsonify(valid_index_variables_for_selection)


@app.route('/generate_index', methods=['POST'])
@login_required
def generate_activity_index():
    """Generates an Activity Space Index using Parquet data and merges into global_gdf."""
    global global_gdf, parquet_df, generated_index_columns, available_geojson_columns
    print("\n--- Received request for /generate_index (Activity) ---")

    if global_gdf is None or parquet_df is None:
        error_msg = "Required data not loaded:"
        if global_gdf is None: error_msg += " GeoJSON"
        if parquet_df is None: error_msg += " Parquet"
        return jsonify({"error": error_msg}), 503

    # --- Extract and Validate Inputs ---
    data = request.get_json()
    if not data: return jsonify({"error": "Invalid request data."}), 400
    base_name_from_user = data.get('name', '').strip()
    selected_vars_js = data.get('variables', [])
    if not base_name_from_user: return jsonify({"error": "Index name required."}), 400
    if not selected_vars_js: return jsonify({"error": "No variables selected."}), 400

    # Clean name
    cleaned_base_name = re.sub(r'[^\w_]', '', re.sub(r'\s+', '_', base_name_from_user))
    if not cleaned_base_name: return jsonify({"error": "Invalid index name after cleaning."}), 400
    index_col_name = f"{cleaned_base_name}_ACT"
    print(f"Generating Activity Index: '{index_col_name}'")

    # Map JS names to backend names and identify required Parquet columns (_zscore_d)
    selected_vars_backend, invalid_vars_received, required_zscore_d_cols_for_request = [], [], []
    for var_js in selected_vars_js:
        backend_name = variable_name_map_js_to_backend.get(var_js)
        if backend_name:
            selected_vars_backend.append(backend_name)
            # Activity index uses _zscore_d columns from the PARQUET file
            required_zscore_d_cols_for_request.append(f"{backend_name}_zscore_d")
        else: invalid_vars_received.append(var_js)
    if invalid_vars_received: print(f"WARNING: Ignoring unknown variables: {invalid_vars_received}")
    if not selected_vars_backend: return jsonify({"error": "No valid variables selected."}), 400
    print(f"Required _zscore_d columns from Parquet: {required_zscore_d_cols_for_request}")

    # Check if required columns exist in the loaded parquet_df
    missing_parquet_cols = [col for col in required_zscore_d_cols_for_request if col not in parquet_df.columns]
    if missing_parquet_cols:
        print(f"ERROR: Required columns missing from Parquet data: {missing_parquet_cols}")
        return jsonify({"error": f"Required data columns missing from source: {', '.join(missing_parquet_cols)}"}), 400
    if 'perc_visit' not in parquet_df.columns:
         return jsonify({"error": f"Required data column 'perc_visit' missing from source."}), 400

    # --- DuckDB Query for Weighted Sum ---
    # DuckDB can query Pandas DataFrames directly in memory
    con = None
    index_by_tract_df = pd.DataFrame()
    try:
        print("Connecting to in-memory DuckDB to query Parquet DataFrame...")
        con = duckdb.connect(database=':memory:', read_only=False)

        # Create SQL expression for the weighted sum
        weighted_sum_expr_parts = [f'"{col}"::DOUBLE * "perc_visit"::DOUBLE' for col in required_zscore_d_cols_for_request]
        weighted_sum_sql = " + ".join(weighted_sum_expr_parts)
        if not weighted_sum_sql: return jsonify({"error": "Internal error: Failed to build query sum expression."}), 500

        # Ensure Origin_tract is string for grouping
        # Note: DuckDB might handle mixed types, but explicit check is safer
        if not pd.api.types.is_string_dtype(parquet_df['Origin_tract']):
             print("Warning: Converting Parquet Origin_tract to string for DuckDB query.")
             parquet_df['Origin_tract'] = parquet_df['Origin_tract'].astype(str).str.strip()

        # Build and execute the query using the DataFrame `parquet_df` as the source table
        query = f"""
            SELECT
                "Origin_tract",
                SUM({weighted_sum_sql}) AS total_weighted_sum
            FROM parquet_df -- Query the pandas DataFrame directly!
            WHERE "perc_visit" IS NOT NULL AND "perc_visit" != 0
            GROUP BY "Origin_tract"
        """
        # print(f"Executing DuckDB Query:\n{query}") # Can be verbose
        index_by_tract_df = con.execute(query).fetchdf() # fetchdf returns a pandas DataFrame
        print(f"DuckDB query returned {len(index_by_tract_df)} aggregated rows.")

    except Exception as e:
        print(f"ERROR: DuckDB query failed: {e}"); traceback.print_exc()
        # Attempt to provide a more specific error
        err_msg = f"Data query failed during aggregation. Error: {e}"
        # Add more specific error checks if needed (e.g., column not found)
        return jsonify({"error": err_msg}), 500
    finally:
        if con: con.close()

    # --- Process Results & Prepare for Merge ---
    if index_by_tract_df.empty:
        print(f"WARNING: DuckDB query returned no results for activity index.")
        # Create empty df with correct columns/types to avoid merge errors
        index_by_tract_df = pd.DataFrame({'Origin_tract': pd.Series(dtype='object'), 'total_weighted_sum': pd.Series(dtype='float64')})

    # Robust Origin_tract Conversion (DuckDB might return different types)
    if 'Origin_tract' in index_by_tract_df.columns:
        try:
            numeric_tracts = pd.to_numeric(index_by_tract_df['Origin_tract'], errors='coerce')
            if numeric_tracts.isna().any(): print(f"WARNING: Some Origin_tracts from DuckDB were non-numeric.")
            try: int_tracts = numeric_tracts.astype(pd.Int64Dtype())
            except TypeError: int_tracts = numeric_tracts.astype('float64').astype('Int64')
            index_by_tract_df["Origin_tract"] = int_tracts.astype(str).str.strip()
        except Exception as e_tract_agg: print(f"ERROR processing Origin_tract from aggregation: {e_tract_agg}"); return jsonify({"error": "Failed post-processing tract IDs."}), 500
    else: return jsonify({"error": "Internal error: Origin_tract missing post-aggregation."}), 500

    # Calculate the final index value (Average weighted sum * 100)
    num_vars = len(selected_vars_backend)
    if num_vars > 0 and 'total_weighted_sum' in index_by_tract_df.columns:
        total_sum = pd.to_numeric(index_by_tract_df["total_weighted_sum"], errors='coerce').replace([np.inf, -np.inf], np.nan)
        index_by_tract_df[index_col_name] = (total_sum / num_vars * 100.0).astype('float32') # Use float32 for memory
    else:
        index_by_tract_df[index_col_name] = np.nan
        print(f"WARNING: Calculation for '{index_col_name}' resulted in NaN (num_vars={num_vars}).")

    # Select columns for merge ('Origin_tract' and the new index column)
    index_to_merge = index_by_tract_df[["Origin_tract", index_col_name]].copy()

    # --- Merge into global_gdf ---
    try:
        print(f"Merging '{index_col_name}' into global_gdf...")
        # Drop existing column from global_gdf if necessary
        if index_col_name in global_gdf.columns:
            print(f"  Dropping existing column '{index_col_name}' from global_gdf.")
            global_gdf = global_gdf.drop(columns=[index_col_name])
            # Also remove from state tracking lists if present
            if index_col_name in available_geojson_columns: available_geojson_columns.remove(index_col_name)
            if index_col_name in generated_index_columns: generated_index_columns.remove(index_col_name)

        # Ensure merge keys are compatible strings
        if not pd.api.types.is_string_dtype(global_gdf['Origin_tract']):
             print("Warning: Converting global_gdf Origin_tract to string before merge.")
             global_gdf['Origin_tract'] = global_gdf['Origin_tract'].astype(str).str.strip()
        if not pd.api.types.is_string_dtype(index_to_merge['Origin_tract']):
             print("Warning: Converting index_to_merge Origin_tract to string before merge.")
             index_to_merge['Origin_tract'] = index_to_merge['Origin_tract'].astype(str).str.strip()

        # Perform the merge
        original_rows = len(global_gdf)
        global_gdf = global_gdf.merge(index_to_merge, on="Origin_tract", how="left")

        # Validation checks
        if len(global_gdf) != original_rows: print(f"WARNING: Row count changed after merge! {original_rows} -> {len(global_gdf)}")
        if index_col_name not in global_gdf.columns: raise ValueError(f"Column '{index_col_name}' missing after merge.")
        merged_nan_count = global_gdf[index_col_name].isna().sum()
        print(f"Merge complete for '{index_col_name}'. NaN count: {merged_nan_count} / {len(global_gdf)}")
        if merged_nan_count == len(global_gdf): print(f"WARNING: All values for '{index_col_name}' are NaN after merge. Check key matching.")
        print(f"Sample values post-merge:\n{global_gdf[index_col_name].head()}")

    except Exception as e_merge:
        print(f"ERROR: Failed during merge: {e_merge}"); traceback.print_exc()
        return jsonify({"error": f"Failed to merge index results: {e_merge}"}), 500

    # --- Update State Tracking Variables ---
    generated_index_columns.add(index_col_name)
    if index_col_name not in available_geojson_columns: available_geojson_columns.append(index_col_name)
    report_memory(f"After generating {index_col_name}")
    del index_by_tract_df, index_to_merge; gc.collect() # Cleanup

    # --- Return FULL UPDATED GDF Slice ---
    try:
        cols_to_send = get_columns_for_frontend()
        if not cols_to_send: return jsonify({"error": "Internal error selecting columns for response."}), 500
        gdf_to_send = global_gdf[cols_to_send].copy() # Send a copy
        print(f"DEBUG Before Send (Activity): Final sample values of '{index_col_name}':\n{gdf_to_send[index_col_name].head()}")
        return jsonify(gdf_to_send.to_crs(epsg=4326).__geo_interface__)
    except Exception as e_final:
        print(f"ERROR: Failed during final GeoJSON conversion/send: {e_final}"); traceback.print_exc()
        return jsonify({"error": f"Failed to format final data: {e_final}"}), 500


# --- Generate Residential Index ---
@app.route('/generate_residential_index', methods=['POST'])
@login_required
def generate_residential_index():
    """Generates a Residential Index using _zscore_o columns from global_gdf."""
    global global_gdf, generated_index_columns, available_geojson_columns
    print("\n--- Received request for /generate_residential_index ---")

    if global_gdf is None: return jsonify({"error": "Map data not loaded."}), 503

    # --- Extract and Validate Inputs ---
    data = request.get_json()
    if not data: return jsonify({"error": "Invalid request."}), 400
    base_name_from_user = data.get('name', '').strip()
    selected_vars_js = data.get('variables', []) # Names as sent by JS
    if not base_name_from_user: return jsonify({"error": "Index name required."}), 400
    if not selected_vars_js: return jsonify({"error": "No variables selected."}), 400

    # Clean user name
    cleaned_base_name = re.sub(r'[^\w_]', '', re.sub(r'\s+', '_', base_name_from_user))
    if not cleaned_base_name: return jsonify({"error": "Invalid index name."}), 400
    index_col_name = f"{cleaned_base_name}_RES"
    print(f"Generating Residential Index: '{index_col_name}'")

    # --- Validate selected variables against AVAILABLE residential vars (_zscore_o columns) ---
    required_zscore_o_cols = []; invalid_vars_for_residential = []
    for var_js in selected_vars_js:
        backend_name = variable_name_map_js_to_backend.get(var_js)
        if not backend_name: print(f"WARNING: Ignoring unknown variable '{var_js}'"); continue
        # Residential index uses _zscore_o columns from the GeoJSON/GDF
        zscore_col = f"{backend_name}_zscore_o"
        if zscore_col in available_geojson_columns: # Check against currently available columns in GDF
             required_zscore_o_cols.append(zscore_col)
        else:
             print(f"WARNING: Required column '{zscore_col}' for variable '{var_js}' not found in current GDF.")
             invalid_vars_for_residential.append(var_js)

    if not required_zscore_o_cols:
        error_msg = "None of the selected variables have required data (_zscore_o columns) available in the map data."
        if invalid_vars_for_residential: error_msg += f" (Missing data for: {', '.join(invalid_vars_for_residential)})"
        return jsonify({"error": error_msg}), 400
    if invalid_vars_for_residential: print(f"WARNING: Calculating residential index '{index_col_name}' skipping unavailable variables: {', '.join(invalid_vars_for_residential)}")
    print(f"Using GeoJSON columns: {required_zscore_o_cols}")

    # --- Calculate Index (Average of existing _zscore_o columns * 100) ---
    try:
        # Select only the required columns that actually exist in the current GDF
        cols_to_average = [col for col in required_zscore_o_cols if col in global_gdf.columns]
        if not cols_to_average: # Should not happen if previous check passed, but safety check
             return jsonify({"error": "No valid columns found in GeoDataFrame for averaging."}), 500

        print(f"Calculating mean for columns: {cols_to_average}")
        index_values = global_gdf[cols_to_average].mean(axis=1, skipna=True).astype('float32') * 100.0

        # --- Add/Update column in global_gdf & state lists ---
        if index_col_name in global_gdf.columns:
            print(f"Dropping existing column '{index_col_name}'.")
            global_gdf = global_gdf.drop(columns=[index_col_name])
            if index_col_name in available_geojson_columns: available_geojson_columns.remove(index_col_name)
            if index_col_name in generated_index_columns: generated_index_columns.remove(index_col_name)

        global_gdf[index_col_name] = index_values
        generated_index_columns.add(index_col_name)
        if index_col_name not in available_geojson_columns: available_geojson_columns.append(index_col_name)

        print(f"Added/Updated residential index '{index_col_name}'. Dtype: {global_gdf[index_col_name].dtype}, NaN Count: {global_gdf[index_col_name].isna().sum()}")
        report_memory(f"After generating {index_col_name}")

    except Exception as e_calc:
        print(f"ERROR during residential index calculation: {e_calc}"); traceback.print_exc()
        return jsonify({"error": f"Calculation failed: {e_calc}"}), 500

    # --- Return FULL UPDATED GDF Slice ---
    try:
        cols_to_send = get_columns_for_frontend()
        print(f"Returning updated GDF slice ({len(cols_to_send)} cols)")
        if not cols_to_send or 'geometry' not in cols_to_send: return jsonify({"error": "Internal error selecting columns for response."}), 500

        gdf_to_send = global_gdf[cols_to_send].copy() # Send a copy
        print(f"DEBUG Before Send (Residential): Final sample values of '{index_col_name}':\n{gdf_to_send[index_col_name].head()}")

        # Ensure CRS before converting
        if gdf_to_send.crs is None:
             print("Warning: Setting CRS to EPSG:4326 before GeoJSON conversion.")
             gdf_to_send.set_crs(epsg=4326, inplace=True)
        return jsonify(gdf_to_send.to_crs(epsg=4326).__geo_interface__)

    except Exception as e_final:
        print(f"ERROR: Failed during final GeoJSON conversion/send for residential: {e_final}"); traceback.print_exc()
        return jsonify({"error": f"Failed to format final data for residential: {e_final}"}), 500

# --- Main Execution Block ---
# (Keep if __name__ == '__main__': block for local development)
if __name__ == '__main__':
    print("Starting Flask Application for local development...")
    # threaded=False can help debug state issues, but might impact performance
    app.run(debug=True, threaded=False)