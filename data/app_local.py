# --- Imports ---
import geopandas as gpd
import duckdb
import numpy as np
import pandas as pd
import gc
import os
import traceback
import re
import pyarrow.parquet as pq # Used only for optional schema inspection
import boto3 # Import boto3
from functools import wraps  # <--- Auth
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
    # You might want to raise an error or log a serious warning
    print("WARNING: No APP_PASSCODE environment variable set. Authentication will not work.")
    # raise ValueError("No APP_PASSCODE set for Flask application") # Optionally raise error

# 3. Login Required Decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function


# # --- Global Configuration & State ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GEOJSON_PATH = os.path.join(BASE_DIR, "data", "data_residential.geojson")
PARQUET_PATH = os.path.join(BASE_DIR, "data", "full_data.parquet")


# Global DataFrame - Modified by index generation routes
global_gdf = None
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

# --- Helper Functions ---
def report_memory(stage=""):
    """Simple memory reporting for global_gdf."""
    if global_gdf is not None:
        try:
            mem_usage = global_gdf.memory_usage(index=True, deep=True).sum() / (1024**2)
            print(f"Memory Usage ({stage}): global_gdf ~ {mem_usage:.2f} MB")
        except Exception as e:
            print(f"Could not report memory usage: {e}")

def clean_col_name(name):
    """Cleans variable names for backend use (removes spaces)."""
    return name.replace(' ', '')

def check_gdf():
    """Checks if global_gdf is loaded."""
    if global_gdf is None:
        raise RuntimeError("Base GeoDataFrame not loaded. Cannot proceed.")

def get_columns_for_frontend():
    """
    Determines which columns currently exist in global_gdf and are needed
    by the frontend (based on initial load + generated indices).
    """
    if global_gdf is None: return []
    # Start with columns verified during load that frontend requires
    cols_to_send = list(verified_frontend_cols)

    # Add any dynamically generated index columns that exist
    for idx_col in generated_index_columns:
        # Check against runtime list of available columns in gdf
        if idx_col in global_gdf.columns and idx_col not in cols_to_send:
             cols_to_send.append(idx_col)

    # Ensure essential identifiers and geometry are included
    for essential in ['Origin_tract', 'geometry']:
        # Check against runtime list of available columns in gdf
        if essential in global_gdf.columns and essential not in cols_to_send:
             cols_to_send.append(essential)

    # Final filter against actual columns currently in the dataframe
    final_cols = sorted(list(set([c for c in cols_to_send if c in global_gdf.columns])))
    print(f"DEBUG get_columns_for_frontend: Final columns selected: {final_cols}")
    return final_cols

# --- Variable Definitions (Executed once at startup) ---
# These lists define the expected variables and map frontend names to backend base names.
valid_index_variables_for_selection = sorted(list(set([
    'no_high_school_ed', 'no_high_school_rate', 'no_car_rate', 'total_no_work_rate',
    'poverty_rate', 'renter_rate', 'total_no_ins_rate', 'sdwalk_length_m',
    'bik_length_m', 'park_area', 'sidewalk_per_cap', 'park_per_cap',
    'bike_per_cap', 'healthy_retailer', 'pharma', 'clinic', 'healthy_ret_cap',
    'pharma_cap', 'clinic_cap', 'PRE1960PCT', 'OZONE', 'PM25', 'PNPL', 'PRMP',
    'PTSDF', 'DSLPM', 'unhealthy_ret_cap', 'liq_tab_cap',
    'food_retailer_cap',
])))
print(f"Variables available for index selection dropdown: {valid_index_variables_for_selection}")

for var in valid_index_variables_for_selection:
     variable_name_map_js_to_backend[var] = clean_col_name(var)
print(f"Mapping JS variable names to backend column base names: {variable_name_map_js_to_backend}")
backend_cleaned_variable_names = list(variable_name_map_js_to_backend.values())

required_frontend_cols_in_geojson = sorted(list(set([
    'Origin_tract', 'geometry', 'COUNTYFP', 'population_x_o', 'race',
    'no_high_school_rate_o', 'no_car_rate_o', 'total_no_work_rate_o',
    'poverty_rate_o', 'renter_rate_o', 'total_no_ins_rate_o',
    'sdwalk_length_m_o', 'bik_length_m_o', 'park_area_o', 'sidewalk_per_cap_o',
    'park_per_cap_o', 'bike_per_cap_o', 'healthy_retailer_o', 'pharma_o', 'clinic_o',
    'healthy_ret_cap_o', 'pharma_cap_o', 'clinic_cap_o', 'unhealthy_ret_cap_o',
    'liq_tab_cap_o', 'food_retailer_cap_o', 'PRE1960PCT_zscore_o', 'OZONE_zscore_o',
    'PM25_zscore_o', 'PNPL_zscore_o', 'PRMP_zscore_o', 'PTSDF_zscore_o',
    'DSLPM_zscore_o', 'Obesity', 'Diabetes', 'High Blood Pressure',
    'Coronary Heart Disease', 'High Cholesterol', 'Depression', 'Stroke',
    'Annual Checkup', 'Physical Inactivity', 'ndi_o', 'uei_o', 'hoi_o',
    'no_high_school_ed_o','PRE1960PCT_o',
    'DSLPM_o','OZONE_o','PM25_o','PNPL_o','PRMP_o','PTSDF_o'
])))
print(f"Columns REQUIRED by Frontend (to be verified in GeoJSON): {required_frontend_cols_in_geojson}")

# --- Initial Data Loading & Processing (Executed once at startup) ---
try:
    print(f"Attempting to load GeoJSON from: {GEOJSON_PATH}")
    gdf_loaded = gpd.read_file(GEOJSON_PATH)
    print("Base GeoJSON loaded successfully.")
    # Store initially available columns
    initial_available_columns = gdf_loaded.columns.tolist()
    print(f"Columns found in GeoJSON ({len(initial_available_columns)}): {initial_available_columns}")

    # --- Essential Column Check & Initial Conversion ---
    essential_cols = ['Origin_tract', 'geometry']
    for col in essential_cols:
        if col not in initial_available_columns:
            raise ValueError(f"Essential column '{col}' missing from GeoJSON '{GEOJSON_PATH}'. Cannot proceed.")

    # Perform initial robust conversion for Origin_tract here
    print(f"DEBUG Initial Load: Applying robust string conversion to loaded['Origin_tract'] (dtype: {gdf_loaded['Origin_tract'].dtype})...")
    numeric_tracts_gdf = pd.to_numeric(gdf_loaded['Origin_tract'], errors='coerce')
    if numeric_tracts_gdf.isna().any(): print(f"WARNING Initial Load: Some Origin_tract values in loaded GeoJSON were non-numeric.")
    try: int_tracts_gdf = numeric_tracts_gdf.astype(pd.Int64Dtype())
    except TypeError:
        print("WARNING Initial Load: Cannot use nullable Int64Dtype. Falling back.")
        placeholder = 0; int_tracts_gdf = numeric_tracts_gdf.fillna(placeholder).astype(np.int64)
    gdf_loaded['Origin_tract'] = int_tracts_gdf.astype(str).str.strip()
    print(f"DEBUG Initial Load: Final Origin_tract sample (string):\n{gdf_loaded['Origin_tract'].head()}")

    # --- Verify Required Frontend Columns Exist ---
    missing_frontend_cols = []
    temp_verified_frontend_cols = []
    for col in required_frontend_cols_in_geojson:
        if col not in initial_available_columns:
            print(f"WARNING Initial Load: Frontend column '{col}' NOT FOUND in GeoJSON.")
            missing_frontend_cols.append(col)
        else:
             temp_verified_frontend_cols.append(col)
    print(f"Verified frontend columns present in GeoJSON: {temp_verified_frontend_cols}")
    if missing_frontend_cols: print(f"MISSING required frontend columns: {missing_frontend_cols}")

    # --- Identify AVAILABLE variables for RESIDENTIAL index ---
    available_residential_zscore_cols = []
    temp_available_residential_vars_js = []
    print("Checking available _zscore_o columns for residential indices...")
    for js_var, backend_cleaned_name in variable_name_map_js_to_backend.items():
        zscore_col = f"{backend_cleaned_name}_zscore_o"
        if zscore_col in initial_available_columns:
            temp_available_residential_vars_js.append(js_var)
            available_residential_zscore_cols.append(zscore_col) # Keep track of actual zscore cols
    print(f"Found {len(available_residential_zscore_cols)} _zscore_o columns usable.")
    print(f"Corresponding JS variable names usable for residential index: {temp_available_residential_vars_js}")


    # --- Optimize Data Types ---
    print("Optimizing data types...")
    for col in gdf_loaded.columns:
        if col in ['geometry', 'Origin_tract']: continue # Skip already processed

        col_dtype = gdf_loaded[col].dtype
        if col_dtype.kind == 'f': # Float
            if col_dtype == np.float64:
                 try: gdf_loaded[col] = pd.to_numeric(gdf_loaded[col], errors='coerce').astype(np.float32)
                 except Exception as e: print(f"Warn: Could not convert float {col} to float32: {e}")
        elif col_dtype.kind == 'i': # Integer
             if col_dtype == np.int64:
                 try:
                     if gdf_loaded[col].isna().any():
                          gdf_loaded[col] = pd.to_numeric(gdf_loaded[col], errors='coerce').astype(np.float32)
                     else:
                          min_val, max_val = gdf_loaded[col].min(), gdf_loaded[col].max()
                          if min_val >= np.iinfo(np.int32).min and max_val <= np.iinfo(np.int32).max:
                              gdf_loaded[col] = gdf_loaded[col].astype(np.int32)
                 except Exception as e: print(f"Warn: Could not convert int {col} to int32/float32: {e}")
        elif col_dtype.kind == 'O': # Object
             try:
                  # Only convert if it looks very likely numeric
                  numeric_test = pd.to_numeric(gdf_loaded[col], errors='coerce')
                  if numeric_test.notna().sum() > 0.95 * len(gdf_loaded[col]) and not numeric_test.isna().all():
                       print(f"Attempting conversion of object column '{col}' to float32.")
                       gdf_loaded[col] = numeric_test.astype(np.float32)
             except Exception as e: pass # Ignore object conversion errors

    print("Data types optimization attempt finished.")

    # --- Assign to global variables *after* all processing ---
    global_gdf = gdf_loaded
    verified_frontend_cols = temp_verified_frontend_cols # Set global list
    available_residential_vars_js = temp_available_residential_vars_js # Set global list
    available_geojson_columns = global_gdf.columns.tolist() # Set based on final gdf

    report_memory("Initial Load")
    print("Initial data load and processing complete.")

# Handle potential loading errors
except FileNotFoundError: print(f"FATAL ERROR: GeoJSON file not found at {GEOJSON_PATH}"); global_gdf = None
except ValueError as e: print(f"FATAL ERROR: {e}"); global_gdf = None
except Exception as e: print(f"FATAL ERROR: Failed to load/process base GeoJSON: {e}"); traceback.print_exc(); global_gdf = None
# --- End Initial Load ---


# --- Flask Routes ---

@app.route('/')
@login_required 
def index():
    """Serves the main HTML page."""
    return render_template("index.html")

@app.route('/geojson')
@login_required # Apply the decorator
def geojson():
    """Serves the current GeoJSON data needed by the frontend."""
    global global_gdf
    print("🔁 /geojson called")
    if global_gdf is None:
         print("Error in /geojson: global_gdf is None.")
         return jsonify({"error": "Map data not loaded on server. Check server logs."}), 500
    try:
        cols_to_send = get_columns_for_frontend()
        print(f"Sending GeoJSON with columns ({len(cols_to_send)}): {cols_to_send}")

        if not cols_to_send or 'geometry' not in cols_to_send:
             print("ERROR: No columns or no geometry column selected to send to frontend.")
             return jsonify({"error": "Internal error preparing GeoJSON data for display."}), 500

        # Create a view (or copy if needed later) for sending
        gdf_to_send = global_gdf[cols_to_send]

        # Convert to GeoJSON interface and return
        return jsonify(gdf_to_send.to_crs(epsg=4326).__geo_interface__)

    except Exception as e:
        print(f"Error in /geojson: {e}")
        traceback.print_exc()
        return jsonify({"error": f"Error preparing GeoJSON for display: {str(e)}"}), 500

@app.route('/get_index_fields')
@login_required # Apply the decorator
def get_index_fields():
    """Returns the list of variables selectable for index creation."""
    return jsonify(valid_index_variables_for_selection)


@app.route('/generate_index', methods=['POST'])
@login_required # Apply the decorator
def generate_index():
    """
    Generates an Activity Space Index. Includes detailed debugging for merge/slice issues.
    """
    # Modifies global state: global_gdf, generated_index_columns, available_geojson_columns
    global global_gdf, generated_index_columns, available_geojson_columns, verified_frontend_cols

    if global_gdf is None:
        print("Error in /generate_index: global_gdf is None.")
        return jsonify({"error": "Map data not loaded on server. Check server logs."}), 500

    # --- Optional: Parquet Schema Inspection ---
    try:
        if os.path.exists(PARQUET_PATH):
            print(f"\n--- Inspecting Parquet Schema: {PARQUET_PATH} ---")
            schema = pq.read_schema(PARQUET_PATH); parquet_columns = schema.names
            print(f"Columns found by PyArrow: {parquet_columns}\n")
        else: print(f"WARNING: Parquet file not found at {PARQUET_PATH} for schema inspection.")
    except Exception as e: print(f"WARNING: Could not read Parquet schema: {e}")

    try:
        check_gdf() # Check if base GeoDataFrame is loaded
        data = request.get_json()
        if not data: return jsonify({"error": "Invalid request data."}), 400

        # --- Extract and Validate Inputs ---
        base_name_from_user = data.get('name', '').strip()
        selected_vars_js = data.get('variables', [])
        if not base_name_from_user: return jsonify({"error": "Index name required."}), 400
        if not selected_vars_js: return jsonify({"error": "No variables selected."}), 400

        # --- Clean names and track required columns ---
        cleaned_base_name = re.sub(r'\s+', '_', base_name_from_user)
        cleaned_base_name = re.sub(r'[^\w_]', '', cleaned_base_name)
        if not cleaned_base_name: return jsonify({"error": "Invalid index name."}), 400
        index_col_name = f"{cleaned_base_name}_ACT"
        print(f"DEBUG: Final index column name will be: '{index_col_name}'")

        selected_vars_backend, invalid_vars_received, required_zscore_d_cols_for_request = [], [], []
        print(f"Processing selection from frontend: {selected_vars_js}")
        for var_js in selected_vars_js:
            backend_name = variable_name_map_js_to_backend.get(var_js)
            if backend_name:
                selected_vars_backend.append(backend_name)
                required_zscore_d_cols_for_request.append(f"{backend_name}_zscore_d")
            else: invalid_vars_received.append(var_js)
        if invalid_vars_received: print(f"WARNING: Ignoring unknown variables: {invalid_vars_received}")
        if not selected_vars_backend: return jsonify({"error": "No valid variables selected."}), 400
        print(f"Backend base variables: {selected_vars_backend}; Required z-score cols: {required_zscore_d_cols_for_request}")

        # --- Main DuckDB Query for Index Calculation ---
        print(f"\nGenerating Activity Index '{index_col_name}' (Main Query)")
        quoted_zscore_cols_d = [f'"{col}"' for col in required_zscore_d_cols_for_request]
        quoted_origin_tract = '"Origin_tract"'; quoted_perc_visit = '"perc_visit"'
        weighted_sum_expr_parts = [f'{q_col}::DOUBLE * {quoted_perc_visit}::DOUBLE' for q_col in quoted_zscore_cols_d]
        weighted_sum_sql = " + ".join(weighted_sum_expr_parts)
        if not weighted_sum_sql: return jsonify({"error": "Internal error: query sum."}), 500

        query = f"""SELECT {quoted_origin_tract}, SUM({weighted_sum_sql}) AS total_weighted_sum
                    FROM read_parquet('{PARQUET_PATH}') WHERE {quoted_perc_visit} IS NOT NULL AND {quoted_perc_visit} != 0
                    GROUP BY {quoted_origin_tract}"""
        print("--- DuckDB Query ---")

        # --- Execute Main Query ---
        con = None; index_by_tract_df = pd.DataFrame()
        try:
            con = duckdb.connect(database=':memory:', read_only=False)
            index_by_tract_df = con.execute(query).fetchdf()
            print(f"DuckDB main query returned {len(index_by_tract_df)} rows.")
        except Exception as e: # Catch DuckDB errors during query
            print(f"ERROR: DuckDB main query failed: {e}"); traceback.print_exc()
            # Try to give a more specific error message
            err_msg = f"Data query failed during aggregation. Check data types/values in Parquet. Error: {e}"
            missing_col_match = re.search(r'(column|Columns \["?)?([^"]*)"? (does not exist|not found|conversion failed)', str(e), re.IGNORECASE)
            if missing_col_match: err_msg = f"Data query failed. Column '{missing_col_match.group(2)}' likely missing or incompatible in Parquet."
            elif isinstance(e, duckdb.IOException): err_msg = f"Could not read data file '{PARQUET_PATH}'. Check path/permissions."
            return jsonify({"error": err_msg}), 500
        finally:
            if con: con.close()

        # --- Process Results & Prepare for Merge ---
        if index_by_tract_df.empty:
             print(f"WARNING: Main activity index query returned no results...")
             index_by_tract_df = pd.DataFrame({'Origin_tract': pd.Series(dtype='object'), 'total_weighted_sum': pd.Series(dtype='float')})

        # --- ROBUST Origin_tract Conversion (Float -> Int -> Str) ---
        if 'Origin_tract' in index_by_tract_df.columns:
            print(f"DEBUG: Applying robust conversion to index_by_tract_df['Origin_tract']...")
            numeric_tracts = pd.to_numeric(index_by_tract_df['Origin_tract'], errors='coerce')
            if numeric_tracts.isna().any(): print(f"WARNING: Some Origin_tracts from DuckDB were non-numeric.")
            try: int_tracts = numeric_tracts.astype(pd.Int64Dtype())
            except TypeError:
                 print("WARNING: Nullable Int64Dtype unavailable. Falling back."); placeholder = 0
                 int_tracts = numeric_tracts.fillna(placeholder).astype(np.int64)
            index_by_tract_df["Origin_tract"] = int_tracts.astype(str).str.strip()
            print(f"DEBUG: Final index_by_tract_df['Origin_tract'] sample (string):\n{index_by_tract_df['Origin_tract'].head()}")
        else: return jsonify({"error": "Internal error: Origin_tract missing."}), 500

        # --- Calculate the final index value ---
        num_vars = len(selected_vars_backend)
        if num_vars > 0 and 'total_weighted_sum' in index_by_tract_df.columns:
             total_sum = pd.to_numeric(index_by_tract_df["total_weighted_sum"], errors='coerce').replace([np.inf, -np.inf], np.nan)
             valid_mask = total_sum.notna() & (num_vars > 0)
             index_by_tract_df[index_col_name] = np.nan
             index_by_tract_df.loc[valid_mask, index_col_name] = (total_sum[valid_mask] / num_vars * 100.0).astype(np.float32)
             # Log calculation result inspection
             print(f"\n--- Inspecting calculated index values for '{index_col_name}' (dtype: {index_by_tract_df[index_col_name].dtype}) ---")
             print(index_by_tract_df[[index_col_name]].head())
             print(f"NaN count: {index_by_tract_df[index_col_name].isna().sum()} / {len(index_by_tract_df)}")
             print("-----------------------------------------------------------\n")
        else: index_by_tract_df[index_col_name] = np.nan; print(f"\n--- Index '{index_col_name}' set to NaN ---\n")

        # --- Select columns for merge ---
        if 'Origin_tract' not in index_by_tract_df.columns: return jsonify({"error": "Internal error preparing data."}), 500
        index_to_merge = index_by_tract_df[["Origin_tract", index_col_name]]

        # --- Merge into global_gdf ---
        # Drop existing column from global_gdf and state lists
        if index_col_name in global_gdf.columns:
            print(f"Column '{index_col_name}' exists. Dropping.")
            global_gdf = global_gdf.drop(columns=[index_col_name])
            if index_col_name in available_geojson_columns: available_geojson_columns.remove(index_col_name)
            if index_col_name in verified_frontend_cols: verified_frontend_cols.remove(index_col_name)
            if index_col_name in generated_index_columns: generated_index_columns.remove(index_col_name)

        # Apply robust conversion to global_gdf['Origin_tract'] just before merge
        if 'Origin_tract' in global_gdf.columns:
            print(f"DEBUG: Applying robust string conversion to global_gdf['Origin_tract']...")
            # ... (robust Float->Int->Str conversion for global_gdf['Origin_tract']) ...
            numeric_tracts_gdf = pd.to_numeric(global_gdf['Origin_tract'], errors='coerce')
            if numeric_tracts_gdf.isna().any(): print(f"WARNING: Some Origin_tract values in global_gdf were non-numeric.")
            try: int_tracts_gdf = numeric_tracts_gdf.astype(pd.Int64Dtype())
            except TypeError: print("WARNING: Nullable Int64Dtype unavailable for gdf. Falling back."); placeholder = 0; int_tracts_gdf = numeric_tracts_gdf.fillna(placeholder).astype(np.int64)
            global_gdf['Origin_tract'] = int_tracts_gdf.astype(str).str.strip()
        else: return jsonify({"error": "Base map missing 'Origin_tract'."}), 500

        # --- Key Comparison Diagnostics (Optional but recommended) ---
        print("\n--- DEBUG: Comparing Merge Keys ---")
        left_keys = set(global_gdf['Origin_tract'].unique()); right_keys = set(index_to_merge['Origin_tract'].unique())
        common_keys = left_keys.intersection(right_keys); print(f"Keys: Left={len(left_keys)}, Right={len(right_keys)}, Common={len(common_keys)}")
        if len(common_keys) == 0: print("!!! CRITICAL WARNING: NO common 'Origin_tract' keys found. Merge will result in NaNs.")
        print("--- End Key Comparison ---\n")

        # --- ATTEMPT MERGE using reset_index approach ---
        print(f"Attempting merge for index '{index_col_name}'...")
        original_rows = len(global_gdf); merged_gdf = None
        try:
            # Use copies to avoid modifying originals if merge check fails later
            gdf_temp = global_gdf.reset_index(drop=True)
            merge_temp = index_to_merge.reset_index(drop=True) # index_to_merge already has simple index
            merged_gdf = gdf_temp.merge(merge_temp, on="Origin_tract", how="left") # Perform merge

            # --- Validation Check AFTER merge attempt ---
            if index_col_name not in merged_gdf.columns:
                 print("ERROR: Merge completed but target column is missing!")
                 raise ValueError(f"Column '{index_col_name}' missing after merge.")
            # Check if the column is NOT entirely NaN (at least one value merged)
            if merged_gdf[index_col_name].isna().all():
                 print(f"ERROR: Merge completed but target column '{index_col_name}' is all NaN! Check key matching.")
                 # Allow proceeding for now, inspection log will show NaNs
                 # raise ValueError(f"Column '{index_col_name}' is all NaN after merge.")
            print("GeoPandas merge with reset_index appears structurally SUCCESSFUL.")

        except Exception as e_merge1:
             print(f"ERROR: GeoPandas merge with reset_index failed: {e_merge1}")
             traceback.print_exc()
             return jsonify({"error": f"Merge failed unexpectedly: {e_merge1}"}), 500

        # --- Assign back to global variable ONLY IF merge succeeded structurally ---
        global_gdf = merged_gdf
        # --- END MERGE ---

        # --- Inspect global_gdf IMMEDIATELY AFTER MERGE ---
        print(f"\n--- Inspecting global_gdf IMMEDIATELY AFTER MERGE for '{index_col_name}' ---")
        if index_col_name in global_gdf.columns:
             print(f"Dtype: {global_gdf[index_col_name].dtype}; NaN count: {global_gdf[index_col_name].isna().sum()} / {len(global_gdf)}")
             print(f"Sample values:\n{global_gdf[index_col_name].head()}")
        else: print(f"ERROR: Column '{index_col_name}' MISSING post-merge!")
        print("----------------------------------------------------------------------\n")

        # --- Update State Tracking Variables ---
        generated_index_columns.add(index_col_name)
        if index_col_name not in available_geojson_columns: available_geojson_columns.append(index_col_name)
        report_memory(f"After generating {index_col_name}")
        del index_by_tract_df, index_to_merge, gdf_temp, merge_temp; gc.collect() # Cleanup


        # --- Return FULL UPDATED GDF Slice to Frontend ---

        # --- Inspect global_gdf AGAIN right before slicing ---
        print(f"\n--- Inspecting global_gdf JUST BEFORE SLICING for '{index_col_name}' ---")
        if index_col_name in global_gdf.columns: print(f"Sample values:\n{global_gdf[index_col_name].head()}") # Focus on values
        else: print(f"ERROR: Column '{index_col_name}' MISSING before slicing!")
        print("------------------------------------------------------------------\n")

        cols_to_send = get_columns_for_frontend()
        if not cols_to_send or 'geometry' not in cols_to_send: return jsonify({"error": "Internal error selecting columns."}), 500

        # --- Prepare data FOR SENDING ---
        # *** Use .copy() - this seems to be the point of failure ***
        print("Creating final gdf_to_send using .copy()...")
        gdf_to_send = global_gdf[cols_to_send].copy()

        # --- Inspect gdf_to_send immediately after creation ---
        print(f"\n--- Inspecting gdf_to_send IMMEDIATELY AFTER SLICE/COPY for '{index_col_name}' ---")
        if index_col_name in gdf_to_send.columns:
            print(f"Dtype: {gdf_to_send[index_col_name].dtype}; NaN count: {gdf_to_send[index_col_name].isna().sum()} / {len(gdf_to_send)}")
            print(f"Sample values:\n{gdf_to_send[index_col_name].head()}") # <<< Does it survive the copy?
        else: print(f"ERROR: Column '{index_col_name}' MISSING from gdf_to_send!")
        print("--------------------------------------------------------------------------\n")

        # --- Final check before sending ---
        if index_col_name in gdf_to_send.columns:
             print(f"DEBUG Before Send: Final sample values of '{index_col_name}' being sent (dtype: {gdf_to_send[index_col_name].dtype}):\n{gdf_to_send[index_col_name].head()}")
        else: print(f"ERROR Before Send: Column '{index_col_name}' missing!")

        # --- Convert to GeoJSON Interface and Return ---
        try:
            geo_interface = gdf_to_send.to_crs(epsg=4326).__geo_interface__
            return jsonify(geo_interface)
        except Exception as e_json:
            print(f"ERROR: Failed during GeoJSON conversion or jsonify: {e_json}"); traceback.print_exc()
            return jsonify({"error": f"Failed to convert data to GeoJSON format: {e_json}"}), 500

    # --- Outer Exception Handling ---
    except Exception as e:
        print(f"ERROR in /generate_index outer try block: {e}"); traceback.print_exc()
        return jsonify({"error": f"Failed to compute activity index: {str(e)}"}), 500


# --- Generate Residential Index ---
@app.route('/generate_residential_index', methods=['POST'])
@login_required # Apply the decorator
def generate_residential_index():
    """Generates a Residential Index based on user-selected variables."""
    # Modifies global state: global_gdf, generated_index_columns, available_geojson_columns
    global global_gdf, generated_index_columns, available_geojson_columns, verified_frontend_cols, available_residential_vars_js

    if global_gdf is None: return jsonify({"error": "Map data not loaded."}), 500

    try:
        check_gdf()
        data = request.get_json()
        if not data: return jsonify({"error": "Invalid request."}), 400

        base_name_from_user = data.get('name', '').strip()
        selected_vars_js = data.get('variables', []) # Names as sent by JS
        if not base_name_from_user: return jsonify({"error": "Index name required."}), 400
        if not selected_vars_js: return jsonify({"error": "No variables selected."}), 400

        # Clean user name for column name
        cleaned_base_name = re.sub(r'\s+', '_', base_name_from_user); cleaned_base_name = re.sub(r'[^\w_]', '', cleaned_base_name)
        if not cleaned_base_name: return jsonify({"error": "Invalid index name."}), 400
        index_col_name = f"{cleaned_base_name}_RES"

        # --- Validate selected variables against AVAILABLE residential vars ---
        required_zscore_o_cols = []; valid_js_vars_for_this_request = []; invalid_vars_for_residential = []
        print(f"Checking availability of _zscore_o columns for selected JS variables: {selected_vars_js}")
        for var_js in selected_vars_js:
            backend_name = variable_name_map_js_to_backend.get(var_js)
            if not backend_name: print(f"WARNING: Ignoring unknown variable '{var_js}'"); continue

            if var_js in available_residential_vars_js: # Check against list populated at startup
                 zscore_col = f"{backend_name}_zscore_o"
                 if zscore_col in global_gdf.columns: # Double check existence in current gdf
                      required_zscore_o_cols.append(zscore_col)
                      valid_js_vars_for_this_request.append(var_js)
                 else: print(f"ERROR: State mismatch! Expected '{zscore_col}' missing."); invalid_vars_for_residential.append(var_js)
            else: invalid_vars_for_residential.append(var_js)

        # --- Error Handling ---
        if not required_zscore_o_cols:
            error_msg = "None of the selected variables have required data (_zscore_o columns) available."
            if invalid_vars_for_residential: error_msg += f" (Missing data for: {', '.join(invalid_vars_for_residential)})"
            return jsonify({"error": error_msg}), 400
        if invalid_vars_for_residential: print(f"WARNING: Calculating residential index '{index_col_name}' skipping variables: {', '.join(invalid_vars_for_residential)}")

        print(f"Generating Residential Index '{index_col_name}' using columns: {required_zscore_o_cols}")

        # --- Calculate Index ---
        index_values = global_gdf[required_zscore_o_cols].mean(axis=1, skipna=True).astype('float32') * 100.0

        # --- Add/Update column in global_gdf & state lists ---
        if index_col_name in global_gdf.columns:
            print(f"Column '{index_col_name}' exists. Dropping.")
            global_gdf = global_gdf.drop(columns=[index_col_name])
            if index_col_name in available_geojson_columns: available_geojson_columns.remove(index_col_name)
            if index_col_name in verified_frontend_cols: verified_frontend_cols.remove(index_col_name)
            if index_col_name in generated_index_columns: generated_index_columns.remove(index_col_name)

        global_gdf[index_col_name] = index_values
        generated_index_columns.add(index_col_name)
        if index_col_name not in available_geojson_columns: available_geojson_columns.append(index_col_name)

        print(f"Added/Updated residential index '{index_col_name}'. Dtype: {global_gdf[index_col_name].dtype}")
        report_memory(f"After generating {index_col_name}")

        # --- Return FULL UPDATED GDF Slice ---
        cols_to_send = get_columns_for_frontend()
        print(f"Returning updated GDF slice ({len(cols_to_send)} cols)")
        if not cols_to_send or 'geometry' not in cols_to_send: return jsonify({"error": "Internal error selecting columns."}), 500

        # Prepare data for sending - use .copy() for consistency
        gdf_to_send = global_gdf[cols_to_send].copy()
        print(f"DEBUG Before Send (Residential): Final sample values of '{index_col_name}' being sent (dtype: {gdf_to_send[index_col_name].dtype}):\n{gdf_to_send[index_col_name].head()}")

        return jsonify(gdf_to_send.to_crs(epsg=4326).__geo_interface__)

    except Exception as e:
        print(f"ERROR in /generate_residential_index: {e}")
        traceback.print_exc()
        return jsonify({"error": f"Failed to compute residential index: {str(e)}"}), 500


##############################################
# Login Route
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        entered_passcode = request.form.get('passcode')

        # --- ADD THESE DEBUG PRINTS ---
        print(f"--- Login Attempt ---")
        print(f"Entered Passcode from form: {repr(entered_passcode)}")
        print(f"Correct Passcode from env:  {repr(CORRECT_PASSCODE)}")
        print(f"Comparison Result (entered == correct): {entered_passcode == CORRECT_PASSCODE}")
        # --- END DEBUG PRINTS ---

        # Original comparison logic:
        if entered_passcode and entered_passcode == CORRECT_PASSCODE:
            session['logged_in'] = True
            session.permanent = True
            flash('You were successfully logged in!', 'success')
            next_page = request.args.get('next')
            print(f"DEBUG: Login SUCCESS, redirecting to: {next_page or url_for('index')}") # Debug redirect
            return redirect(next_page or url_for('index'))
        else:
            flash('Invalid passcode. Please try again.', 'danger')
            print(f"DEBUG: Login FAILED.") # Debug failure path
    # If GET request or login failed, show login page
    return render_template('login.html')

# Logout Route
@app.route('/logout')
def logout():
    session.pop('logged_in', None) # Remove logged_in status from session
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))




# --- Main Execution Block ---
if __name__ == '__main__':
    print("Starting Flask Application...")
    # threaded=False is crucial for debugging global state issues
    app.run(debug=True, threaded=False)