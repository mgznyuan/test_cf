// functions/_worker.js
import { Router, error, json } from 'itty-router';
import { tableFromIPC, RecordBatchStreamReader } from 'apache-arrow'; // <<< --- ADDED: Verify exact Arrow imports needed
import bcrypt from 'bcryptjs'; 

// Create a new router
const router = Router();

// Basic route
router.get('/api/hello', () => {
  return json({ message: 'Hello from itty-router!' });
});

// Catch-all for other routes
router.all('*', () => new Response('Not Found.', { status: 404 }));

/*
 * Standard Cloudflare Pages function export
 */
export async function onRequest(context) {
  // context includes: request, env, params, waitUntil, next, data
  return router.handle(context.request);
}