/**
 * Error Handling Example
 *
 * Demonstrates how to handle common error scenarios:
 * - SQL syntax errors
 * - Table not found errors
 * - Query errors with proper connection cleanup
 * - Always detaching connections in finally blocks
 */

const { RmPools } = require('rm-connector-js');

async function main() {
  const pools = new RmPools({
    pools: [
      {
        id: 'mydb',
        PoolOptions: {
          creds: {
            host: 'myibmi.com',
            user: 'MYUSER',
            password: 'MYPASSWORD',
            rejectUnauthorized: false
          },
          initialConnections: { size: 2 },
        },
      },
    ],
  });

  await pools.init();

  const pool = await pools.get('mydb');
  if (!pool) {
    throw new Error('Pool not found');
  }

  // --- 1. SQL syntax error ---
  console.log('=== Test 1: SQL syntax error ===');
  let conn = await pool.attach();
  try {
    await conn.query('SELEC * FORM QIWS.QCUSTCDT');  // intentional typos
  } catch (err) {
    console.log('Caught SQL error:', err.message);
  } finally {
    // Always detach — even after errors the connection is still usable
    await pool.detach(conn);
  }

  // --- 2. Table not found ---
  console.log('\n=== Test 2: Table not found ===');
  conn = await pool.attach();
  try {
    await conn.query('SELECT * FROM QIWS.NO_SUCH_TABLE');
  } catch (err) {
    console.log('Caught table error:', err.message);
  } finally {
    await pool.detach(conn);
  }

  // --- 3. Successful query after errors ---
  console.log('\n=== Test 3: Successful query after errors ===');
  conn = await pool.attach();
  try {
    const result = await conn.query('SELECT COUNT(*) AS TOTAL FROM QIWS.QCUSTCDT');
    console.log('Query succeeded:', result.data);
  } catch (err) {
    console.log('Unexpected error:', err.message);
  } finally {
    await pool.detach(conn);
  }

  // --- 4. Helper function pattern ---
  console.log('\n=== Test 4: Helper function pattern ===');
  const result = await safeQuery(pool, 'SELECT STATE, COUNT(*) AS CNT FROM QIWS.QCUSTCDT GROUP BY STATE');
  if (result.success) {
    console.log('States:', result.data);
  } else {
    console.log('Query failed:', result.error);
  }

  // Intentional error through helper
  const badResult = await safeQuery(pool, 'SELECT * FROM NONEXISTENT.TABLE');
  if (badResult.success) {
    console.log('Data:', badResult.data);
  } else {
    console.log('Expected error:', badResult.error);
  }

  await pools.close();
}

/**
 * A reusable helper that handles attach/detach/error for you.
 * Returns { success, data } or { success, error }.
 */
async function safeQuery(pool, sql, opts) {
  const conn = await pool.attach();
  try {
    const result = await conn.query(sql, opts);
    return { success: true, data: result.data };
  } catch (err) {
    return { success: false, error: err.message };
  } finally {
    await pool.detach(conn);
  }
}

main().catch(console.error);
