/**
 * BLOB Proof-of-Concept (raw @ibm/mapepire-js)
 *
 * Validates that mapepire-js accepts a hex-encoded BLOB parameter and
 * that the server stores bytes correctly. This runs BEFORE any wrapper
 * changes — if the server rejects or mis-decodes hex-encoded binary
 * bindings, the Buffer-normalization approach needs to change.
 *
 * Usage:
 *   IBMI_HOST=myibmi.com IBMI_USER=ME IBMI_PASSWORD=secret \
 *     node examples/blob-poc-mapepire.js
 *
 *   # Keep the POC table in place afterwards for independent ACS verification:
 *   IBMI_HOST=... IBMI_USER=... IBMI_PASSWORD=... \
 *     node examples/blob-poc-mapepire.js --keep
 *
 * Override the schema (default QGPL):
 *   BLOB_POC_SCHEMA=MYLIB node examples/blob-poc-mapepire.js
 *
 * By default the POC drops the BLOB_POC table before exiting so the script
 * is a self-cleaning diagnostic. Pass --keep to leave the two rows in place,
 * then independently verify the bytes via ACS Run SQL Scripts:
 *   SELECT ID, LENGTH(DATA) AS LEN, HEX(DATA) AS HEX_DATA
 *     FROM <SCHEMA>.BLOB_POC ORDER BY ID;
 *
 * Expected HEX_DATA values (uppercase):
 *   Row 1: DEADBEEF00FF0180
 *   Row 2: 54686973206973206120424C4F4220726F756E642D747269702074657374
 *          (= "This is a BLOB round-trip test" in UTF-8)
 */

const { SQLJob } = require('@ibm/mapepire-js');

const creds = {
  host: process.env.IBMI_HOST,
  user: process.env.IBMI_USER,
  password: process.env.IBMI_PASSWORD,
  rejectUnauthorized: false,
};

if (!creds.host || !creds.user || !creds.password) {
  console.error('Set IBMI_HOST, IBMI_USER, IBMI_PASSWORD in the environment.');
  process.exit(1);
}

const SCHEMA = process.env.BLOB_POC_SCHEMA || 'QGPL';
const TABLE = `${SCHEMA}.BLOB_POC`;
const KEEP_TABLE = process.argv.includes('--keep');

const SHORT_BYTES = Buffer.from([0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x01, 0x80]);
const MID_BYTES = Buffer.from('This is a BLOB round-trip test', 'utf8');

async function tryExecute(job, sql, params) {
  const opts = params ? { parameters: params } : undefined;
  return job.execute(sql, opts);
}

async function insertWithHex(job, id, bytes, label, useUpperCase) {
  const hex = useUpperCase
    ? bytes.toString('hex').toUpperCase()
    : bytes.toString('hex');
  const res = await tryExecute(
    job,
    `INSERT INTO ${TABLE} (ID, DATA) VALUES (?, ?)`,
    [id, hex],
  );
  if (!res.success) {
    throw new Error(
      `INSERT for row ${id} (${label}, ${useUpperCase ? 'UPPER' : 'lower'} hex) failed: ${res.error || JSON.stringify(res)}`,
    );
  }
}

async function dropTable(job) {
  const res = await tryExecute(job, `DROP TABLE ${TABLE}`);
  if (res.success) {
    console.log(`Dropped ${TABLE}`);
  } else {
    console.log(`DROP TABLE returned: ${res.error || JSON.stringify(res)}`);
  }
}

async function runPoc() {
  const job = new SQLJob();
  await job.connect(creds);
  console.log(`Connected to ${creds.host} as ${creds.user} (job ${job.id})`);

  try {
    // Create table (ignore "already exists" — table may persist across runs)
    try {
      await tryExecute(
        job,
        `CREATE TABLE ${TABLE} (ID INTEGER NOT NULL, DATA BLOB(1M), PRIMARY KEY (ID))`,
      );
      console.log(`Created table ${TABLE}`);
    } catch (e) {
      // -601 = duplicate table; other errors propagate
      const msg = e && e.message ? e.message : String(e);
      if (!/-601|already exists|duplicate/i.test(msg)) throw e;
      console.log(`Table ${TABLE} already exists — reusing`);
    }

    await tryExecute(job, `DELETE FROM ${TABLE}`);
    console.log(`Cleared ${TABLE}`);

    // Try lowercase hex first; fall back to uppercase on failure.
    let usedCase = 'lower';
    try {
      await insertWithHex(job, 1, SHORT_BYTES, 'short', false);
      await insertWithHex(job, 2, MID_BYTES, 'mid', false);
    } catch (lowerErr) {
      console.log(`Lowercase hex insert failed: ${lowerErr.message}`);
      console.log('Retrying with uppercase hex…');
      await tryExecute(job, `DELETE FROM ${TABLE}`);
      await insertWithHex(job, 1, SHORT_BYTES, 'short', true);
      await insertWithHex(job, 2, MID_BYTES, 'mid', true);
      usedCase = 'upper';
    }
    console.log(`Inserts succeeded using ${usedCase}case hex`);

    // Read back — mapepire returns BLOB as uppercase hex string
    const selectRes = await tryExecute(
      job,
      `SELECT ID, DATA, HEX(DATA) AS DATA_HEX, LENGTH(DATA) AS LEN FROM ${TABLE} ORDER BY ID`,
    );
    if (!selectRes.success) {
      throw new Error(`SELECT failed: ${selectRes.error || JSON.stringify(selectRes)}`);
    }

    const rows = selectRes.data;
    console.log('\nReturned rows:');
    for (const r of rows) {
      console.log(`  ID=${r.ID}  LEN=${r.LEN}  DATA=${r.DATA}  HEX(DATA)=${r.DATA_HEX}`);
    }

    // Assertions
    const expectedShort = SHORT_BYTES.toString('hex').toUpperCase();
    const expectedMid = MID_BYTES.toString('hex').toUpperCase();

    const row1 = rows.find(r => r.ID === 1);
    const row2 = rows.find(r => r.ID === 2);
    if (!row1 || !row2) throw new Error(`Missing rows; got ${rows.length}`);

    const ok =
      row1.DATA === expectedShort &&
      row1.DATA_HEX === expectedShort &&
      row1.LEN === SHORT_BYTES.length &&
      row2.DATA === expectedMid &&
      row2.DATA_HEX === expectedMid &&
      row2.LEN === MID_BYTES.length;

    if (!ok) {
      console.error('\nFAIL — round-trip mismatch:');
      console.error(`  row 1 expected DATA/HEX(DATA) = ${expectedShort} (len ${SHORT_BYTES.length})`);
      console.error(`  row 1 got      DATA          = ${row1.DATA}`);
      console.error(`  row 1 got      HEX(DATA)     = ${row1.DATA_HEX}`);
      console.error(`  row 1 got      LEN           = ${row1.LEN}`);
      console.error(`  row 2 expected DATA/HEX(DATA) = ${expectedMid} (len ${MID_BYTES.length})`);
      console.error(`  row 2 got      DATA          = ${row2.DATA}`);
      console.error(`  row 2 got      HEX(DATA)     = ${row2.DATA_HEX}`);
      console.error(`  row 2 got      LEN           = ${row2.LEN}`);
      process.exitCode = 1;
      return;
    }

    console.log('\nPASS — in-script round-trip matches on both rows.');
    console.log(`\nHex case accepted by mapepire: ${usedCase}case`);

    if (KEEP_TABLE) {
      console.log('\nIndependent verification — run in ACS Run SQL Scripts:');
      console.log(`  SELECT ID, LENGTH(DATA) AS LEN, HEX(DATA) AS HEX_DATA`);
      console.log(`    FROM ${TABLE} ORDER BY ID;`);
      console.log('\nExpected HEX_DATA:');
      console.log(`  Row 1: ${expectedShort}`);
      console.log(`  Row 2: ${expectedMid}`);
      console.log(`\n--keep was set — ${TABLE} has been left in place for verification.`);
    } else {
      console.log('');
      await dropTable(job);
    }
  } finally {
    await job.close();
  }
}

runPoc().catch(err => {
  console.error(err);
  process.exit(1);
});
