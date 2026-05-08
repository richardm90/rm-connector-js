/**
 * Backend Parity Tests
 *
 * These integration tests run the SAME operations against both the mapepire and
 * idb backends on a real IBM i system, then compare the results to ensure they
 * produce equivalent output.
 *
 * These tests are NOT part of the regular `npm test` suite. They require:
 *   - Running on IBM i (or network access to an IBM i with mapepire)
 *   - Environment variables: IBMI_HOST, IBMI_USER, IBMI_PASSWORD
 *   - A running mapepire server on the target IBM i
 *
 * Run with: npm run test:parity
 */

import RmConnection from '../../src/rmConnection';
import { RmConnectionOptions, RmQueryResult } from '../../src/types';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const MAPEPIRE_CREDS = {
  host: process.env.IBMI_HOST || 'localhost',
  user: process.env.IBMI_USER || '',
  password: process.env.IBMI_PASSWORD || '',
  rejectUnauthorized: false,
};

/** Fields that are expected to differ between backends (see docs/BACKEND-DIFFERENCES.md) */
function normalise(result: RmQueryResult<any>): {
  success: boolean;
  data: any[];
  has_results: boolean;
  is_done: boolean;
} {
  return {
    success: result.success,
    data: result.data,
    has_results: result.has_results,
    is_done: result.is_done,
  };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function baseOpts(): { idb: RmConnectionOptions; mapepire: RmConnectionOptions } {
  return {
    idb: { backend: 'idb' },
    mapepire: { backend: 'mapepire', creds: MAPEPIRE_CREDS },
  };
}

async function withBothBackends(
  optsOverride: Partial<RmConnectionOptions> = {},
  fn: (idb: RmConnection, mapepire: RmConnection) => Promise<void>,
): Promise<void> {
  const base = baseOpts();
  const idb = new RmConnection({ ...base.idb, ...optsOverride });
  const mapepire = new RmConnection({ ...base.mapepire, ...optsOverride });

  await Promise.all([idb.init(true), mapepire.init(true)]);

  try {
    await fn(idb, mapepire);
  } finally {
    await Promise.all([idb.close(), mapepire.close()]);
  }
}

// ---------------------------------------------------------------------------
// Guard: skip entire suite if credentials are missing
// ---------------------------------------------------------------------------

const skip = !process.env.IBMI_HOST || !process.env.IBMI_USER || !process.env.IBMI_PASSWORD;

const describeIf = skip ? describe.skip : describe;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describeIf('Backend Parity', () => {
  jest.setTimeout(30_000);

  const SHARED_LIB = 'PARITYTEST';

  // Top-level teardown: drop the shared test schema after all tests complete
  afterAll(async () => {
    const teardown = new RmConnection({ backend: 'idb' });
    await teardown.init(true);
    try {
      await teardown.execute(`DROP SCHEMA ${SHARED_LIB} CASCADE`);
    } catch (e) {
      // Best-effort cleanup
    } finally {
      await teardown.close();
    }
  });

  // ----- Basic queries -----

  describe('Simple queries', () => {
    it('SELECT with mixed data types', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT
          CUSNUM, LSTNAM, INIT, STREET, CITY, STATE, ZIPCOD, CDTLMT, CHGCOD, BALDUE, CDTDUE
          FROM QIWS.QCUSTCDT ORDER BY CUSNUM`;

        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });

    it('VALUES expression', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = 'VALUES (1, 2, 3)';

        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });

    it('SELECT with no results', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT * FROM QIWS.QCUSTCDT WHERE CUSNUM = -1`;

        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });

    it('CURRENT TIMESTAMP / CURRENT DATE / CURRENT TIME', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        // Use CURRENT_TIMESTAMP with ISO format to avoid job date format differences
        const sql = `VALUES VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS')`;

        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        // Both should return a single row with the same formatted timestamp (within the same second)
        expect(idbRes.success).toBe(true);
        expect(mapRes.success).toBe(true);
        expect(idbRes.data.length).toBe(1);
        expect(mapRes.data.length).toBe(1);
        // Verify same column name
        expect(Object.keys(idbRes.data[0])).toEqual(Object.keys(mapRes.data[0]));
      });
    });
  });

  // ----- Parameterized queries -----

  describe('Parameterized queries', () => {
    it('SELECT with parameter markers', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = 'SELECT CUSNUM, LSTNAM, STATE FROM QIWS.QCUSTCDT WHERE STATE = ? ORDER BY CUSNUM';
        const opts = { parameters: ['NY'] };

        const [idbRes, mapRes] = await Promise.all([
          idb.execute(sql, opts),
          mapepire.execute(sql, opts),
        ]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });

    it('SELECT with multiple parameters', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = 'SELECT CUSNUM, LSTNAM FROM QIWS.QCUSTCDT WHERE STATE = ? AND CDTLMT > ? ORDER BY CUSNUM';
        const opts = { parameters: ['TX', 100] };

        const [idbRes, mapRes] = await Promise.all([
          idb.execute(sql, opts),
          mapepire.execute(sql, opts),
        ]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });
  });

  // ----- CL commands -----

  describe('CL commands via QCMDEXC', () => {
    it('CHGJOB command succeeds on both backends', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = 'CALL QSYS2.QCMDEXC(?)';
        const opts = { parameters: ['CHGJOB INQMSGRPY(*DFT)'] };

        const [idbRes, mapRes] = await Promise.all([
          idb.execute(sql, opts),
          mapepire.execute(sql, opts),
        ]);

        expect(idbRes.success).toBe(true);
        expect(mapRes.success).toBe(true);
      });
    });
  });

  // ----- String trimming -----

  describe('String trimming', () => {
    it('CHAR columns should be trimmed consistently', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        // LSTNAM is CHAR(8) — both backends should trimEnd trailing spaces
        const sql = 'SELECT LSTNAM FROM QIWS.QCUSTCDT ORDER BY CUSNUM';

        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });
  });

  // ----- Numeric types -----

  describe('Numeric types', () => {
    it('numeric values should match (decimal, integer)', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = 'SELECT CUSNUM, CDTLMT, BALDUE, CDTDUE FROM QIWS.QCUSTCDT ORDER BY CUSNUM';

        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });
  });

  // ----- Error handling -----

  describe('Error handling', () => {
    it('both backends should fail on invalid SQL', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = 'SELECT * FROM NONEXISTENT.TABLE_DOES_NOT_EXIST';

        await expect(idb.execute(sql)).rejects.toThrow();
        await expect(mapepire.execute(sql)).rejects.toThrow();
      });
    });

    it('both backends should fail on syntax errors', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = 'SELCT * FORM QIWS.QCUSTCDT';

        await expect(idb.execute(sql)).rejects.toThrow();
        await expect(mapepire.execute(sql)).rejects.toThrow();
      });
    });
  });

  // ----- Column names -----

  describe('Column names', () => {
    it('column names should match between backends', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = 'SELECT CUSNUM, LSTNAM, INIT, STATE FROM QIWS.QCUSTCDT FETCH FIRST 1 ROW ONLY';

        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        const idbCols = Object.keys(idbRes.data[0]).sort();
        const mapCols = Object.keys(mapRes.data[0]).sort();
        expect(idbCols).toEqual(mapCols);
      });
    });
  });

  // ----- JDBCOptions: libraries -----

  describe('JDBCOptions: libraries', () => {
    it('should resolve library objects identically', async () => {
      await withBothBackends({ JDBCOptions: { libraries: ['QIWS'] } }, async (idb, mapepire) => {
        // With QIWS in library list, unqualified access should work
        const sql = 'SELECT CUSNUM, LSTNAM FROM QCUSTCDT ORDER BY CUSNUM FETCH FIRST 3 ROWS ONLY';

        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });

    it('should accept single library as string', async () => {
      await withBothBackends({ JDBCOptions: { libraries: 'QIWS' as any } }, async (idb, mapepire) => {
        const sql = 'SELECT COUNT(*) AS CNT FROM QCUSTCDT';

        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });
  });

  // ----- JDBCOptions: multiple libraries -----

  describe('JDBCOptions: multiple libraries', () => {
    const TEST_LIB = 'PARITYTEST';

    // Setup: create a test library with a table and seed data
    beforeAll(async () => {
      const setup = new RmConnection({ backend: 'idb' });
      await setup.init(true);
      try {
        // Create library (ignore error if it already exists).
        // We use CREATE SCHEMA instead of QCMDEXC CRTLIB because idb-pconnector
        // wraps CL errors as generic SQLCODE=-443 without the underlying CPF code,
        // making it impossible to distinguish "already exists" from real failures.
        try {
          await setup.execute(`CREATE SCHEMA ${TEST_LIB}`);
        } catch (e: any) {
          // SQLCODE=-601 = object already exists
          if (!e?.message?.includes('SQLCODE=-601')) throw e;
        }
        // Create and populate test table
        await setup.execute(`CREATE OR REPLACE TABLE ${TEST_LIB}.PRODUCTS (
          PRODID INT NOT NULL, PRODNAME VARCHAR(30), PRICE DECIMAL(9,2)
        )`);
        await setup.execute(`DELETE FROM ${TEST_LIB}.PRODUCTS`);
        await setup.execute(`INSERT INTO ${TEST_LIB}.PRODUCTS VALUES (1, 'Widget', 9.99)`);
        await setup.execute(`INSERT INTO ${TEST_LIB}.PRODUCTS VALUES (2, 'Gadget', 24.50)`);
        await setup.execute(`INSERT INTO ${TEST_LIB}.PRODUCTS VALUES (3, 'Sprocket', 3.75)`);
      } finally {
        await setup.close();
      }
    });

    // Teardown: drop only the table — the PARITYTEST schema is shared with
    // other describe blocks and is cleaned up by the top-level afterAll.
    afterAll(async () => {
      const teardown = new RmConnection({ backend: 'idb' });
      await teardown.init(true);
      try {
        await teardown.execute(`DROP TABLE IF EXISTS ${TEST_LIB}.PRODUCTS`);
      } catch (e) {
        // Best-effort cleanup
      } finally {
        await teardown.close();
      }
    });

    it('should resolve unqualified table from custom library', async () => {
      await withBothBackends(
        { JDBCOptions: { libraries: [TEST_LIB] } },
        async (idb, mapepire) => {
          const sql = 'SELECT PRODID, PRODNAME, PRICE FROM PRODUCTS ORDER BY PRODID';

          const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
          expect(idbRes.data.length).toBe(3);
        },
      );
    });

    it('should resolve unqualified from first library, qualified from others', async () => {
      // mapepire sets default schema to first library; idb adds all to library list.
      // Both can resolve unqualified from the first library and qualified from others.
      await withBothBackends(
        { JDBCOptions: { libraries: [TEST_LIB, 'QIWS'] } },
        async (idb, mapepire) => {
          // Unqualified: resolves from first library (PARITYTEST)
          const prodSql = 'SELECT PRODID, PRODNAME FROM PRODUCTS ORDER BY PRODID';
          const [idbProd, mapProd] = await Promise.all([idb.execute(prodSql), mapepire.execute(prodSql)]);
          expect(normalise(idbProd)).toEqual(normalise(mapProd));

          // Qualified: explicit library reference works regardless
          const custSql = 'SELECT CUSNUM, LSTNAM FROM QIWS.QCUSTCDT ORDER BY CUSNUM FETCH FIRST 3 ROWS ONLY';
          const [idbCust, mapCust] = await Promise.all([idb.execute(custSql), mapepire.execute(custSql)]);
          expect(normalise(idbCust)).toEqual(normalise(mapCust));
        },
      );
    });

    it('should resolve with system naming and multiple libraries', async () => {
      await withBothBackends(
        { JDBCOptions: { libraries: [TEST_LIB, 'QIWS'], naming: 'system' } },
        async (idb, mapepire) => {
          // Unqualified access resolves from first library
          const prodSql = 'SELECT PRODID, PRODNAME, PRICE FROM PRODUCTS ORDER BY PRODID';
          const [idbProd, mapProd] = await Promise.all([idb.execute(prodSql), mapepire.execute(prodSql)]);
          expect(normalise(idbProd)).toEqual(normalise(mapProd));

          // Unqualified access resolves from second library via *LIBL
          const custSql = 'SELECT CUSNUM, LSTNAM FROM QCUSTCDT ORDER BY CUSNUM FETCH FIRST 3 ROWS ONLY';
          const [idbCust, mapCust] = await Promise.all([idb.execute(custSql), mapepire.execute(custSql)]);
          expect(normalise(idbCust)).toEqual(normalise(mapCust));
        },
      );
    });

    it('first library determines default schema for both backends', async () => {
      // When QIWS is first, unqualified QCUSTCDT resolves from QIWS on both backends
      await withBothBackends(
        { JDBCOptions: { libraries: ['QIWS', TEST_LIB] } },
        async (idb, mapepire) => {
          const sql = 'SELECT CUSNUM, LSTNAM FROM QCUSTCDT ORDER BY CUSNUM FETCH FIRST 3 ROWS ONLY';

          const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
        },
      );
    });
  });

  // ----- JDBCOptions: naming -----

  describe('JDBCOptions: naming', () => {
    it('system naming should allow slash separator', async () => {
      await withBothBackends({ JDBCOptions: { naming: 'system' } }, async (idb, mapepire) => {
        const sql = 'SELECT CUSNUM, LSTNAM FROM QIWS/QCUSTCDT ORDER BY CUSNUM FETCH FIRST 3 ROWS ONLY';

        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });

    it('sql naming should allow dot separator', async () => {
      await withBothBackends({ JDBCOptions: { naming: 'sql' } }, async (idb, mapepire) => {
        const sql = 'SELECT CUSNUM, LSTNAM FROM QIWS.QCUSTCDT ORDER BY CUSNUM FETCH FIRST 3 ROWS ONLY';

        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });
  });

  // ----- JDBCOptions: transaction isolation -----

  describe('JDBCOptions: transaction isolation', () => {
    const isolationLevels = [
      'none',
      'read uncommitted',
      'read committed',
      'repeatable read',
      'serializable',
    ] as const;

    for (const level of isolationLevels) {
      it(`isolation '${level}' — query results should match`, async () => {
        await withBothBackends(
          { JDBCOptions: { 'transaction isolation': level } },
          async (idb, mapepire) => {
            const sql = 'SELECT CUSNUM, LSTNAM FROM QIWS.QCUSTCDT ORDER BY CUSNUM FETCH FIRST 3 ROWS ONLY';

            const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

            expect(normalise(idbRes)).toEqual(normalise(mapRes));
          },
        );
      });
    }
  });

  // ----- JDBCOptions: auto commit -----

  describe('JDBCOptions: auto commit', () => {
    it('auto commit true — query results should match', async () => {
      await withBothBackends(
        { JDBCOptions: { 'auto commit': true } },
        async (idb, mapepire) => {
          const sql = 'SELECT CUSNUM, LSTNAM FROM QIWS.QCUSTCDT ORDER BY CUSNUM FETCH FIRST 3 ROWS ONLY';

          const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
        },
      );
    });

    it('auto commit false — query results should match', async () => {
      await withBothBackends(
        { JDBCOptions: { 'auto commit': false, 'transaction isolation': 'read committed' } },
        async (idb, mapepire) => {
          const sql = 'SELECT CUSNUM, LSTNAM FROM QIWS.QCUSTCDT ORDER BY CUSNUM FETCH FIRST 3 ROWS ONLY';

          const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
        },
      );
    });
  });

  // ----- JDBCOptions: combined -----

  describe('JDBCOptions: combined options', () => {
    it('libraries + naming + transaction isolation together', async () => {
      await withBothBackends(
        {
          JDBCOptions: {
            libraries: ['QIWS'],
            naming: 'system',
            'transaction isolation': 'read committed',
            'auto commit': true,
          },
        },
        async (idb, mapepire) => {
          const sql = 'SELECT CUSNUM, LSTNAM FROM QIWS/QCUSTCDT ORDER BY CUSNUM FETCH FIRST 3 ROWS ONLY';

          const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
        },
      );
    });
  });

  // ----- DML with commitment control -----

  describe('DML with commitment control', () => {
    // Each test creates its own temp table (DECLARE GLOBAL TEMPORARY TABLE)
    // which is session-scoped and automatically cleaned up on disconnect.

    it('INSERT with auto commit true', async () => {
      await withBothBackends(
        { JDBCOptions: { 'auto commit': true, 'transaction isolation': 'read committed' } },
        async (idb, mapepire) => {
          const createSql = `DECLARE GLOBAL TEMPORARY TABLE PARITY_INS (
            ID INT, NAME VARCHAR(20)
          ) WITH REPLACE`;
          const insertSql = 'INSERT INTO SESSION.PARITY_INS VALUES (?, ?)';
          const selectSql = 'SELECT ID, NAME FROM SESSION.PARITY_INS ORDER BY ID';

          // Create table and insert on both
          for (const conn of [idb, mapepire]) {
            await conn.execute(createSql);
            await conn.execute(insertSql, { parameters: [1, 'Alice'] });
            await conn.execute(insertSql, { parameters: [2, 'Bob'] });
          }

          const [idbRes, mapRes] = await Promise.all([
            idb.execute(selectSql),
            mapepire.execute(selectSql),
          ]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
        },
      );
    });

    it('UPDATE with auto commit true', async () => {
      await withBothBackends(
        { JDBCOptions: { 'auto commit': true, 'transaction isolation': 'read committed' } },
        async (idb, mapepire) => {
          const createSql = `DECLARE GLOBAL TEMPORARY TABLE PARITY_UPD (
            ID INT, NAME VARCHAR(20)
          ) WITH REPLACE`;
          const insertSql = 'INSERT INTO SESSION.PARITY_UPD VALUES (?, ?)';
          const updateSql = 'UPDATE SESSION.PARITY_UPD SET NAME = ? WHERE ID = ?';
          const selectSql = 'SELECT ID, NAME FROM SESSION.PARITY_UPD ORDER BY ID';

          for (const conn of [idb, mapepire]) {
            await conn.execute(createSql);
            await conn.execute(insertSql, { parameters: [1, 'Alice'] });
            await conn.execute(insertSql, { parameters: [2, 'Bob'] });
            await conn.execute(updateSql, { parameters: ['Charlie', 2] });
          }

          const [idbRes, mapRes] = await Promise.all([
            idb.execute(selectSql),
            mapepire.execute(selectSql),
          ]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
        },
      );
    });

    it('DELETE with auto commit true', async () => {
      await withBothBackends(
        { JDBCOptions: { 'auto commit': true, 'transaction isolation': 'read committed' } },
        async (idb, mapepire) => {
          const createSql = `DECLARE GLOBAL TEMPORARY TABLE PARITY_DEL (
            ID INT, NAME VARCHAR(20)
          ) WITH REPLACE`;
          const insertSql = 'INSERT INTO SESSION.PARITY_DEL VALUES (?, ?)';
          const deleteSql = 'DELETE FROM SESSION.PARITY_DEL WHERE ID = ?';
          const selectSql = 'SELECT ID, NAME FROM SESSION.PARITY_DEL ORDER BY ID';

          for (const conn of [idb, mapepire]) {
            await conn.execute(createSql);
            await conn.execute(insertSql, { parameters: [1, 'Alice'] });
            await conn.execute(insertSql, { parameters: [2, 'Bob'] });
            await conn.execute(insertSql, { parameters: [3, 'Charlie'] });
            await conn.execute(deleteSql, { parameters: [2] });
          }

          const [idbRes, mapRes] = await Promise.all([
            idb.execute(selectSql),
            mapepire.execute(selectSql),
          ]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
        },
      );
    });

    it('INSERT/SELECT with no commit (isolation none)', async () => {
      await withBothBackends(
        { JDBCOptions: { 'transaction isolation': 'none' } },
        async (idb, mapepire) => {
          const createSql = `DECLARE GLOBAL TEMPORARY TABLE PARITY_NC (
            ID INT, NAME VARCHAR(20)
          ) WITH REPLACE`;
          const insertSql = 'INSERT INTO SESSION.PARITY_NC VALUES (?, ?)';
          const selectSql = 'SELECT ID, NAME FROM SESSION.PARITY_NC ORDER BY ID';

          for (const conn of [idb, mapepire]) {
            await conn.execute(createSql);
            await conn.execute(insertSql, { parameters: [1, 'Alpha'] });
            await conn.execute(insertSql, { parameters: [2, 'Beta'] });
          }

          const [idbRes, mapRes] = await Promise.all([
            idb.execute(selectSql),
            mapepire.execute(selectSql),
          ]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
        },
      );
    });

    it('INSERT/SELECT with read uncommitted', async () => {
      await withBothBackends(
        { JDBCOptions: { 'auto commit': false, 'transaction isolation': 'read uncommitted' } },
        async (idb, mapepire) => {
          const createSql = `DECLARE GLOBAL TEMPORARY TABLE PARITY_RU (
            ID INT, NAME VARCHAR(20)
          ) WITH REPLACE`;
          const insertSql = 'INSERT INTO SESSION.PARITY_RU VALUES (?, ?)';
          const selectSql = 'SELECT ID, NAME FROM SESSION.PARITY_RU ORDER BY ID';

          for (const conn of [idb, mapepire]) {
            await conn.execute(createSql);
            await conn.execute(insertSql, { parameters: [1, 'Gamma'] });
            await conn.execute(insertSql, { parameters: [2, 'Delta'] });
          }

          const [idbRes, mapRes] = await Promise.all([
            idb.execute(selectSql),
            mapepire.execute(selectSql),
          ]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
        },
      );
    });

    it('INSERT/SELECT with serializable', async () => {
      await withBothBackends(
        { JDBCOptions: { 'auto commit': false, 'transaction isolation': 'serializable' } },
        async (idb, mapepire) => {
          const createSql = `DECLARE GLOBAL TEMPORARY TABLE PARITY_SER (
            ID INT, NAME VARCHAR(20)
          ) WITH REPLACE`;
          const insertSql = 'INSERT INTO SESSION.PARITY_SER VALUES (?, ?)';
          const selectSql = 'SELECT ID, NAME FROM SESSION.PARITY_SER ORDER BY ID';

          for (const conn of [idb, mapepire]) {
            await conn.execute(createSql);
            await conn.execute(insertSql, { parameters: [1, 'Epsilon'] });
            await conn.execute(insertSql, { parameters: [2, 'Zeta'] });
          }

          const [idbRes, mapRes] = await Promise.all([
            idb.execute(selectSql),
            mapepire.execute(selectSql),
          ]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
        },
      );
    });
  });

  // ----- Commitment control: transactional guarantees -----

  describe('Commitment control: transactional guarantees', () => {

    it('COMMIT should persist changes', async () => {
      await withBothBackends(
        { JDBCOptions: { 'auto commit': false, 'transaction isolation': 'read committed' } },
        async (idb, mapepire) => {
          const createSql = `DECLARE GLOBAL TEMPORARY TABLE PARITY_CMT (
            ID INT, NAME VARCHAR(20)
          ) ON COMMIT PRESERVE ROWS WITH REPLACE`;
          const insertSql = 'INSERT INTO SESSION.PARITY_CMT VALUES (?, ?)';
          const selectSql = 'SELECT ID, NAME FROM SESSION.PARITY_CMT ORDER BY ID';

          for (const conn of [idb, mapepire]) {
            await conn.execute(createSql);
            await conn.execute(insertSql, { parameters: [1, 'Alice'] });
            await conn.execute('COMMIT');
            await conn.execute(insertSql, { parameters: [2, 'Bob'] });
            await conn.execute('COMMIT');
          }

          const [idbRes, mapRes] = await Promise.all([
            idb.execute(selectSql),
            mapepire.execute(selectSql),
          ]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
          expect(idbRes.data.length).toBe(2);
        },
      );
    });

    it('ROLLBACK should revert uncommitted changes', async () => {
      await withBothBackends(
        { JDBCOptions: { 'auto commit': false, 'transaction isolation': 'read committed' } },
        async (idb, mapepire) => {
          const createSql = `DECLARE GLOBAL TEMPORARY TABLE PARITY_RBK (
            ID INT, NAME VARCHAR(20)
          ) ON COMMIT PRESERVE ROWS WITH REPLACE`;
          const insertSql = 'INSERT INTO SESSION.PARITY_RBK VALUES (?, ?)';
          const selectSql = 'SELECT ID, NAME FROM SESSION.PARITY_RBK ORDER BY ID';

          for (const conn of [idb, mapepire]) {
            await conn.execute(createSql);
            await conn.execute(insertSql, { parameters: [1, 'Alice'] });
            await conn.execute('COMMIT');
            await conn.execute(insertSql, { parameters: [2, 'Bob'] });
            await conn.execute('ROLLBACK');
          }

          const [idbRes, mapRes] = await Promise.all([
            idb.execute(selectSql),
            mapepire.execute(selectSql),
          ]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
          // Only the committed row should remain
          expect(idbRes.data.length).toBe(1);
          expect(idbRes.data[0].NAME).toBe('Alice');
        },
      );
    });

    it('failed statement should not corrupt transaction state', async () => {
      await withBothBackends(
        { JDBCOptions: { 'auto commit': false, 'transaction isolation': 'read committed' } },
        async (idb, mapepire) => {
          const createSql = `DECLARE GLOBAL TEMPORARY TABLE PARITY_ERR (
            ID INT, NAME VARCHAR(20)
          ) ON COMMIT PRESERVE ROWS WITH REPLACE`;
          const insertSql = 'INSERT INTO SESSION.PARITY_ERR VALUES (?, ?)';
          const selectSql = 'SELECT ID, NAME FROM SESSION.PARITY_ERR ORDER BY ID';

          for (const conn of [idb, mapepire]) {
            await conn.execute(createSql);
            await conn.execute(insertSql, { parameters: [1, 'Alice'] });

            // Execute an invalid statement
            try {
              await conn.execute('SELECT * FROM NONEXISTENT.TABLE_DOES_NOT_EXIST');
            } catch (_e) {
              // Expected to fail
            }

            // Connection should still be usable — insert and commit
            await conn.execute(insertSql, { parameters: [2, 'Bob'] });
            await conn.execute('COMMIT');
          }

          const [idbRes, mapRes] = await Promise.all([
            idb.execute(selectSql),
            mapepire.execute(selectSql),
          ]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
          expect(idbRes.data.length).toBe(2);
        },
      );
    });
  });

  // ----- initCommands -----

  describe('initCommands', () => {
    it('CL init commands execute on both backends', async () => {
      await withBothBackends(
        { initCommands: [{ command: 'CHGJOB INQMSGRPY(*DFT)', type: 'cl' }] },
        async (idb, mapepire) => {
          const sql = 'SELECT CUSNUM FROM QIWS.QCUSTCDT ORDER BY CUSNUM FETCH FIRST 1 ROW ONLY';

          const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
        },
      );
    });

    it('SQL init commands execute on both backends', async () => {
      await withBothBackends(
        { initCommands: [{ command: 'SET SCHEMA QIWS', type: 'sql' }] },
        async (idb, mapepire) => {
          const sql = 'SELECT CUSNUM FROM QCUSTCDT ORDER BY CUSNUM FETCH FIRST 3 ROWS ONLY';

          const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

          expect(normalise(idbRes)).toEqual(normalise(mapRes));
        },
      );
    });
  });

  // ===================================================================
  // Data type parity
  // ===================================================================
  // Tests that all major DB2 for i SQL data types produce identical
  // results from both backends.
  //
  // Types NOT covered: DATALINK, ROWID, XML, DBCLOB, FLOAT (alias for DOUBLE)
  // ===================================================================

  describe('Data type parity', () => {
    const DT_LIB = 'PARITYTEST';
    const DT_TABLE = `${DT_LIB}.DATATYPES`;

    // --- Setup: create DATATYPES table and insert test rows ---

    beforeAll(async () => {
      const setup = new RmConnection({ backend: 'idb' });
      await setup.init(true);
      try {
        // Ensure library exists (ignore -601 if already there).
        // CREATE SCHEMA gives us a standard SQL error; QCMDEXC wraps CL errors
        // as generic SQLCODE=-443 losing the CPF code from the message.
        try {
          await setup.execute(`CREATE SCHEMA ${DT_LIB}`);
        } catch (e: any) {
          if (!e?.message?.includes('SQLCODE=-601')) throw e;
        }

        await setup.execute(`CREATE OR REPLACE TABLE ${DT_TABLE} (
          ROW_ID         INTEGER       NOT NULL,
          ROW_LABEL      VARCHAR(30)   NOT NULL,
          COL_SMALLINT   SMALLINT,
          COL_INT        INTEGER,
          COL_BIGINT     BIGINT,
          COL_DECIMAL    DECIMAL(15,4),
          COL_NUMERIC    NUMERIC(15,4),
          COL_REAL       REAL,
          COL_DOUBLE     DOUBLE,
          COL_CHAR       CHAR(20),
          COL_VARCHAR    VARCHAR(100),
          COL_CLOB       CLOB(1K),
          COL_DATE       DATE,
          COL_TIME       TIME,
          COL_TIMESTAMP  TIMESTAMP,
          COL_BINARY     BINARY(16),
          COL_VARBINARY  VARBINARY(64),
          COL_BLOB       BLOB(1K),
          COL_GRAPHIC    GRAPHIC(10) CCSID 13488,
          COL_VARGRAPHIC VARGRAPHIC(40) CCSID 13488,
          COL_DF16       DECFLOAT(16),
          COL_DF34       DECFLOAT(34)
        )`);

        // Row 1: all values populated
        await setup.execute(`DELETE FROM ${DT_TABLE}`);
        await setup.execute(`INSERT INTO ${DT_TABLE} VALUES (
          1, 'ALL_VALUES',
          32000, 2147483000, 9007199254740000,
          12345.6789, 98765.4321,
          3.14, 2.718281828459045,
          'HELLO', 'World of DB2 for i', 'This is a CLOB value',
          '2024-06-15', '13:45:30', '2024-06-15-13.45.30.123456',
          CAST(X'48454C4C4F0000000000000000000000' AS BINARY(16)),
          CAST(X'DEADBEEF' AS VARBINARY(64)),
          CAST(X'0102030405' AS BLOB(1K)),
          'TestGraph', 'VarGraphic',
          12345.6789, 98765432101234.5678901234567890
        )`);

        // Row 2: all nulls (except PK and ROW_LABEL)
        await setup.execute(
          `INSERT INTO ${DT_TABLE} (ROW_ID, ROW_LABEL) VALUES (2, 'ALL_NULLS')`
        );

        // Row 3: edge cases
        await setup.execute(`INSERT INTO ${DT_TABLE} VALUES (
          3, 'EDGE_CASES',
          -32768, -2147483648, -9007199254740000,
          0.0000, 0.0000,
          0.0, -1.7976931348623157E+308,
          '                    ', '', '',
          '1970-01-01', '00:00:00', '1970-01-01-00.00.00.000000',
          CAST(X'00000000000000000000000000000000' AS BINARY(16)),
          CAST(X'' AS VARBINARY(64)),
          CAST(X'' AS BLOB(1K)),
          '          ', '',
          0.0, -1.23456789E+10
        )`);
      } finally {
        await setup.close();
      }
    }, 60_000);

    afterAll(async () => {
      const teardown = new RmConnection({ backend: 'idb' });
      await teardown.init(true);
      try {
        await teardown.execute(`DROP TABLE ${DT_TABLE}`);
      } catch (e) {
        // Best-effort cleanup
      } finally {
        await teardown.close();
      }
    });

    // ----- Integer types -----

    it('integer types (SMALLINT, INTEGER, BIGINT)', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_SMALLINT, COL_INT, COL_BIGINT FROM ${DT_TABLE} WHERE ROW_ID = 1`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        // SMALLINT and INTEGER match across backends
        expect(idbRes.data[0].COL_SMALLINT).toBe(mapRes.data[0].COL_SMALLINT);
        expect(idbRes.data[0].COL_INT).toBe(mapRes.data[0].COL_INT);
        expect(idbRes.data[0].COL_SMALLINT).toBe(32000);
        expect(idbRes.data[0].COL_INT).toBe(2147483000);

        // BIGINT: idb returns string, mapepire returns number
        expect(idbRes.data[0].COL_BIGINT).toBe('9007199254740000');
        expect(mapRes.data[0].COL_BIGINT).toBe(9007199254740000);
      });
    });

    // ----- Exact numeric types -----

    it('exact numeric types (DECIMAL, NUMERIC)', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_DECIMAL, COL_NUMERIC FROM ${DT_TABLE} WHERE ROW_ID = 1`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);
        expect(normalise(idbRes)).toEqual(normalise(mapRes));
        expect(idbRes.data[0].COL_DECIMAL).toBe(12345.6789);
        expect(idbRes.data[0].COL_NUMERIC).toBe(98765.4321);
      });
    });

    // ----- Approximate numeric types -----

    it('approximate numeric types (REAL, DOUBLE)', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_REAL, COL_DOUBLE FROM ${DT_TABLE} WHERE ROW_ID = 1`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        // REAL matches across backends
        expect(idbRes.data[0].COL_REAL).toBe(mapRes.data[0].COL_REAL);
        expect(idbRes.data[0].COL_REAL).toBe(3.14);

        // DOUBLE: idb truncates to ~6 significant digits, mapepire returns full precision
        expect(idbRes.data[0].COL_DOUBLE).toBeCloseTo(mapRes.data[0].COL_DOUBLE, 5);
        expect(mapRes.data[0].COL_DOUBLE).toBe(2.718281828459045);
        expect(idbRes.data[0].COL_DOUBLE).toBe(2.71828);
      });
    });

    // ----- Character types -----

    it('character types (CHAR, VARCHAR)', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_CHAR, COL_VARCHAR FROM ${DT_TABLE} WHERE ROW_ID = 1`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);
        expect(normalise(idbRes)).toEqual(normalise(mapRes));
        // CHAR(20) 'HELLO' should be trimmed by both backends
        expect(idbRes.data[0].COL_CHAR).toBe('HELLO');
        expect(idbRes.data[0].COL_VARCHAR).toBe('World of DB2 for i');
      });
    });

    it('CLOB values', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_CLOB FROM ${DT_TABLE} WHERE ROW_ID = 1`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);
        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });

    // ----- Date/time types -----

    it('date/time via VARCHAR_FORMAT (canonical)', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT
          VARCHAR_FORMAT(COL_DATE, 'YYYY-MM-DD') AS COL_DATE,
          VARCHAR_FORMAT(CAST(COL_TIME AS TIMESTAMP), 'HH24:MI:SS') AS COL_TIME,
          VARCHAR_FORMAT(COL_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.NNNNNN') AS COL_TIMESTAMP
          FROM ${DT_TABLE} WHERE ROW_ID = 1`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);
        expect(normalise(idbRes)).toEqual(normalise(mapRes));
        expect(idbRes.data[0].COL_DATE).toBe('2024-06-15');
        expect(idbRes.data[0].COL_TIME).toBe('13:45:30');
        expect(idbRes.data[0].COL_TIMESTAMP).toBe('2024-06-15 13:45:30.123456');
      });
    });

    it('date/time raw format comparison', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_DATE, COL_TIME, COL_TIMESTAMP FROM ${DT_TABLE} WHERE ROW_ID = 1`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        // With ISO format defaults injected on both backends, DATE and TIME
        // now match byte-for-byte. Note DB2's ISO time format uses '.' as the
        // separator, not ':' — the `time separator` default is therefore a
        // no-op when `time format=iso`. TIMESTAMP is driver-fixed (DB2 native
        // on idb, `YYYY-MM-DD HH:MM:SS.ffffff` on mapepire) and still diverges.
        expect(idbRes.data.length).toBe(1);
        expect(mapRes.data.length).toBe(1);
        expect(idbRes.data[0].COL_DATE).toBe('2024-06-15');
        expect(mapRes.data[0].COL_DATE).toBe('2024-06-15');
        expect(idbRes.data[0].COL_TIME).toBe('13.45.30');
        expect(mapRes.data[0].COL_TIME).toBe('13.45.30');
        // TIMESTAMP: mapepire normalises JT400 format to DB2 native
        expect(idbRes.data[0].COL_TIMESTAMP).toBe('2024-06-15-13.45.30.123456');
        expect(mapRes.data[0].COL_TIMESTAMP).toBe('2024-06-15-13.45.30.123456');
      });
    });

    it('honours caller-supplied date format override', async () => {
      await withBothBackends(
        { JDBCOptions: { 'date format': 'usa', 'date separator': '/' } as any },
        async (idb, mapepire) => {
          const sql = `VALUES CAST('2024-06-15' AS DATE)`;
          const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);
          // USA format is MM/DD/YYYY on both backends.
          const idbVal = Object.values(idbRes.data[0])[0];
          const mapVal = Object.values(mapRes.data[0])[0];
          expect(idbVal).toBe('06/15/2024');
          expect(mapVal).toBe('06/15/2024');
        },
      );
    });

    // ----- Binary types -----

    it('binary types (BINARY, VARBINARY) return Buffer on both backends', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_BINARY, COL_VARBINARY FROM ${DT_TABLE} WHERE ROW_ID = 1`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        const expectedBinary = Buffer.from('48454C4C4F0000000000000000000000', 'hex');
        const expectedVarbinary = Buffer.from('DEADBEEF', 'hex');

        for (const res of [idbRes, mapRes]) {
          expect(Buffer.isBuffer(res.data[0].COL_BINARY)).toBe(true);
          expect(Buffer.isBuffer(res.data[0].COL_VARBINARY)).toBe(true);
          expect(res.data[0].COL_BINARY.equals(expectedBinary)).toBe(true);
          expect(res.data[0].COL_VARBINARY.equals(expectedVarbinary)).toBe(true);
        }

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });

    it('BLOB values return Buffer on both backends', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_BLOB FROM ${DT_TABLE} WHERE ROW_ID = 1`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        const expected = Buffer.from([0x01, 0x02, 0x03, 0x04, 0x05]);
        for (const res of [idbRes, mapRes]) {
          expect(Buffer.isBuffer(res.data[0].COL_BLOB)).toBe(true);
          expect(res.data[0].COL_BLOB.equals(expected)).toBe(true);
        }

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });

    it('BLOB round-trip: Buffer parameter insert + select', async () => {
      const RT_TABLE = `${DT_LIB}.BLOB_RT`;
      const idb = new RmConnection({ ...baseOpts().idb });
      const mapepire = new RmConnection({ ...baseOpts().mapepire });
      await Promise.all([idb.init(true), mapepire.init(true)]);
      try {
        await idb.execute(`CREATE OR REPLACE TABLE ${RT_TABLE} (ID INTEGER NOT NULL, DATA BLOB(2M))`);

        // Small payload with varied bytes — exercises the byte-level correctness
        // of the hex encode/decode path and the driver's small-value path.
        const idbPayload = Buffer.from([0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x01, 0x80]);
        const mapepirePayload = Buffer.from('Round-trip via mapepire Buffer parameter', 'utf8');

        // Larger payloads — exercise the path past idb-pconnector's ~64 KiB
        // Buffer-mutation threshold and past mapepire's tens-of-KiB native
        // path transition. Different fill bytes on each backend so a
        // cross-read can't accidentally pass due to stale data.
        const idbLargePayload = Buffer.alloc(1024 * 1024, 0xAB);
        const mapepireLargePayload = Buffer.alloc(1024 * 1024, 0xCD);

        await idb.execute(`DELETE FROM ${RT_TABLE}`);
        await idb.execute(`INSERT INTO ${RT_TABLE} (ID, DATA) VALUES (?, ?)`, {
          parameters: [1, idbPayload],
        });
        await mapepire.execute(`INSERT INTO ${RT_TABLE} (ID, DATA) VALUES (?, ?)`, {
          parameters: [2, mapepirePayload],
        });
        await idb.execute(`INSERT INTO ${RT_TABLE} (ID, DATA) VALUES (?, ?)`, {
          parameters: [3, idbLargePayload],
        });
        await mapepire.execute(`INSERT INTO ${RT_TABLE} (ID, DATA) VALUES (?, ?)`, {
          parameters: [4, mapepireLargePayload],
        });

        const [idbRead, mapRead, idbLargeRead, mapLargeRead] = await Promise.all([
          idb.execute(`SELECT DATA FROM ${RT_TABLE} WHERE ID = ?`, { parameters: [1] }),
          mapepire.execute(`SELECT DATA FROM ${RT_TABLE} WHERE ID = ?`, { parameters: [2] }),
          idb.execute(`SELECT DATA FROM ${RT_TABLE} WHERE ID = ?`, { parameters: [3] }),
          mapepire.execute(`SELECT DATA FROM ${RT_TABLE} WHERE ID = ?`, { parameters: [4] }),
        ]);

        expect(Buffer.isBuffer(idbRead.data[0].DATA)).toBe(true);
        expect(Buffer.isBuffer(mapRead.data[0].DATA)).toBe(true);
        expect(idbRead.data[0].DATA.equals(idbPayload)).toBe(true);
        expect(mapRead.data[0].DATA.equals(mapepirePayload)).toBe(true);

        // 1 MiB round-trips — proves the defensive Buffer copy in idb.ts and
        // the hex-encoding path in mapepire.ts both preserve bytes at size.
        expect(idbLargeRead.data[0].DATA.length).toBe(idbLargePayload.length);
        expect(mapLargeRead.data[0].DATA.length).toBe(mapepireLargePayload.length);
        expect(idbLargeRead.data[0].DATA.equals(idbLargePayload)).toBe(true);
        expect(mapLargeRead.data[0].DATA.equals(mapepireLargePayload)).toBe(true);

        // Also verify the caller's Buffer was not mutated by the driver. This
        // catches any regression of the defensive copy in IdbBackend.
        expect(idbLargePayload[0]).toBe(0xAB);
        expect(idbLargePayload[idbLargePayload.length - 1]).toBe(0xAB);

        // Cross-read: each backend reads the other backend's row to prove the
        // bytes on disk are the same regardless of which backend wrote them.
        // Covers both the small and the 1 MiB pairs.
        const [idbCross, mapCross, idbLargeCross, mapLargeCross] = await Promise.all([
          idb.execute(`SELECT DATA FROM ${RT_TABLE} WHERE ID = ?`, { parameters: [2] }),
          mapepire.execute(`SELECT DATA FROM ${RT_TABLE} WHERE ID = ?`, { parameters: [1] }),
          idb.execute(`SELECT DATA FROM ${RT_TABLE} WHERE ID = ?`, { parameters: [4] }),
          mapepire.execute(`SELECT DATA FROM ${RT_TABLE} WHERE ID = ?`, { parameters: [3] }),
        ]);
        expect(idbCross.data[0].DATA.equals(mapepirePayload)).toBe(true);
        expect(mapCross.data[0].DATA.equals(idbPayload)).toBe(true);
        expect(idbLargeCross.data[0].DATA.equals(mapepireLargePayload)).toBe(true);
        expect(mapLargeCross.data[0].DATA.equals(idbLargePayload)).toBe(true);
      } finally {
        try {
          await idb.execute(`DROP TABLE ${RT_TABLE}`);
        } catch (e) {
          // best-effort cleanup
        }
        await Promise.all([idb.close(), mapepire.close()]);
      }
    });

    // ----- GRAPHIC types -----

    it('GRAPHIC and VARGRAPHIC', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_GRAPHIC, COL_VARGRAPHIC FROM ${DT_TABLE} WHERE ROW_ID = 1`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);
        expect(normalise(idbRes)).toEqual(normalise(mapRes));
      });
    });

    // ----- NULL values -----

    it('NULL values across all types', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT * FROM ${DT_TABLE} WHERE ROW_ID = 2`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        const idbRow = idbRes.data[0];
        const mapRow = mapRes.data[0];

        // Most types return null on both backends
        expect(idbRow.COL_SMALLINT).toBeNull();
        expect(idbRow.COL_INT).toBeNull();
        expect(idbRow.COL_BIGINT).toBeNull();
        expect(idbRow.COL_DECIMAL).toBeNull();
        expect(idbRow.COL_VARCHAR).toBeNull();
        expect(idbRow.COL_DATE).toBeNull();
        expect(idbRow.COL_BLOB).toBeNull();

        // DECFLOAT nulls
        expect(idbRow.COL_DF16).toBeNull();
        expect(mapRow.COL_DF16).toBeNull();
        expect(idbRow.COL_DF34).toBeNull();
        expect(mapRow.COL_DF34).toBeNull();

        // CLOB: idb returns empty string for NULL, mapepire returns null
        expect(idbRow.COL_CLOB).toBe('');
        expect(mapRow.COL_CLOB).toBeNull();
      });
    });

    // ----- Edge cases -----

    it('edge case values', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_SMALLINT, COL_INT, COL_BIGINT, COL_DECIMAL, COL_NUMERIC,
          COL_REAL, COL_DOUBLE, COL_CHAR, COL_VARCHAR
          FROM ${DT_TABLE} WHERE ROW_ID = 3`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        // Types that match across backends
        expect(idbRes.data[0].COL_SMALLINT).toBe(mapRes.data[0].COL_SMALLINT);
        expect(idbRes.data[0].COL_INT).toBe(mapRes.data[0].COL_INT);
        expect(idbRes.data[0].COL_SMALLINT).toBe(-32768);
        expect(idbRes.data[0].COL_INT).toBe(-2147483648);
        expect(idbRes.data[0].COL_DECIMAL).toBe(0);
        expect(idbRes.data[0].COL_VARCHAR).toBe('');

        // BIGINT: idb returns string, mapepire returns number
        expect(idbRes.data[0].COL_BIGINT).toBe('-9007199254740000');
        expect(mapRes.data[0].COL_BIGINT).toBe(-9007199254740000);

        // DOUBLE: idb truncates to ~6 significant digits
        const idbDouble = idbRes.data[0].COL_DOUBLE;
        const mapDouble = mapRes.data[0].COL_DOUBLE;
        const relError = Math.abs((idbDouble - mapDouble) / mapDouble);
        expect(relError).toBeLessThan(1e-5);
      });
    });

    // ----- DECFLOAT -----

    it('DECFLOAT(16) and DECFLOAT(34) values', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_DF16, COL_DF34 FROM ${DT_TABLE} WHERE ROW_ID = 1`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        // idb returns DECFLOAT as strings (preserving full precision),
        // mapepire returns as numbers (truncated to JS double precision)
        expect(idbRes.data[0].COL_DF16).toBe('12345.6789');
        expect(mapRes.data[0].COL_DF16).toBe(12345.6789);
        expect(idbRes.data[0].COL_DF34).toBe('98765432101234.5678901234567890');
        expect(mapRes.data[0].COL_DF34).toBe(98765432101234.56);
      });
    });

    // ----- BOOLEAN (V7R4+) -----

    describe('BOOLEAN (V7R4+)', () => {
      const BOOL_TABLE = `${DT_LIB}.DT_BOOL`;
      let supported = true;

      beforeAll(async () => {
        const setup = new RmConnection({ backend: 'idb' });
        await setup.init(true);
        try {
          await setup.execute(`CREATE OR REPLACE TABLE ${BOOL_TABLE} (
            ROW_ID INT NOT NULL, COL_BOOL BOOLEAN
          )`);
          await setup.execute(`INSERT INTO ${BOOL_TABLE} VALUES (1, TRUE)`);
          await setup.execute(`INSERT INTO ${BOOL_TABLE} VALUES (2, FALSE)`);
          await setup.execute(`INSERT INTO ${BOOL_TABLE} VALUES (3, NULL)`);
        } catch (e) {
          supported = false;
          console.log('BOOLEAN type not supported on this IBM i version, skipping');
        } finally {
          await setup.close();
        }
      });

      afterAll(async () => {
        if (!supported) return;
        const teardown = new RmConnection({ backend: 'idb' });
        await teardown.init(true);
        try {
          await teardown.execute(`DROP TABLE ${BOOL_TABLE}`);
        } catch (e) { /* best-effort */ }
        finally { await teardown.close(); }
      });

      // Note: idb-pconnector returns BOOLEAN as strings due to a buffer handling
      // issue in nodejs-idb-connector. A fix for garbage bytes after "FALSE" is
      // pending upstream: https://github.com/IBM/nodejs-idb-connector/pull/191
      it('BOOLEAN true/false/null values match', async () => {
        if (!supported) return;
        await withBothBackends({}, async (idb, mapepire) => {
          const sql = `SELECT ROW_ID, COL_BOOL FROM ${BOOL_TABLE} ORDER BY ROW_ID`;
          const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

          // mapepire returns native booleans
          expect(mapRes.data[0].COL_BOOL).toBe(true);
          expect(mapRes.data[1].COL_BOOL).toBe(false);
          expect(mapRes.data[2].COL_BOOL).toBeNull();

          // idb returns BOOLEAN as strings; pending upstream fix, FALSE may
          // contain trailing garbage bytes so we use startsWith
          expect(idbRes.data[0].COL_BOOL).toBe('TRUE');
          expect(typeof idbRes.data[1].COL_BOOL).toBe('string');
          expect(idbRes.data[1].COL_BOOL.startsWith('FALSE')).toBe(true);
          expect(idbRes.data[2].COL_BOOL).toBeNull();
        });
      });
    });

  });

  // ===================================================================
  // CCSID & UTF-8 character encoding parity
  //
  // Tests that non-ASCII and Unicode character data round-trips
  // correctly through both backends and produces identical results.
  // ===================================================================

  describe('CCSID & UTF-8 character encoding', () => {
    const ENC_LIB = 'PARITYTEST';
    const ENC_TABLE = `${ENC_LIB}.ENCODING`;

    beforeAll(async () => {
      const setup = new RmConnection({ backend: 'idb' });
      await setup.init(true);
      try {
        try {
          await setup.execute(`CREATE SCHEMA ${ENC_LIB}`);
        } catch (e: any) {
          if (!e?.message?.includes('SQLCODE=-601')) throw e;
        }

        await setup.execute(`CREATE OR REPLACE TABLE ${ENC_TABLE} (
          ROW_ID         INTEGER NOT NULL,
          ROW_LABEL      VARCHAR(30) NOT NULL,
          COL_CHAR       CHAR(40),
          COL_VARCHAR    VARCHAR(200),
          COL_GRAPHIC    GRAPHIC(20) CCSID 13488,
          COL_VARGRAPHIC VARGRAPHIC(100) CCSID 13488,
          COL_UTF8       VARCHAR(200) CCSID 1208
        )`);

        // Row 1: accented Latin characters
        await setup.execute(`INSERT INTO ${ENC_TABLE} (ROW_ID, ROW_LABEL, COL_CHAR, COL_VARCHAR) VALUES (1, 'ACCENTED_LATIN', 'café résumé naïve', 'über straße Ñoño')`);

        // Row 2: Western European mix (diacritics, ligatures, symbols)
        await setup.execute(`INSERT INTO ${ENC_TABLE} (ROW_ID, ROW_LABEL, COL_CHAR, COL_VARCHAR) VALUES (2, 'WESTERN_EURO', 'àáâãäåæçèéêë', 'ìíîïðñòóôõöùúûüý')`);

        // Row 3: CJK characters in GRAPHIC/VARGRAPHIC
        await setup.execute(`INSERT INTO ${ENC_TABLE} (ROW_ID, ROW_LABEL, COL_GRAPHIC, COL_VARGRAPHIC) VALUES (3, 'CJK', '日本語テスト', '中文数据库测试')`);

        // Row 4: mixed scripts in GRAPHIC/VARGRAPHIC
        await setup.execute(`INSERT INTO ${ENC_TABLE} (ROW_ID, ROW_LABEL, COL_GRAPHIC, COL_VARGRAPHIC) VALUES (4, 'MIXED_SCRIPT', 'Hello 世界', 'café 東京 über')`);

        // Row 5: accented characters in GRAPHIC/VARGRAPHIC
        await setup.execute(`INSERT INTO ${ENC_TABLE} (ROW_ID, ROW_LABEL, COL_GRAPHIC, COL_VARGRAPHIC) VALUES (5, 'ACCENTED_GRAPHIC', 'àéîõü', 'ñçßøå')`);

        // Row 6: emoji in VARGRAPHIC (supplementary Unicode / non-BMP)
        // CCSID 13488 is UCS-2 which cannot represent characters above U+FFFF;
        // this tests whether the database accepts or rejects them
        try {
          await setup.execute(`INSERT INTO ${ENC_TABLE} (ROW_ID, ROW_LABEL, COL_VARGRAPHIC) VALUES (6, 'EMOJI', '😀🎉🚀')`);
        } catch (e) {
          // Expected — UCS-2 cannot represent supplementary characters.
          // Insert a marker row so the test can detect this.
          await setup.execute(`INSERT INTO ${ENC_TABLE} (ROW_ID, ROW_LABEL) VALUES (6, 'EMOJI_UNSUPPORTED')`);
        }

        // Row 7: UTF-8 column with multi-byte content (Latin + CJK mix)
        await setup.execute(`INSERT INTO ${ENC_TABLE} (ROW_ID, ROW_LABEL, COL_UTF8) VALUES (7, 'UTF8_MULTIBYTE', 'café 日本語 über')`);

        // Row 8: UTF-8 column with Greek and Cyrillic
        await setup.execute(`INSERT INTO ${ENC_TABLE} (ROW_ID, ROW_LABEL, COL_UTF8) VALUES (8, 'UTF8_EXTENDED', 'αβγδ АБВГ')`);

        // Row 9: multi-byte length semantics — 'café' is 4 chars but 5 bytes in UTF-8
        await setup.execute(`INSERT INTO ${ENC_TABLE} (ROW_ID, ROW_LABEL, COL_CHAR, COL_VARCHAR, COL_UTF8) VALUES (9, 'LENGTH_TEST', 'café', 'café', 'café')`);
      } finally {
        await setup.close();
      }
    }, 60_000);

    afterAll(async () => {
      const teardown = new RmConnection({ backend: 'idb' });
      await teardown.init(true);
      try {
        await teardown.execute(`DROP TABLE ${ENC_TABLE}`);
      } catch (e) { /* best-effort */ }
      finally { await teardown.close(); }
    });

    it('accented Latin characters in CHAR/VARCHAR', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_CHAR, COL_VARCHAR FROM ${ENC_TABLE} WHERE ROW_ID = 1`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        // Both backends should return identical character data
        expect(normalise(idbRes)).toEqual(normalise(mapRes));

        // Verify the actual content survived the round-trip
        expect(idbRes.data[0].COL_CHAR).toBe('café résumé naïve');
        expect(idbRes.data[0].COL_VARCHAR).toBe('über straße Ñoño');
      });
    });

    it('Western European diacritics and special characters', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_CHAR, COL_VARCHAR FROM ${ENC_TABLE} WHERE ROW_ID = 2`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));

        expect(idbRes.data[0].COL_CHAR).toBe('àáâãäåæçèéêë');
        expect(idbRes.data[0].COL_VARCHAR).toBe('ìíîïðñòóôõöùúûüý');
      });
    });

    it('CJK characters in GRAPHIC/VARGRAPHIC', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_GRAPHIC, COL_VARGRAPHIC FROM ${ENC_TABLE} WHERE ROW_ID = 3`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));

        expect(idbRes.data[0].COL_GRAPHIC).toBe('日本語テスト');
        expect(idbRes.data[0].COL_VARGRAPHIC).toBe('中文数据库测试');
      });
    });

    it('mixed scripts in GRAPHIC/VARGRAPHIC', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_GRAPHIC, COL_VARGRAPHIC FROM ${ENC_TABLE} WHERE ROW_ID = 4`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));

        expect(idbRes.data[0].COL_GRAPHIC).toBe('Hello 世界');
        expect(idbRes.data[0].COL_VARGRAPHIC).toBe('café 東京 über');
      });
    });

    it('accented characters in GRAPHIC/VARGRAPHIC', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_GRAPHIC, COL_VARGRAPHIC FROM ${ENC_TABLE} WHERE ROW_ID = 5`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));

        expect(idbRes.data[0].COL_GRAPHIC).toBe('àéîõü');
        expect(idbRes.data[0].COL_VARGRAPHIC).toBe('ñçßøå');
      });
    });

    it('emoji/supplementary Unicode in VARGRAPHIC (UCS-2 boundary)', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT ROW_LABEL, COL_VARGRAPHIC FROM ${ENC_TABLE} WHERE ROW_ID = 6`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        const label = idbRes.data[0].ROW_LABEL;

        if (label === 'EMOJI_UNSUPPORTED') {
          // UCS-2 rejected the supplementary characters — both backends
          // should see the marker row with no VARGRAPHIC data
          expect(idbRes.data[0].COL_VARGRAPHIC).toBeNull();
          expect(mapRes.data[0].COL_VARGRAPHIC).toBeNull();
        } else {
          // Database accepted the emoji — verify both backends agree
          expect(normalise(idbRes)).toEqual(normalise(mapRes));
          expect(idbRes.data[0].COL_VARGRAPHIC).toBe('😀🎉🚀');
        }
      });
    });

    it('UTF-8 column with multi-byte content (VARCHAR CCSID 1208)', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_UTF8 FROM ${ENC_TABLE} WHERE ROW_ID = 7`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
        expect(idbRes.data[0].COL_UTF8).toBe('café 日本語 über');
      });
    });

    it('UTF-8 column with Greek and Cyrillic characters', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_UTF8 FROM ${ENC_TABLE} WHERE ROW_ID = 8`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
        expect(idbRes.data[0].COL_UTF8).toBe('αβγδ АБВГ');
      });
    });

    it('multi-byte character length semantics (CHAR padding and trimming)', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT COL_CHAR, COL_VARCHAR, COL_UTF8 FROM ${ENC_TABLE} WHERE ROW_ID = 9`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));

        // All three columns should return 'café' with correct character count
        // despite é being multi-byte in UTF-8
        expect(idbRes.data[0].COL_CHAR).toBe('café');
        expect(idbRes.data[0].COL_VARCHAR).toBe('café');
        expect(idbRes.data[0].COL_UTF8).toBe('café');

        // JavaScript string length should be 4 characters, not 5 bytes
        expect(idbRes.data[0].COL_CHAR.length).toBe(4);
        expect(idbRes.data[0].COL_VARCHAR.length).toBe(4);
        expect(idbRes.data[0].COL_UTF8.length).toBe(4);
      });
    });

    it('all encoding rows match across backends', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `SELECT ROW_ID, ROW_LABEL, COL_CHAR, COL_VARCHAR, COL_GRAPHIC, COL_VARGRAPHIC, COL_UTF8 FROM ${ENC_TABLE} ORDER BY ROW_ID`;
        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        // Same number of rows
        expect(idbRes.data.length).toBe(mapRes.data.length);

        // Every row should produce identical data from both backends
        for (let i = 0; i < idbRes.data.length; i++) {
          expect(idbRes.data[i]).toEqual(mapRes.data[i]);
        }
      });
    });
  });

  // ===================================================================
  // CCSID & UTF-8 write-path parity
  //
  // The tests above insert data via idb and read from both backends.
  // These tests verify that each backend can independently round-trip
  // non-ASCII character data (write then read within the same backend).
  // ===================================================================

  describe('CCSID & UTF-8 write-path parity', () => {
    const WP_LIB = 'PARITYTEST';
    const WP_TABLE = `${WP_LIB}.ENC_WRITE`;

    beforeAll(async () => {
      const setup = new RmConnection({ backend: 'idb' });
      await setup.init(true);
      try {
        try {
          await setup.execute(`CREATE SCHEMA ${WP_LIB}`);
        } catch (e: any) {
          if (!e?.message?.includes('SQLCODE=-601')) throw e;
        }

        await setup.execute(`CREATE OR REPLACE TABLE ${WP_TABLE} (
          ROW_ID         INTEGER NOT NULL,
          ROW_LABEL      VARCHAR(30) NOT NULL,
          COL_CHAR       CHAR(40),
          COL_VARCHAR    VARCHAR(200),
          COL_GRAPHIC    GRAPHIC(20) CCSID 13488,
          COL_VARGRAPHIC VARGRAPHIC(100) CCSID 13488,
          COL_UTF8       VARCHAR(200) CCSID 1208
        )`);
      } finally {
        await setup.close();
      }
    }, 60_000);

    afterAll(async () => {
      const teardown = new RmConnection({ backend: 'idb' });
      await teardown.init(true);
      try {
        await teardown.execute(`DROP TABLE ${WP_TABLE}`);
      } catch (e) { /* best-effort */ }
      finally { await teardown.close(); }
    });

    it('idb write and read round-trip (all column types)', async () => {
      const conn = new RmConnection({ backend: 'idb' });
      await conn.init(true);
      try {
        await conn.execute(`INSERT INTO ${WP_TABLE} (ROW_ID, ROW_LABEL, COL_CHAR, COL_VARCHAR, COL_GRAPHIC, COL_VARGRAPHIC, COL_UTF8) VALUES (1, 'IDB_ROUNDTRIP', 'café résumé', 'über straße', '日本語テスト', 'café 東京 über', 'αβγδ АБВГ')`);

        const res = await conn.execute(`SELECT COL_CHAR, COL_VARCHAR, COL_GRAPHIC, COL_VARGRAPHIC, COL_UTF8 FROM ${WP_TABLE} WHERE ROW_ID = 1`);

        expect(res.data[0].COL_CHAR).toBe('café résumé');
        expect(res.data[0].COL_VARCHAR).toBe('über straße');
        expect(res.data[0].COL_GRAPHIC).toBe('日本語テスト');
        expect(res.data[0].COL_VARGRAPHIC).toBe('café 東京 über');
        expect(res.data[0].COL_UTF8).toBe('αβγδ АБВГ');
      } finally {
        await conn.close();
      }
    });

    it('mapepire write and read round-trip (all column types)', async () => {
      const conn = new RmConnection({ backend: 'mapepire', creds: MAPEPIRE_CREDS });
      await conn.init(true);
      try {
        await conn.execute(`INSERT INTO ${WP_TABLE} (ROW_ID, ROW_LABEL, COL_CHAR, COL_VARCHAR, COL_GRAPHIC, COL_VARGRAPHIC, COL_UTF8) VALUES (2, 'MAP_ROUNDTRIP', 'café résumé', 'über straße', '日本語テスト', 'café 東京 über', 'αβγδ АБВГ')`);

        const res = await conn.execute(`SELECT COL_CHAR, COL_VARCHAR, COL_GRAPHIC, COL_VARGRAPHIC, COL_UTF8 FROM ${WP_TABLE} WHERE ROW_ID = 2`);

        expect(res.data[0].COL_CHAR).toBe('café résumé');
        expect(res.data[0].COL_VARCHAR).toBe('über straße');
        expect(res.data[0].COL_GRAPHIC).toBe('日本語テスト');
        expect(res.data[0].COL_VARGRAPHIC).toBe('café 東京 über');
        expect(res.data[0].COL_UTF8).toBe('αβγδ АБВГ');
      } finally {
        await conn.close();
      }
    });
  });

  // ===================================================================
  // Stored procedure output parameters
  //
  // Tests that output parameters from stored procedures are returned
  // correctly on both backends. A test procedure is created with IN,
  // OUT, and INOUT parameters of various types.
  //
  // Known differences (see docs/BACKEND-DIFFERENCES.md):
  //   - Both backends return entries for all parameters (IN + OUT + INOUT)
  //   - idb: value present on all parameters; mapepire: value only on OUT/INOUT
  //   - mapepire includes extra metadata (type, name, precision, scale, ccsid)
  //   - NULL OUT params: idb returns zero values (""/0), mapepire returns null
  // ===================================================================

  describe('Stored procedure output parameters', () => {
    const SP_LIB = 'PARITYTEST';
    const SP_NAME = `${SP_LIB}.PARITY_SP`;

    beforeAll(async () => {
      const setup = new RmConnection({ backend: 'idb' });
      await setup.init(true);
      try {
        try {
          await setup.execute(`CREATE SCHEMA ${SP_LIB}`);
        } catch (e: any) {
          if (!e?.message?.includes('SQLCODE=-601')) throw e;
        }

        await setup.execute(`CREATE OR REPLACE PROCEDURE ${SP_NAME} (
          IN  P_NAME     VARCHAR(50),
          IN  P_AMOUNT   DECIMAL(9,2),
          OUT P_GREETING VARCHAR(100),
          OUT P_DOUBLED  DECIMAL(11,2),
          INOUT P_COUNTER INTEGER
        )
        LANGUAGE SQL
        BEGIN
          SET P_GREETING = 'Hello, ' CONCAT TRIM(P_NAME) CONCAT '!';
          SET P_DOUBLED  = P_AMOUNT * 2;
          SET P_COUNTER  = P_COUNTER + 1;
        END`);
      } finally {
        await setup.close();
      }
    }, 60_000);

    afterAll(async () => {
      const teardown = new RmConnection({ backend: 'idb' });
      await teardown.init(true);
      try {
        await teardown.execute(`DROP PROCEDURE ${SP_NAME}`);
      } catch (e) {
        // Best-effort cleanup
      } finally {
        await teardown.close();
      }
    });

    it('basic OUT parameter values match', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `CALL ${SP_NAME}(?, ?, ?, ?, ?)`;
        const opts = { parameters: ['Alice', 12.50, '', 0, 5] };

        const [idbRes, mapRes] = await Promise.all([
          idb.execute(sql, opts),
          mapepire.execute(sql, opts),
        ]);

        expect(idbRes.success).toBe(true);
        expect(mapRes.success).toBe(true);

        const idbParms = (idbRes as any).output_parms;
        const mapParms = (mapRes as any).output_parms;

        expect(idbParms).toBeDefined();
        expect(mapParms).toBeDefined();

        // Both backends should return the correct output values.
        // Find by index — idb uses 1-based indexing for all params,
        // mapepire may only include output params.
        const idbGreeting = idbParms.find((p: any) => p.index === 3);
        const mapGreeting = mapParms.find((p: any) => p.index === 3);
        expect(idbGreeting.value).toBe('Hello, Alice!');
        expect(mapGreeting.value).toBe('Hello, Alice!');

        const idbDoubled = idbParms.find((p: any) => p.index === 4);
        const mapDoubled = mapParms.find((p: any) => p.index === 4);
        expect(Number(idbDoubled.value)).toBe(25.00);
        expect(Number(mapDoubled.value)).toBe(25.00);

        // INOUT parameter
        const idbCounter = idbParms.find((p: any) => p.index === 5);
        const mapCounter = mapParms.find((p: any) => p.index === 5);
        expect(Number(idbCounter.value)).toBe(6);
        expect(Number(mapCounter.value)).toBe(6);
      });
    });

    it('both backends return all parameters but differ on IN param values', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `CALL ${SP_NAME}(?, ?, ?, ?, ?)`;
        const opts = { parameters: ['Bob', 7.00, '', 0, 10] };

        const [idbRes, mapRes] = await Promise.all([
          idb.execute(sql, opts),
          mapepire.execute(sql, opts),
        ]);

        const idbParms = (idbRes as any).output_parms;
        const mapParms = (mapRes as any).output_parms;

        // Both backends return entries for ALL parameters (input + output)
        expect(idbParms.length).toBe(5);
        expect(mapParms.length).toBe(5);

        // Both have entries for IN parameters (index 1 and 2)
        expect(idbParms.find((p: any) => p.index === 1)).toBeDefined();
        expect(mapParms.find((p: any) => p.index === 1)).toBeDefined();

        // idb includes value on IN parameters
        const idbInParam = idbParms.find((p: any) => p.index === 1);
        expect(idbInParam.value).toBe('Bob');

        // mapepire does NOT include value on IN parameters
        const mapInParam = mapParms.find((p: any) => p.index === 1);
        expect(mapInParam.value).toBeUndefined();
      });
    });

    it('mapepire includes extra metadata fields', async () => {
      await withBothBackends({}, async (idb, mapepire) => {
        const sql = `CALL ${SP_NAME}(?, ?, ?, ?, ?)`;
        const opts = { parameters: ['Charlie', 1.00, '', 0, 0] };

        const [idbRes, mapRes] = await Promise.all([
          idb.execute(sql, opts),
          mapepire.execute(sql, opts),
        ]);

        const idbParms = (idbRes as any).output_parms;
        const mapParms = (mapRes as any).output_parms;

        const idbGreeting = idbParms.find((p: any) => p.index === 3);
        const mapGreeting = mapParms.find((p: any) => p.index === 3);

        // idb output_parms entries only have index and value
        expect(Object.keys(idbGreeting).sort()).toEqual(['index', 'value']);

        // mapepire output_parms entries include additional metadata
        expect(mapGreeting).toHaveProperty('type');
        expect(mapGreeting).toHaveProperty('name');
        expect(mapGreeting).toHaveProperty('precision');
        expect(mapGreeting).toHaveProperty('scale');
        expect(mapGreeting).toHaveProperty('ccsid');
      });
    });

    it('NULL output parameter values across types', async () => {
      // idb never returns null for NULL OUT params — it returns the type's
      // zero value (empty string for string/date types, 0 for numerics).
      // mapepire returns null for all types.
      const SP_NULL = `${SP_LIB}.PARITY_SP_NULL`;
      const setup = new RmConnection({ backend: 'idb' });
      await setup.init(true);
      try {
        await setup.execute(`CREATE OR REPLACE PROCEDURE ${SP_NULL} (
          OUT P_VARCHAR    VARCHAR(50),
          OUT P_INT        INTEGER,
          OUT P_DECIMAL    DECIMAL(9,2),
          OUT P_DATE       DATE,
          OUT P_CLOB       CLOB(1K),
          OUT P_SMALLINT   SMALLINT,
          OUT P_NUMERIC    NUMERIC(9,2),
          OUT P_CHAR       CHAR(20),
          OUT P_TIME       TIME,
          OUT P_TIMESTAMP  TIMESTAMP,
          OUT P_BLOB       BLOB(1K)
        )
        LANGUAGE SQL
        BEGIN
          SET P_VARCHAR   = NULL;
          SET P_INT       = NULL;
          SET P_DECIMAL   = NULL;
          SET P_DATE      = NULL;
          SET P_CLOB      = NULL;
          SET P_SMALLINT  = NULL;
          SET P_NUMERIC   = NULL;
          SET P_CHAR      = NULL;
          SET P_TIME      = NULL;
          SET P_TIMESTAMP = NULL;
          SET P_BLOB      = NULL;
        END`);
      } finally {
        await setup.close();
      }

      try {
        await withBothBackends({}, async (idb, mapepire) => {
          const sql = `CALL ${SP_NULL}(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
          const opts = { parameters: ['', 0, 0, '', '', 0, 0, '', '', '', ''] };

          const [idbRes, mapRes] = await Promise.all([
            idb.execute(sql, opts),
            mapepire.execute(sql, opts),
          ]);

          const idbParms = (idbRes as any).output_parms;
          const mapParms = (mapRes as any).output_parms;

          // mapepire returns null for all NULL OUT params
          for (let i = 1; i <= 11; i++) {
            const mapVal = mapParms.find((p: any) => p.index === i);
            expect(mapVal.value).toBeNull();
          }

          // idb returns zero values instead of null:
          // string/date/time/clob → empty string, numeric → 0
          const idbVarchar = idbParms.find((p: any) => p.index === 1);
          expect(idbVarchar.value).toBe('');

          const idbInt = idbParms.find((p: any) => p.index === 2);
          expect(idbInt.value).toBe(0);

          const idbDecimal = idbParms.find((p: any) => p.index === 3);
          expect(idbDecimal.value).toBe(0);

          const idbDate = idbParms.find((p: any) => p.index === 4);
          expect(idbDate.value).toBe('');

          const idbClob = idbParms.find((p: any) => p.index === 5);
          expect(idbClob.value).toBe('');

          const idbSmallint = idbParms.find((p: any) => p.index === 6);
          expect(idbSmallint.value).toBe(0);

          const idbNumeric = idbParms.find((p: any) => p.index === 7);
          expect(idbNumeric.value).toBe(0);

          const idbChar = idbParms.find((p: any) => p.index === 8);
          expect(idbChar.value).toBe('');

          const idbTime = idbParms.find((p: any) => p.index === 9);
          expect(idbTime.value).toBe('');

          const idbTimestamp = idbParms.find((p: any) => p.index === 10);
          expect(idbTimestamp.value).toBe('');

          const idbBlob = idbParms.find((p: any) => p.index === 11);
          expect(idbBlob.value).toBe('');
        });
      } finally {
        const teardown = new RmConnection({ backend: 'idb' });
        await teardown.init(true);
        try {
          await teardown.execute(`DROP PROCEDURE ${SP_NULL}`);
        } catch (e) {
          // Best-effort
        } finally {
          await teardown.close();
        }
      }
    });

    it('multiple output parameters with different types', async () => {
      const SP_MULTI = `${SP_LIB}.PARITY_SP_MULTI`;
      const setup = new RmConnection({ backend: 'idb' });
      await setup.init(true);
      try {
        await setup.execute(`CREATE OR REPLACE PROCEDURE ${SP_MULTI} (
          OUT P_STR    VARCHAR(50),
          OUT P_INT    INTEGER,
          OUT P_DEC    DECIMAL(9,2),
          OUT P_DATE   DATE
        )
        LANGUAGE SQL
        BEGIN
          SET P_STR  = 'test output';
          SET P_INT  = 42;
          SET P_DEC  = 123.45;
          SET P_DATE = DATE('2024-06-15');
        END`);
      } finally {
        await setup.close();
      }

      try {
        await withBothBackends({}, async (idb, mapepire) => {
          const sql = `CALL ${SP_MULTI}(?, ?, ?, ?)`;
          const opts = { parameters: ['', 0, 0, ''] };

          const [idbRes, mapRes] = await Promise.all([
            idb.execute(sql, opts),
            mapepire.execute(sql, opts),
          ]);

          const idbParms = (idbRes as any).output_parms;
          const mapParms = (mapRes as any).output_parms;

          // VARCHAR output
          const idbStr = idbParms.find((p: any) => p.index === 1);
          const mapStr = mapParms.find((p: any) => p.index === 1);
          expect(idbStr.value).toBe('test output');
          expect(mapStr.value).toBe('test output');

          // INTEGER output
          const idbInt = idbParms.find((p: any) => p.index === 2);
          const mapInt = mapParms.find((p: any) => p.index === 2);
          expect(Number(idbInt.value)).toBe(42);
          expect(Number(mapInt.value)).toBe(42);

          // DECIMAL output
          const idbDec = idbParms.find((p: any) => p.index === 3);
          const mapDec = mapParms.find((p: any) => p.index === 3);
          expect(Number(idbDec.value)).toBe(123.45);
          expect(Number(mapDec.value)).toBe(123.45);

          // DATE output — both should return a date string
          const idbDate = idbParms.find((p: any) => p.index === 4);
          const mapDate = mapParms.find((p: any) => p.index === 4);
          expect(idbDate.value).toBeDefined();
          expect(mapDate.value).toBeDefined();
          // The actual format may differ (see docs/BACKEND-DIFFERENCES.md raw DATE format)
          // but both should represent the same date
        });
      } finally {
        const teardown = new RmConnection({ backend: 'idb' });
        await teardown.init(true);
        try {
          await teardown.execute(`DROP PROCEDURE ${SP_MULTI}`);
        } catch (e) {
          // Best-effort
        } finally {
          await teardown.close();
        }
      }
    });
  });

  // ===================================================================
  // BLOB sizing (gated) — only runs when RM_RUN_SIZING=1
  //
  // Measures the practical ceiling for BLOB round-trips on each backend.
  // The mapepire backend goes through a hex-string intermediate, so the
  // memory footprint is roughly 3x the payload during conversion.
  //
  // Run with:  RM_RUN_SIZING=1 npm run test:parity -- --testNamePattern=BLOB-sizing
  // ===================================================================

  const runSizing = process.env.RM_RUN_SIZING === '1';
  const describeSizing = runSizing ? describe : describe.skip;

  describeSizing('BLOB-sizing (gated)', () => {
    // Single it() that iterates sizes with shared connections. Opening a fresh
    // connection pair per size triggers a Node/ws zlib assertion on some
    // platforms under rapid WebSocket churn (node_zlib.cc: zlib_memory_ == 0).
    jest.setTimeout(900_000); // 15 minutes — 128 MiB round-trips are slow

    const SIZING_LIB = 'PARITYTEST';
    const SIZING_TABLE = `${SIZING_LIB}.BLOB_SIZING`;

    const SIZES: [string, number][] = [
      ['1 KiB',   1 * 1024],
      ['64 KiB',  64 * 1024],
      ['1 MiB',   1 * 1024 * 1024],
      ['8 MiB',   8 * 1024 * 1024],
      ['16 MiB',  16 * 1024 * 1024],
      ['24 MiB',  24 * 1024 * 1024],
      ['32 MiB',  32 * 1024 * 1024],
      ['128 MiB', 128 * 1024 * 1024],
    ];

    it('round-trips a range of BLOB sizes on both backends', async () => {
      // Write directly to stdout/stderr to bypass Jest's console interception
      // (which prefixes each line with "console.log" and a source location,
      // making the diagnostic sweep hard to read).
      const write = (s: string) => process.stdout.write(`${s}\n`);
      const writeErr = (s: string) => process.stderr.write(`${s}\n`);

      // Shared connections for the whole suite — one connect + one close per
      // backend to minimise WebSocket lifecycle churn.
      const idb = new RmConnection({ backend: 'idb' });
      const mapepire = new RmConnection({ backend: 'mapepire', creds: MAPEPIRE_CREDS });
      await Promise.all([idb.init(true), mapepire.init(true)]);

      // Per-backend results: largest size that round-tripped successfully, and
      // the error message (if any) that ended the run.
      const results: Record<string, { largestSuccess: string | null; stopReason: string | null }> = {
        idb: { largestSuccess: null, stopReason: null },
        mapepire: { largestSuccess: null, stopReason: null },
      };

      async function runSize(name: string, conn: RmConnection, id: number, label: string, size: number): Promise<boolean> {
        const payload = Buffer.alloc(size, 0xAB);
        try {
          await conn.execute(`DELETE FROM ${SIZING_TABLE} WHERE ID = ?`, { parameters: [id] });

          const insertStart = Date.now();
          await conn.execute(
            `INSERT INTO ${SIZING_TABLE} (ID, DATA) VALUES (?, ?)`,
            { parameters: [id, payload] },
          );
          const insertMs = Date.now() - insertStart;

          const selectStart = Date.now();
          const read = await conn.execute(
            `SELECT DATA FROM ${SIZING_TABLE} WHERE ID = ?`,
            { parameters: [id] },
          );
          const selectMs = Date.now() - selectStart;

          const gotBuf: Buffer = read.data[0].DATA;
          if (!Buffer.isBuffer(gotBuf) || gotBuf.length !== size || !gotBuf.equals(payload)) {
            throw new Error(
              `round-trip mismatch: got isBuffer=${Buffer.isBuffer(gotBuf)} length=${gotBuf?.length} equals=${gotBuf?.equals?.(payload)}`,
            );
          }

          const heapMiB = (process.memoryUsage().heapUsed / (1024 * 1024)).toFixed(1);
          write(`  [${name}] ${label}: insert=${insertMs}ms select=${selectMs}ms heap=${heapMiB}MiB OK`);
          results[name].largestSuccess = label;
          return true;
        } catch (e: any) {
          const msg = e?.message || String(e);
          writeErr(`  [${name}] ${label}: LIMIT HIT — ${msg}`);
          results[name].stopReason = `${label}: ${msg}`;
          return false;
        }
      }

      try {
        try {
          await idb.execute(`CREATE SCHEMA ${SIZING_LIB}`);
        } catch (e: any) {
          if (!e?.message?.includes('SQLCODE=-601')) throw e;
        }
        await idb.execute(
          `CREATE OR REPLACE TABLE ${SIZING_TABLE} (ID INTEGER NOT NULL, DATA BLOB(256M))`,
        );

        // Run each backend independently so one hitting its ceiling does not
        // stop the other from finishing. Within a backend, stop at the first
        // failure — larger sizes will fail the same way.
        for (const [label, size] of SIZES) {
          write(`\n--- ${label} (${size.toLocaleString()} bytes) [idb] ---`);
          const ok = await runSize('idb', idb, 1, label, size);
          if (!ok) break;
        }

        for (const [label, size] of SIZES) {
          write(`\n--- ${label} (${size.toLocaleString()} bytes) [mapepire] ---`);
          const ok = await runSize('mapepire', mapepire, 2, label, size);
          if (!ok) break;
        }

        try {
          await idb.execute(`DROP TABLE ${SIZING_TABLE}`);
        } catch (e) {
          // best-effort
        }
      } finally {
        await Promise.all([idb.close(), mapepire.close()]);
      }

      write('\n=== BLOB sizing summary ===');
      for (const name of ['idb', 'mapepire']) {
        const r = results[name];
        write(
          `  ${name}: largest success = ${r.largestSuccess ?? 'none'}` +
          (r.stopReason ? `, stopped at ${r.stopReason}` : ''),
        );
      }

      // The test is diagnostic — we don't fail the run just because a backend
      // hits a ceiling. We only fail if no size succeeded on either backend.
      expect(results.idb.largestSuccess).not.toBeNull();
      expect(results.mapepire.largestSuccess).not.toBeNull();
    });
  });
});
