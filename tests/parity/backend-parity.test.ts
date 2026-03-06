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

/** Fields that are expected to differ between backends (see BACKEND-DIFFERENCES.md) */
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
        // Use DATE only — TIMESTAMP includes fractional seconds that may differ
        const sql = 'VALUES CURRENT DATE';

        const [idbRes, mapRes] = await Promise.all([idb.execute(sql), mapepire.execute(sql)]);

        expect(normalise(idbRes)).toEqual(normalise(mapRes));
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

  // ----- initCommands -----

  describe('initCommands', () => {
    it('CL init commands execute on both backends', async () => {
      await withBothBackends(
        { initCommands: [{ command: 'CHGJOB INQMSGRPY(*DFT)', type: 'cl' }] },
        async (idb, mapepire) => {
          const sql = 'SELECT CUSNUM FROM QIWS.QCUSTCDT FETCH FIRST 1 ROW ONLY';

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
});
