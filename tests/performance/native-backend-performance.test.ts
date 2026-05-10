/**
 * Native Backend Performance Tests
 *
 * Mirrors rm-backend-performance.test.ts row-for-row, but every query goes
 * through the native driver API directly — no rm-connector-js wrapper.
 * The intent is to measure rm-connector-js's wrapper overhead by
 * running this with the same QUERY_COUNT and SAMPLE_SCHEMA as
 * rm-backend-performance, then diffing the two output tables.
 *
 * Asymmetry warning (same as the rm-connector-js suite):
 *   The Pool Promise.all row uses native @ibm/mapepire-js Pool, which
 *   multiplexes unconditionally, vs idb-pconnector DBPool which is
 *   one-query-at-a-time per connection. The mapepire-side number
 *   therefore reflects multiplexing, not raw protocol overhead.
 *
 * idb-pconnector is a native addon that only builds on IBM i. When this
 * file is run on any other platform (e.g. a workstation pointing at a
 * remote IBM i), the idb branch of every test is skipped and only the
 * mapepire side runs — so the suite can still produce remote mapepire
 * baseline data.
 *
 * Run with:
 *   IBMI_HOST=... IBMI_USER=... IBMI_PASSWORD=... \
 *     npm run test:performance -- --testPathPatterns=native-backend-performance
 */

import { performance } from 'perf_hooks';
import { SQLJob, Pool as MapepirePool, States } from '@ibm/mapepire-js';

// idb-pconnector loaded dynamically (only available on IBM i).
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let idbModule: any;
try {
  idbModule = require('idb-pconnector');
} catch {
  idbModule = null;
}

/** True when idb-pconnector loaded successfully (i.e. running on IBM i). */
const RUN_IDB = idbModule !== null;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const MAPEPIRE_CREDS = {
  host: process.env.IBMI_HOST || 'localhost',
  user: process.env.IBMI_USER || '',
  password: process.env.IBMI_PASSWORD || '',
  rejectUnauthorized: false,
};

const QUERY_COUNT = Number(process.env.QUERY_COUNT) || 50;
const WARMUP_COUNT = 3;

/**
 * Pool sizing.
 *
 * Pool — sequential uses DBPool (idb) and a Pool (mapepire). Pool — Promise.all
 * uses different mechanisms because both native pool implementations have
 * pathological behaviour under high concurrency that doesn't reflect the
 * underlying drivers' actual throughput:
 *
 * - `DBPool.attach()` serialises and grows slowly; with 1000 concurrent
 *   `runSql()` calls per-query medians balloon to seconds.
 * - The native mapepire `Pool` has no rate-limiter on growth: under high
 *   concurrency it tries to open many fresh WebSocket+TLS connections in
 *   parallel and overwhelms the mapepire server.
 *
 * For Pool — Promise.all we therefore pre-allocate NATIVE_POOL_BURST_SIZE
 * connections up front on both sides — raw idb Connection objects (round-
 * robin dispatched) and a pre-sized native mapepire Pool with growth disabled.
 * The asymmetry with rm-connector-js (which has a growable pool with attach-
 * mutex rate-limiting) is called out in docs/PERFORMANCE-COMPARISON.md.
 */
const POOL_INCREMENT_SIZE = 5;
const NATIVE_POOL_BURST_SIZE = 50;

const SAMPLE_SCHEMA = process.env.SAMPLE_SCHEMA || 'SAMPLE';
const SQL_STANDARD = `SELECT * FROM ${SAMPLE_SCHEMA}.DEPARTMENT`;
const SQL_LARGE = `SELECT * FROM ${SAMPLE_SCHEMA}.EMPLOYEE CROSS JOIN (VALUES 1,2,3,4,5,6,7,8,9,10) AS T(N)`;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface TimingResult {
  times: number[];
  wallClock: number;
}

interface Stats {
  min: number;
  max: number;
  avg: number;
  median: number;
  total: number;
  wallClock: number;
  count: number;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function calcStats(timing: TimingResult): Stats {
  const sorted = [...timing.times].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, t) => acc + t, 0);
  const mid = Math.floor(sorted.length / 2);
  const median = sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid];

  return {
    min: sorted[0],
    max: sorted[sorted.length - 1],
    avg: sum / sorted.length,
    median,
    total: sum,
    wallClock: timing.wallClock,
    count: sorted.length,
  };
}

function formatMs(ms: number): string {
  return ms.toFixed(2) + 'ms';
}

const println = (s: string = ''): void => {
  process.stdout.write(s + '\n');
};

function printComparison(label: string, idbStats: Stats | null, mapStats: Stats): void {
  println('');
  if (idbStats) {
    println(`  ┌─────────────────────────────────────────────────────────────────┐`);
    println(`  │ ${label.padEnd(63)} │`);
    println(`  ├──────────────────┬──────────────────┬──────────────────┬────────┤`);
    println(`  │ Metric           │ idb (native)     │ mapepire (native)│ Ratio  │`);
    println(`  ├──────────────────┼──────────────────┼──────────────────┼────────┤`);

    const rows: [string, number, number][] = [
      ['Min', idbStats.min, mapStats.min],
      ['Max', idbStats.max, mapStats.max],
      ['Avg', idbStats.avg, mapStats.avg],
      ['Median', idbStats.median, mapStats.median],
      ['Total (sum)', idbStats.total, mapStats.total],
      ['Wall clock', idbStats.wallClock, mapStats.wallClock],
    ];

    for (const [metric, idb, map] of rows) {
      const r = map / idb;
      const ratioStr = r > 1 ? `${r.toFixed(1)}x` : `${(1 / r).toFixed(1)}x`;
      println(
        `  │ ${metric.padEnd(16)} │ ${formatMs(idb).padStart(16)} │ ${formatMs(map).padStart(16)} │ ${ratioStr.padStart(6)} │`,
      );
    }

    println(`  └──────────────────┴──────────────────┴──────────────────┴────────┘`);
  } else {
    println(`  ┌─────────────────────────────────────────────────────┐`);
    println(`  │ ${label.padEnd(51)} │`);
    println(`  ├──────────────────┬──────────────────────────────────┤`);
    println(`  │ Metric           │ mapepire (native)                │`);
    println(`  ├──────────────────┼──────────────────────────────────┤`);
    const rows: [string, number][] = [
      ['Min', mapStats.min],
      ['Max', mapStats.max],
      ['Avg', mapStats.avg],
      ['Median', mapStats.median],
      ['Total (sum)', mapStats.total],
      ['Wall clock', mapStats.wallClock],
    ];
    for (const [metric, map] of rows) {
      println(`  │ ${metric.padEnd(16)} │ ${formatMs(map).padStart(32)} │`);
    }
    println(`  └──────────────────┴──────────────────────────────────┘`);
  }
  println(`  Queries: ${mapStats.count}, Warm-up: ${WARMUP_COUNT}, Pool: idb DBPool incr:${POOL_INCREMENT_SIZE}, mapepire fixed:${NATIVE_POOL_BURST_SIZE}`);
}

// ---------------------------------------------------------------------------
// Native idb single-connection helpers
//
// idb-pconnector Connection is one-query-at-a-time. Each call gets a
// fresh Statement so concurrent Promise.all calls on a single Connection
// will end up serialized at the CLI level — the same as the rm-connector-js
// behaviour through this same path.
// ---------------------------------------------------------------------------

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function idbExec(conn: any, sql: string): Promise<unknown[]> {
  const stmt = conn.getStatement();
  try {
    stmt.enableNumericTypeConversion(true);
    return await stmt.exec(sql);
  } finally {
    stmt.close();
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function idbExecParam(conn: any, sql: string, params: unknown[]): Promise<unknown[]> {
  const stmt = conn.getStatement();
  try {
    stmt.enableNumericTypeConversion(true);
    await stmt.prepare(sql);
    await stmt.bindParameters(params);
    await stmt.execute();
    return await stmt.fetchAll();
  } finally {
    stmt.close();
  }
}

// ---------------------------------------------------------------------------
// Timing helpers
// ---------------------------------------------------------------------------

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function timeIdbSequential(conn: any, sql: string, count: number): Promise<TimingResult> {
  const times: number[] = [];
  for (let i = 0; i < WARMUP_COUNT; i++) await idbExec(conn, sql);

  const wallStart = performance.now();
  for (let i = 0; i < count; i++) {
    const start = performance.now();
    await idbExec(conn, sql);
    times.push(performance.now() - start);
  }
  return { times, wallClock: performance.now() - wallStart };
}

async function timeMapJobSequential(job: SQLJob, sql: string, count: number): Promise<TimingResult> {
  const times: number[] = [];
  for (let i = 0; i < WARMUP_COUNT; i++) await job.execute(sql);

  const wallStart = performance.now();
  for (let i = 0; i < count; i++) {
    const start = performance.now();
    await job.execute(sql);
    times.push(performance.now() - start);
  }
  return { times, wallClock: performance.now() - wallStart };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function timeIdbConcurrent(conn: any, sql: string, count: number): Promise<TimingResult> {
  for (let i = 0; i < WARMUP_COUNT; i++) await idbExec(conn, sql);

  const times: number[] = [];
  const wallStart = performance.now();
  await Promise.all(
    Array.from({ length: count }, async () => {
      const start = performance.now();
      await idbExec(conn, sql);
      times.push(performance.now() - start);
    }),
  );
  return { times, wallClock: performance.now() - wallStart };
}

async function timeMapJobConcurrent(job: SQLJob, sql: string, count: number): Promise<TimingResult> {
  for (let i = 0; i < WARMUP_COUNT; i++) await job.execute(sql);

  const times: number[] = [];
  const wallStart = performance.now();
  await Promise.all(
    Array.from({ length: count }, async () => {
      const start = performance.now();
      await job.execute(sql);
      times.push(performance.now() - start);
    }),
  );
  return { times, wallClock: performance.now() - wallStart };
}

// ---------------------------------------------------------------------------
// Guard: skip the entire suite only when credentials are missing.
// idb is gated per-test on RUN_IDB so the mapepire side can still run from
// non-IBM-i platforms (e.g. a workstation pointing at a remote IBM i).
// ---------------------------------------------------------------------------

const skip =
  !process.env.IBMI_HOST ||
  !process.env.IBMI_USER ||
  !process.env.IBMI_PASSWORD;

const describeIf = skip ? describe.skip : describe;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describeIf('Native Backend Performance', () => {
  // 10 minutes per test — needed for QUERY_COUNT=1000 over a remote network
  // (the large result set query alone runs ~1000 round-trips of ~420 rows each).
  jest.setTimeout(600_000);

  // -------------------------------------------------------------------
  // 1. Connection Creation
  // -------------------------------------------------------------------
  describe('Connection creation', () => {
    it('measures connection creation time for both backends', async () => {
      const iterations = 10;
      const idbTimes: number[] = [];
      const mapepireTimes: number[] = [];

      for (let i = 0; i < iterations; i++) {
        if (RUN_IDB) {
          const { Connection } = idbModule;
          // idb: new Connection({ url: '*LOCAL' }) auto-connects in the constructor
          const start = performance.now();
          const idbConn = new Connection({ url: '*LOCAL' });
          idbTimes.push(performance.now() - start);
          idbConn.disconn();
          idbConn.close();
        }

        // mapepire: new SQLJob(creds) does not auto-connect; connect() is async
        const mapJob = new SQLJob(MAPEPIRE_CREDS as never);
        const mStart = performance.now();
        if (mapJob.getStatus() === States.JobStatus.NOT_STARTED) {
          await mapJob.connect(MAPEPIRE_CREDS as never);
        }
        mapepireTimes.push(performance.now() - mStart);
        await mapJob.close();
      }

      const idbStats = RUN_IDB
        ? calcStats({ times: idbTimes, wallClock: idbTimes.reduce((a, b) => a + b, 0) })
        : null;
      const mapStats = calcStats({ times: mapepireTimes, wallClock: mapepireTimes.reduce((a, b) => a + b, 0) });

      printComparison(`Connection Creation (${iterations} iterations)`, idbStats, mapStats);

      expect(mapepireTimes.length).toBe(iterations);
      if (RUN_IDB) expect(idbTimes.length).toBe(iterations);
    });
  });

  // -------------------------------------------------------------------
  // 2. Single Connection — Sequential
  // -------------------------------------------------------------------
  describe('Single connection — sequential', () => {
    it('standard query', async () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let idbConn: any = null;
      if (RUN_IDB) {
        const { Connection } = idbModule;
        idbConn = new Connection({ url: '*LOCAL' });
      }
      const mapJob = new SQLJob(MAPEPIRE_CREDS as never);
      await mapJob.connect(MAPEPIRE_CREDS as never);

      try {
        const idbT = idbConn ? await timeIdbSequential(idbConn, SQL_STANDARD, QUERY_COUNT) : null;
        const mapT = await timeMapJobSequential(mapJob, SQL_STANDARD, QUERY_COUNT);

        printComparison(
          `Single Connection — Sequential (${SQL_STANDARD})`,
          idbT ? calcStats(idbT) : null,
          calcStats(mapT),
        );
      } finally {
        if (idbConn) {
          idbConn.disconn();
          idbConn.close();
        }
        await mapJob.close();
      }
    });

    it('large result set', async () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let idbConn: any = null;
      if (RUN_IDB) {
        const { Connection } = idbModule;
        idbConn = new Connection({ url: '*LOCAL' });
      }
      const mapJob = new SQLJob(MAPEPIRE_CREDS as never);
      await mapJob.connect(MAPEPIRE_CREDS as never);

      try {
        const idbT = idbConn ? await timeIdbSequential(idbConn, SQL_LARGE, QUERY_COUNT) : null;
        const mapT = await timeMapJobSequential(mapJob, SQL_LARGE, QUERY_COUNT);

        printComparison(
          `Single Connection — Sequential — Large Result Set`,
          idbT ? calcStats(idbT) : null,
          calcStats(mapT),
        );
      } finally {
        if (idbConn) {
          idbConn.disconn();
          idbConn.close();
        }
        await mapJob.close();
      }
    });
  });

  // -------------------------------------------------------------------
  // 3. Single Connection — Promise.all
  // -------------------------------------------------------------------
  describe('Single connection — Promise.all', () => {
    it('standard query', async () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let idbConn: any = null;
      if (RUN_IDB) {
        const { Connection } = idbModule;
        idbConn = new Connection({ url: '*LOCAL' });
      }
      const mapJob = new SQLJob(MAPEPIRE_CREDS as never);
      await mapJob.connect(MAPEPIRE_CREDS as never);

      try {
        const idbT = idbConn ? await timeIdbConcurrent(idbConn, SQL_STANDARD, QUERY_COUNT) : null;
        const mapT = await timeMapJobConcurrent(mapJob, SQL_STANDARD, QUERY_COUNT);

        printComparison(
          `Single Connection — Promise.all (${SQL_STANDARD})`,
          idbT ? calcStats(idbT) : null,
          calcStats(mapT),
        );
      } finally {
        if (idbConn) {
          idbConn.disconn();
          idbConn.close();
        }
        await mapJob.close();
      }
    });
  });

  // -------------------------------------------------------------------
  // 4. Pool — Sequential
  // -------------------------------------------------------------------
  describe('Pool — sequential', () => {
    it('standard query', async () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let idbPool: any = null;
      if (RUN_IDB) {
        const { DBPool } = idbModule;
        // DBPool's `incrementSize` is both the initial pool size and the
        // grow-on-demand increment. With POOL_INCREMENT_SIZE=5 the pool starts
        // with 5 connections and adds another 5 each time it runs out — same
        // shape as rm-backend-performance.test.ts.
        idbPool = new DBPool({ url: '*LOCAL' }, { incrementSize: POOL_INCREMENT_SIZE, debug: false });
      }

      const mapPool = new MapepirePool({
        creds: MAPEPIRE_CREDS,
        maxSize: NATIVE_POOL_BURST_SIZE,
        startingSize: NATIVE_POOL_BURST_SIZE,
      });
      await mapPool.init();

      try {
        // Warm-up
        for (let i = 0; i < WARMUP_COUNT; i++) {
          if (idbPool) await idbPool.runSql(SQL_STANDARD);
          await mapPool.execute(SQL_STANDARD);
        }

        let idbStats: Stats | null = null;
        if (idbPool) {
          const idbTimes: number[] = [];
          const idbWallStart = performance.now();
          for (let i = 0; i < QUERY_COUNT; i++) {
            const start = performance.now();
            await idbPool.runSql(SQL_STANDARD);
            idbTimes.push(performance.now() - start);
          }
          const idbWall = performance.now() - idbWallStart;
          idbStats = calcStats({ times: idbTimes, wallClock: idbWall });
        }

        const mapTimes: number[] = [];
        const mapWallStart = performance.now();
        for (let i = 0; i < QUERY_COUNT; i++) {
          const start = performance.now();
          await mapPool.execute(SQL_STANDARD);
          mapTimes.push(performance.now() - start);
        }
        const mapWall = performance.now() - mapWallStart;

        printComparison(
          `Pool (idb DBPool incr:${POOL_INCREMENT_SIZE} / mapepire fixed:${NATIVE_POOL_BURST_SIZE}) — Sequential (${SQL_STANDARD})`,
          idbStats,
          calcStats({ times: mapTimes, wallClock: mapWall }),
        );
      } finally {
        try { mapPool.end(); } catch { /* best-effort */ }
        // DBPool has no wholesale close; jest exit will tear down N-API objects
      }
    });
  });

  // -------------------------------------------------------------------
  // 5. Pool — Promise.all
  //
  // Pre-allocates NATIVE_POOL_BURST_SIZE connections on each side and dispatches
  // the QUERY_COUNT queries across them. idb uses raw Connection objects round-
  // robin; mapepire uses a Pool sized fixed at NATIVE_POOL_BURST_SIZE so growth
  // never fires during the test. Neither path goes through DBPool's attach()
  // serialisation (which inflates wall clocks dramatically under burst) or the
  // native mapepire Pool's concurrent-handshake spike. The mapepire side
  // multiplexes within each pre-created connection — the asymmetry is the same
  // as the rm-connector-js benchmark and is flagged in the table title.
  // -------------------------------------------------------------------
  describe('Pool — Promise.all', () => {
    it('standard query', async () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let idbConns: any[] = [];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let buckets: number[][] = [];
      if (RUN_IDB) {
        const { Connection } = idbModule;
        idbConns = Array.from({ length: NATIVE_POOL_BURST_SIZE }, () => new Connection({ url: '*LOCAL' }));
        buckets = Array.from({ length: NATIVE_POOL_BURST_SIZE }, () => [] as number[]);
        for (let i = 0; i < QUERY_COUNT; i++) {
          buckets[i % NATIVE_POOL_BURST_SIZE].push(i);
        }
      }

      const mapPool = new MapepirePool({
        creds: MAPEPIRE_CREDS,
        maxSize: NATIVE_POOL_BURST_SIZE,
        startingSize: NATIVE_POOL_BURST_SIZE,
      });
      await mapPool.init();

      try {
        // Warm-up
        for (let i = 0; i < WARMUP_COUNT; i++) {
          if (RUN_IDB) {
            for (const conn of idbConns) {
              await idbExec(conn, SQL_STANDARD);
            }
          }
          await mapPool.execute(SQL_STANDARD);
        }

        let idbStats: Stats | null = null;
        if (RUN_IDB) {
          const idbTimes: number[] = [];
          const idbWallStart = performance.now();
          await Promise.all(
            idbConns.map(async (conn, connIdx) => {
              const myCount = buckets[connIdx].length;
              for (let q = 0; q < myCount; q++) {
                const start = performance.now();
                await idbExec(conn, SQL_STANDARD);
                idbTimes.push(performance.now() - start);
              }
            }),
          );
          const idbWall = performance.now() - idbWallStart;
          idbStats = calcStats({ times: idbTimes, wallClock: idbWall });
        }

        const mapTimes: number[] = [];
        const mapWallStart = performance.now();
        await Promise.all(
          Array.from({ length: QUERY_COUNT }, async () => {
            const start = performance.now();
            await mapPool.execute(SQL_STANDARD);
            mapTimes.push(performance.now() - start);
          }),
        );
        const mapWall = performance.now() - mapWallStart;

        printComparison(
          `Pool (idb raw:${NATIVE_POOL_BURST_SIZE} / mapepire fixed:${NATIVE_POOL_BURST_SIZE}) — Promise.all (${SQL_STANDARD})  *mapepire side multiplexes*`,
          idbStats,
          calcStats({ times: mapTimes, wallClock: mapWall }),
        );
      } finally {
        for (const c of idbConns) {
          try { c.disconn(); c.close(); } catch { /* best-effort */ }
        }
        try { mapPool.end(); } catch { /* best-effort */ }
      }
    });
  });

  // -------------------------------------------------------------------
  // 6. Parameterized Queries — Sequential
  // -------------------------------------------------------------------
  describe('Parameterized queries — sequential', () => {
    it('measures parameterized query performance', async () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let idbConn: any = null;
      if (RUN_IDB) {
        const { Connection } = idbModule;
        idbConn = new Connection({ url: '*LOCAL' });
      }
      const mapJob = new SQLJob(MAPEPIRE_CREDS as never);
      await mapJob.connect(MAPEPIRE_CREDS as never);

      const sql = 'SELECT * FROM QIWS.QCUSTCDT WHERE STATE = ?';
      const params = ['TX'];

      try {
        for (let i = 0; i < WARMUP_COUNT; i++) {
          if (idbConn) await idbExecParam(idbConn, sql, params);
          await mapJob.execute(sql, { parameters: params });
        }

        let idbStats: Stats | null = null;
        if (idbConn) {
          const idbTimes: number[] = [];
          const idbWallStart = performance.now();
          for (let i = 0; i < QUERY_COUNT; i++) {
            const start = performance.now();
            await idbExecParam(idbConn, sql, params);
            idbTimes.push(performance.now() - start);
          }
          const idbWall = performance.now() - idbWallStart;
          idbStats = calcStats({ times: idbTimes, wallClock: idbWall });
        }

        const mapTimes: number[] = [];
        const mapWallStart = performance.now();
        for (let i = 0; i < QUERY_COUNT; i++) {
          const start = performance.now();
          await mapJob.execute(sql, { parameters: params });
          mapTimes.push(performance.now() - start);
        }
        const mapWall = performance.now() - mapWallStart;

        printComparison(
          `Parameterized Query — Sequential`,
          idbStats,
          calcStats({ times: mapTimes, wallClock: mapWall }),
        );
      } finally {
        if (idbConn) {
          idbConn.disconn();
          idbConn.close();
        }
        await mapJob.close();
      }
    });
  });
});
