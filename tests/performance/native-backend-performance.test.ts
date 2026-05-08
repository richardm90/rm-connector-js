/**
 * Native Backend Performance Tests
 *
 * Mirrors backend-performance.test.ts row-for-row, but every query goes
 * through the native driver API directly — no rm-connector-js wrapper.
 * The intent is to measure rm-connector-js's wrapper overhead by
 * running this with the same QUERY_COUNT and SAMPLE_SCHEMA as
 * backend-performance, then diffing the two output tables.
 *
 * Asymmetry warning (same as the rm-connector-js suite):
 *   The Pool Promise.all row uses native @ibm/mapepire-js Pool, which
 *   multiplexes unconditionally, vs idb-pconnector DBPool which is
 *   one-query-at-a-time per connection. The mapepire-side number
 *   therefore reflects multiplexing, not raw protocol overhead.
 *
 * Run with:
 *   IBMI_HOST=... IBMI_USER=... IBMI_PASSWORD=... \
 *     npm run test:performance -- --testPathPattern=native-backend-performance
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
const POOL_SIZE = 5;

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

function printComparison(label: string, idbStats: Stats, mapStats: Stats): void {
  println('');
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
  println(`  Queries: ${idbStats.count}, Warm-up: ${WARMUP_COUNT}, Pool size: ${POOL_SIZE}`);
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
// Guard: skip if env vars or idb-pconnector are missing
// ---------------------------------------------------------------------------

const skip =
  !process.env.IBMI_HOST ||
  !process.env.IBMI_USER ||
  !process.env.IBMI_PASSWORD ||
  !idbModule;

const describeIf = skip ? describe.skip : describe;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describeIf('Native Backend Performance', () => {
  jest.setTimeout(180_000);

  // -------------------------------------------------------------------
  // 1. Connection Creation
  // -------------------------------------------------------------------
  describe('Connection creation', () => {
    it('measures connection creation time for both backends', async () => {
      const { Connection } = idbModule;
      const iterations = 10;
      const idbTimes: number[] = [];
      const mapepireTimes: number[] = [];

      for (let i = 0; i < iterations; i++) {
        // idb: new Connection({ url: '*LOCAL' }) auto-connects in the constructor
        const start = performance.now();
        const idbConn = new Connection({ url: '*LOCAL' });
        idbTimes.push(performance.now() - start);
        idbConn.disconn();
        idbConn.close();

        // mapepire: new SQLJob(creds) does not auto-connect; connect() is async
        const mapJob = new SQLJob(MAPEPIRE_CREDS as never);
        const mStart = performance.now();
        if (mapJob.getStatus() === States.JobStatus.NOT_STARTED) {
          await mapJob.connect(MAPEPIRE_CREDS as never);
        }
        mapepireTimes.push(performance.now() - mStart);
        await mapJob.close();
      }

      printComparison(
        `Connection Creation (${iterations} iterations)`,
        calcStats({ times: idbTimes, wallClock: idbTimes.reduce((a, b) => a + b, 0) }),
        calcStats({ times: mapepireTimes, wallClock: mapepireTimes.reduce((a, b) => a + b, 0) }),
      );

      expect(idbTimes.length).toBe(iterations);
      expect(mapepireTimes.length).toBe(iterations);
    });
  });

  // -------------------------------------------------------------------
  // 2. Single Connection — Sequential
  // -------------------------------------------------------------------
  describe('Single connection — sequential', () => {
    it('standard query', async () => {
      const { Connection } = idbModule;
      const idbConn = new Connection({ url: '*LOCAL' });
      const mapJob = new SQLJob(MAPEPIRE_CREDS as never);
      await mapJob.connect(MAPEPIRE_CREDS as never);

      try {
        const idbT = await timeIdbSequential(idbConn, SQL_STANDARD, QUERY_COUNT);
        const mapT = await timeMapJobSequential(mapJob, SQL_STANDARD, QUERY_COUNT);

        printComparison(
          `Single Connection — Sequential (${SQL_STANDARD})`,
          calcStats(idbT),
          calcStats(mapT),
        );
      } finally {
        idbConn.disconn();
        idbConn.close();
        await mapJob.close();
      }
    });

    it('large result set', async () => {
      const { Connection } = idbModule;
      const idbConn = new Connection({ url: '*LOCAL' });
      const mapJob = new SQLJob(MAPEPIRE_CREDS as never);
      await mapJob.connect(MAPEPIRE_CREDS as never);

      try {
        const idbT = await timeIdbSequential(idbConn, SQL_LARGE, QUERY_COUNT);
        const mapT = await timeMapJobSequential(mapJob, SQL_LARGE, QUERY_COUNT);

        printComparison(
          `Single Connection — Sequential — Large Result Set`,
          calcStats(idbT),
          calcStats(mapT),
        );
      } finally {
        idbConn.disconn();
        idbConn.close();
        await mapJob.close();
      }
    });
  });

  // -------------------------------------------------------------------
  // 3. Single Connection — Promise.all
  // -------------------------------------------------------------------
  describe('Single connection — Promise.all', () => {
    it('standard query', async () => {
      const { Connection } = idbModule;
      const idbConn = new Connection({ url: '*LOCAL' });
      const mapJob = new SQLJob(MAPEPIRE_CREDS as never);
      await mapJob.connect(MAPEPIRE_CREDS as never);

      try {
        const idbT = await timeIdbConcurrent(idbConn, SQL_STANDARD, QUERY_COUNT);
        const mapT = await timeMapJobConcurrent(mapJob, SQL_STANDARD, QUERY_COUNT);

        printComparison(
          `Single Connection — Promise.all (${SQL_STANDARD})`,
          calcStats(idbT),
          calcStats(mapT),
        );
      } finally {
        idbConn.disconn();
        idbConn.close();
        await mapJob.close();
      }
    });
  });

  // -------------------------------------------------------------------
  // 4. Pool — Sequential
  // -------------------------------------------------------------------
  describe('Pool — sequential', () => {
    it('standard query', async () => {
      const { DBPool } = idbModule;
      const idbPool = new DBPool({ url: '*LOCAL' }, { incrementSize: POOL_SIZE, debug: false });

      // Pre-warm DBPool to POOL_SIZE connections (DBPool grows lazily)
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const preconns: any[] = [];
      for (let i = 0; i < POOL_SIZE; i++) preconns.push(await idbPool.attach());
      for (const c of preconns) idbPool.detach(c);

      const mapPool = new MapepirePool({
        creds: MAPEPIRE_CREDS,
        maxSize: POOL_SIZE,
        startingSize: POOL_SIZE,
      });
      await mapPool.init();

      try {
        // Warm-up
        for (let i = 0; i < WARMUP_COUNT; i++) {
          await idbPool.runSql(SQL_STANDARD);
          await mapPool.execute(SQL_STANDARD);
        }

        const idbTimes: number[] = [];
        const idbWallStart = performance.now();
        for (let i = 0; i < QUERY_COUNT; i++) {
          const start = performance.now();
          await idbPool.runSql(SQL_STANDARD);
          idbTimes.push(performance.now() - start);
        }
        const idbWall = performance.now() - idbWallStart;

        const mapTimes: number[] = [];
        const mapWallStart = performance.now();
        for (let i = 0; i < QUERY_COUNT; i++) {
          const start = performance.now();
          await mapPool.execute(SQL_STANDARD);
          mapTimes.push(performance.now() - start);
        }
        const mapWall = performance.now() - mapWallStart;

        printComparison(
          `Pool (${POOL_SIZE}) — Sequential (${SQL_STANDARD})`,
          calcStats({ times: idbTimes, wallClock: idbWall }),
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
  // DBPool grows on demand under concurrent attach() pressure (no maxSize
  // option), which produces wildly inflated wall clocks vs RmPool's bounded
  // maxSize: POOL_SIZE behaviour. We bypass DBPool for this scenario and
  // construct POOL_SIZE raw Connection objects directly, then dispatch the
  // QUERY_COUNT queries round-robin across them. Each connection runs its
  // share sequentially (idb is one-query-at-a-time per Connection), all
  // chains run in parallel via Promise.all. This is the cleanest native
  // baseline at fixed concurrency POOL_SIZE — comparison against RmPool's
  // pool.query() Promise.all reflects the cost of attach/detach + health
  // checks per query.
  //
  // The mapepire side keeps the native Pool's built-in multiplexing — the
  // asymmetry is the same as the rm-connector-js benchmark and is flagged
  // in the table title.
  // -------------------------------------------------------------------
  describe('Pool — Promise.all', () => {
    it('standard query', async () => {
      const { Connection } = idbModule;

      // POOL_SIZE raw connections, held throughout the burst.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const idbConns: any[] = Array.from(
        { length: POOL_SIZE },
        () => new Connection({ url: '*LOCAL' }),
      );

      const mapPool = new MapepirePool({
        creds: MAPEPIRE_CREDS,
        maxSize: POOL_SIZE,
        startingSize: POOL_SIZE,
      });
      await mapPool.init();

      // Distribute QUERY_COUNT queries round-robin across POOL_SIZE
      // connections. When QUERY_COUNT % POOL_SIZE != 0, the first few
      // connections handle one extra query each.
      const buckets: number[][] = Array.from({ length: POOL_SIZE }, () => []);
      for (let i = 0; i < QUERY_COUNT; i++) {
        buckets[i % POOL_SIZE].push(i);
      }

      try {
        // Warm-up: run on each connection so all are equally hot.
        for (let i = 0; i < WARMUP_COUNT; i++) {
          for (const conn of idbConns) {
            await idbExec(conn, SQL_STANDARD);
          }
          await mapPool.execute(SQL_STANDARD);
        }

        // idb: per-connection sequential chains, all chains in parallel.
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

        // mapepire: native Pool, fully concurrent (multiplexes across POOL_SIZE jobs).
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
          `Pool (${POOL_SIZE}) — Promise.all (${SQL_STANDARD})  *mapepire side multiplexes*`,
          calcStats({ times: idbTimes, wallClock: idbWall }),
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
      const { Connection } = idbModule;
      const idbConn = new Connection({ url: '*LOCAL' });
      const mapJob = new SQLJob(MAPEPIRE_CREDS as never);
      await mapJob.connect(MAPEPIRE_CREDS as never);

      const sql = 'SELECT * FROM QIWS.QCUSTCDT WHERE STATE = ?';
      const params = ['TX'];

      try {
        for (let i = 0; i < WARMUP_COUNT; i++) {
          await idbExecParam(idbConn, sql, params);
          await mapJob.execute(sql, { parameters: params });
        }

        const idbTimes: number[] = [];
        const idbWallStart = performance.now();
        for (let i = 0; i < QUERY_COUNT; i++) {
          const start = performance.now();
          await idbExecParam(idbConn, sql, params);
          idbTimes.push(performance.now() - start);
        }
        const idbWall = performance.now() - idbWallStart;

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
          calcStats({ times: idbTimes, wallClock: idbWall }),
          calcStats({ times: mapTimes, wallClock: mapWall }),
        );
      } finally {
        idbConn.disconn();
        idbConn.close();
        await mapJob.close();
      }
    });
  });
});
