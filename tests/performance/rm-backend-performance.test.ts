/**
 * rm-connector-js Backend Performance Tests
 *
 * Measures the performance of the mapepire and idb backends when accessed
 * through the rm-connector-js wrapper (RmConnection / RmPool). The native
 * baseline (drivers used directly, no rm-connector-js on the data path)
 * lives in `native-backend-performance.test.ts`.
 *
 * Env vars that select which scenario this run measures (see
 * `docs/PERFORMANCE-COMPARISON.md` Section 3f):
 *   - IBMI_HOST / IBMI_USER / IBMI_PASSWORD — connection details (required).
 *   - QUERY_COUNT (default 50) — queries per scenario.
 *   - SAMPLE_SCHEMA (default SAMPLE) — schema name.
 *   - RM_ON_ATTACH (default true) — health check on each attach() call.
 *   - RM_MULTIPLEX (default false) — opt in to mapepire multiplex mode;
 *     when true the idb branch of every test is skipped (idb cannot multiplex).
 *   - RM_KEEPALIVE (default unset/disabled) — keepalive interval in minutes;
 *     fractional values allowed; only meaningful for mapepire.
 *   Pool sizing is fixed across scenarios: maxSize=1000, initial=5, increment=5.
 *
 * Run via `bench-runs.sh` for the standard 3-runs × 3-query-counts harness,
 * or directly with `npm run test:performance -- --testPathPatterns=rm-backend-performance`.
 */

import { performance } from 'perf_hooks';
import RmConnection from '../../src/rmConnection';
import RmPool from '../../src/rmPool';
import { RmConnectionOptions, PoolOptions } from '../../src/types';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const MAPEPIRE_CREDS = {
  host: process.env.IBMI_HOST || 'localhost',
  user: process.env.IBMI_USER || '',
  password: process.env.IBMI_PASSWORD || '',
  rejectUnauthorized: false,
};

/** Number of queries per scenario (configurable via QUERY_COUNT env var) */
const QUERY_COUNT = Number(process.env.QUERY_COUNT) || 50;

/** Number of warm-up queries before measurement */
const WARMUP_COUNT = 3;

/**
 * Pool sizing — same across every rm scenario. The pool starts at 5
 * connections and grows in increments of 5 up to a 1000-connection ceiling
 * if demand forces it. In scenarios with the per-attach health check on
 * (1, 3, 4) the rate-limiter behaviour means the pool effectively stays
 * near 5; in Scenario 2 (onAttach=false) the pool grows under burst load
 * because attach is no longer rate-limited. Sharing the same config across
 * scenarios keeps the comparison apples-to-apples.
 */
const POOL_MAX_SIZE = 1000;
const POOL_INITIAL_SIZE = 5;
const POOL_INCREMENT_SIZE = 5;

/** Per-attach health check (configurable via RM_ON_ATTACH env var; default true) */
const RM_ON_ATTACH = process.env.RM_ON_ATTACH !== 'false';

/** Multiplex mode (mapepire only, idb is skipped when true) */
const RM_MULTIPLEX = process.env.RM_MULTIPLEX === 'true';

/** Keepalive interval in minutes; null = disabled */
const RM_KEEPALIVE: number | null =
  process.env.RM_KEEPALIVE !== undefined && process.env.RM_KEEPALIVE !== ''
    ? Number(process.env.RM_KEEPALIVE)
    : null;

/**
 * True only on IBM i, where idb-pconnector (a native addon) loads. On any
 * other platform require() throws and the idb branch of every test is skipped
 * so the mapepire side can still produce remote-from-workstation data.
 */
let idbAvailable = false;
try {
  require('idb-pconnector');
  idbAvailable = true;
} catch {
  // idb-pconnector is not installable off IBM i; idb tests will be skipped.
}

/** When true, the idb branch of every test runs alongside mapepire */
const RUN_IDB = !RM_MULTIPLEX && idbAvailable;

/** SAMPLE schema name (configurable via SAMPLE_SCHEMA env var) */
const SAMPLE_SCHEMA = process.env.SAMPLE_SCHEMA || 'SAMPLE';

/** SQL statement for standard benchmarks (same as Liam's) */
const SQL_STANDARD = `SELECT * FROM ${SAMPLE_SCHEMA}.DEPARTMENT`;

/** SQL statement for large result set benchmarks */
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

function baseOpts(): { idb: RmConnectionOptions | null; mapepire: RmConnectionOptions } {
  const mapepire: RmConnectionOptions = {
    backend: 'mapepire',
    creds: MAPEPIRE_CREDS,
    logLevel: 'none',
    multiplex: RM_MULTIPLEX,
  };
  if (RM_KEEPALIVE !== null) {
    mapepire.keepalive = RM_KEEPALIVE;
  }
  return {
    idb: RUN_IDB ? { backend: 'idb', logLevel: 'none' } : null,
    mapepire,
  };
}

function poolOpts(): { idb: PoolOptions | null; mapepire: PoolOptions } {
  const mapepire: PoolOptions = {
    backend: 'mapepire',
    creds: MAPEPIRE_CREDS,
    logLevel: 'none',
    maxSize: POOL_MAX_SIZE,
    initialConnections: { size: POOL_INITIAL_SIZE },
    incrementConnections: { size: POOL_INCREMENT_SIZE },
    multiplex: RM_MULTIPLEX,
    healthCheck: { onAttach: RM_ON_ATTACH, keepalive: RM_KEEPALIVE },
  };
  return {
    idb: RUN_IDB
      ? {
          backend: 'idb',
          logLevel: 'none',
          maxSize: POOL_MAX_SIZE,
          initialConnections: { size: POOL_INITIAL_SIZE },
          incrementConnections: { size: POOL_INCREMENT_SIZE },
          healthCheck: { onAttach: RM_ON_ATTACH, keepalive: RM_KEEPALIVE },
        }
      : null,
    mapepire,
  };
}

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

// Write directly to stdout to bypass Jest's console.log decoration
// (which prefixes each line with "console.log" and a source location).
const println = (s: string = ''): void => {
  process.stdout.write(s + '\n');
};

function printResults(label: string, idbStats: Stats | null, mapepireStats: Stats): void {
  println('');
  if (idbStats) {
    println(`  ┌─────────────────────────────────────────────────────────────────┐`);
    println(`  │ ${label.padEnd(63)} │`);
    println(`  ├──────────────────┬──────────────────┬──────────────────┬────────┤`);
    println(`  │ Metric           │ idb              │ mapepire         │ Ratio  │`);
    println(`  ├──────────────────┼──────────────────┼──────────────────┼────────┤`);
    const rows: [string, number, number][] = [
      ['Min', idbStats.min, mapepireStats.min],
      ['Max', idbStats.max, mapepireStats.max],
      ['Avg', idbStats.avg, mapepireStats.avg],
      ['Median', idbStats.median, mapepireStats.median],
      ['Total (sum)', idbStats.total, mapepireStats.total],
      ['Wall clock', idbStats.wallClock, mapepireStats.wallClock],
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
    println(`  │ Metric           │ mapepire                         │`);
    println(`  ├──────────────────┼──────────────────────────────────┤`);
    const rows: [string, number][] = [
      ['Min', mapepireStats.min],
      ['Max', mapepireStats.max],
      ['Avg', mapepireStats.avg],
      ['Median', mapepireStats.median],
      ['Total (sum)', mapepireStats.total],
      ['Wall clock', mapepireStats.wallClock],
    ];
    for (const [metric, map] of rows) {
      println(`  │ ${metric.padEnd(16)} │ ${formatMs(map).padStart(32)} │`);
    }
    println(`  └──────────────────┴──────────────────────────────────┘`);
  }
  println(
    `  Queries: ${mapepireStats.count}, Warm-up: ${WARMUP_COUNT}, ` +
      `pool=init:${POOL_INITIAL_SIZE} max:${POOL_MAX_SIZE} incr:${POOL_INCREMENT_SIZE}, ` +
      `multiplex=${RM_MULTIPLEX}, ` +
      `onAttach=${RM_ON_ATTACH}, keepalive=${RM_KEEPALIVE === null ? 'off' : `${RM_KEEPALIVE}min`}`,
  );
}

// ---------------------------------------------------------------------------
// Timing functions
// ---------------------------------------------------------------------------

/** Time N sequential queries on a single connection */
async function timeSequential(conn: RmConnection, sql: string, count: number): Promise<TimingResult> {
  const times: number[] = [];

  // Warm-up
  for (let i = 0; i < WARMUP_COUNT; i++) {
    await conn.execute(sql);
  }

  const wallStart = performance.now();
  for (let i = 0; i < count; i++) {
    const start = performance.now();
    await conn.execute(sql);
    times.push(performance.now() - start);
  }
  const wallClock = performance.now() - wallStart;

  return { times, wallClock };
}

/** Time N concurrent queries on a single connection via Promise.all */
async function timeConcurrentSingle(conn: RmConnection, sql: string, count: number): Promise<TimingResult> {
  // Warm-up
  for (let i = 0; i < WARMUP_COUNT; i++) {
    await conn.execute(sql);
  }

  const times: number[] = [];
  const wallStart = performance.now();

  await Promise.all(
    Array.from({ length: count }, async () => {
      const start = performance.now();
      await conn.execute(sql);
      times.push(performance.now() - start);
    }),
  );

  const wallClock = performance.now() - wallStart;
  return { times, wallClock };
}

/** Time N sequential queries using pool.query() (handles attach/detach internally) */
async function timePoolSequential(pool: RmPool, sql: string, count: number): Promise<TimingResult> {
  const times: number[] = [];

  // Warm-up
  for (let i = 0; i < WARMUP_COUNT; i++) {
    await pool.query(sql);
  }

  const wallStart = performance.now();
  for (let i = 0; i < count; i++) {
    const start = performance.now();
    await pool.query(sql);
    times.push(performance.now() - start);
  }
  const wallClock = performance.now() - wallStart;

  return { times, wallClock };
}

/**
 * Time N concurrent queries using pool.query() via Promise.allSettled.
 *
 * Uses allSettled rather than all because Scenario 2 (onAttach=false) can
 * exhaust the pool under high concurrency — the implicit rate-limiter is
 * gone, so attach() throws "Maximum number of connections reached" once
 * concurrent demand exceeds maxSize. We need to capture how many queries
 * succeeded vs failed rather than abort on the first failure.
 *
 * Returns timing for the SUCCESSFUL queries plus a `failures` count.
 * Wall clock spans the full batch (success or fail) end-to-end.
 */
async function timePoolConcurrent(
  pool: RmPool,
  sql: string,
  count: number,
): Promise<TimingResult & { failures: number }> {
  // Warm-up
  for (let i = 0; i < WARMUP_COUNT; i++) {
    await pool.query(sql);
  }

  const times: number[] = [];
  const wallStart = performance.now();

  const results = await Promise.allSettled(
    Array.from({ length: count }, async () => {
      const start = performance.now();
      await pool.query(sql);
      times.push(performance.now() - start);
    }),
  );

  let failures = 0;
  for (const r of results) {
    if (r.status === 'rejected') failures += 1;
  }

  const wallClock = performance.now() - wallStart;
  return { times, wallClock, failures };
}

// ---------------------------------------------------------------------------
// Guard: skip entire suite if credentials are missing
// ---------------------------------------------------------------------------

const skip = !process.env.IBMI_HOST || !process.env.IBMI_USER || !process.env.IBMI_PASSWORD;

const describeIf = skip ? describe.skip : describe;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describeIf('rm-connector-js Backend Performance', () => {
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
      const opts = baseOpts();

      for (let i = 0; i < iterations; i++) {
        if (opts.idb) {
          const idbConn = new RmConnection(opts.idb);
          const start = performance.now();
          await idbConn.init(true);
          idbTimes.push(performance.now() - start);
          await idbConn.close();
        }

        const mapConn = new RmConnection(opts.mapepire);
        const mStart = performance.now();
        await mapConn.init(true);
        mapepireTimes.push(performance.now() - mStart);
        await mapConn.close();
      }

      const idbStats = opts.idb
        ? calcStats({ times: idbTimes, wallClock: idbTimes.reduce((a, b) => a + b, 0) })
        : null;
      const mapStats = calcStats({ times: mapepireTimes, wallClock: mapepireTimes.reduce((a, b) => a + b, 0) });

      printResults(`Connection Creation (${iterations} iterations)`, idbStats, mapStats);

      // Sanity check: mapepire should always have connected; idb only when enabled
      expect(mapepireTimes.length).toBe(iterations);
      if (opts.idb) expect(idbTimes.length).toBe(iterations);
    });
  });

  // -------------------------------------------------------------------
  // 2. Single Connection — Sequential (baseline latency)
  // -------------------------------------------------------------------
  describe('Single connection — sequential', () => {
    it('standard query', async () => {
      const opts = baseOpts();
      const idbConn = opts.idb ? new RmConnection(opts.idb) : null;
      const mapConn = new RmConnection(opts.mapepire);
      await Promise.all([idbConn ? idbConn.init(true) : Promise.resolve(), mapConn.init(true)]);

      try {
        const idbTiming = idbConn ? await timeSequential(idbConn, SQL_STANDARD, QUERY_COUNT) : null;
        const mapTiming = await timeSequential(mapConn, SQL_STANDARD, QUERY_COUNT);

        printResults(
          `Single Connection — Sequential (${SQL_STANDARD})`,
          idbTiming ? calcStats(idbTiming) : null,
          calcStats(mapTiming),
        );
      } finally {
        await Promise.all([idbConn ? idbConn.close() : Promise.resolve(), mapConn.close()]);
      }
    });

    it('large result set', async () => {
      const opts = baseOpts();
      const idbConn = opts.idb ? new RmConnection(opts.idb) : null;
      const mapConn = new RmConnection(opts.mapepire);
      await Promise.all([idbConn ? idbConn.init(true) : Promise.resolve(), mapConn.init(true)]);

      try {
        const idbTiming = idbConn ? await timeSequential(idbConn, SQL_LARGE, QUERY_COUNT) : null;
        const mapTiming = await timeSequential(mapConn, SQL_LARGE, QUERY_COUNT);

        printResults(
          `Single Connection — Sequential — Large Result Set`,
          idbTiming ? calcStats(idbTiming) : null,
          calcStats(mapTiming),
        );
      } finally {
        await Promise.all([idbConn ? idbConn.close() : Promise.resolve(), mapConn.close()]);
      }
    });
  });

  // -------------------------------------------------------------------
  // 3. Single Connection — Promise.all (concurrent on one connection)
  // -------------------------------------------------------------------
  describe('Single connection — Promise.all', () => {
    it('standard query', async () => {
      const opts = baseOpts();
      const idbConn = opts.idb ? new RmConnection(opts.idb) : null;
      const mapConn = new RmConnection(opts.mapepire);
      await Promise.all([idbConn ? idbConn.init(true) : Promise.resolve(), mapConn.init(true)]);

      try {
        const idbTiming = idbConn ? await timeConcurrentSingle(idbConn, SQL_STANDARD, QUERY_COUNT) : null;
        const mapTiming = await timeConcurrentSingle(mapConn, SQL_STANDARD, QUERY_COUNT);

        printResults(
          `Single Connection — Promise.all (${SQL_STANDARD})`,
          idbTiming ? calcStats(idbTiming) : null,
          calcStats(mapTiming),
        );
      } finally {
        await Promise.all([idbConn ? idbConn.close() : Promise.resolve(), mapConn.close()]);
      }
    });
  });

  // -------------------------------------------------------------------
  // 4. Pool — Sequential
  // -------------------------------------------------------------------
  describe('Pool — sequential', () => {
    it('standard query', async () => {
      const opts = poolOpts();
      const idbPool = opts.idb
        ? new RmPool({ id: 'idb-perf', config: { id: 'idb-perf', PoolOptions: opts.idb } }, 'none')
        : null;
      const mapPool = new RmPool({ id: 'map-perf', config: { id: 'map-perf', PoolOptions: opts.mapepire } }, 'none');
      await Promise.all([idbPool ? idbPool.init() : Promise.resolve(), mapPool.init()]);

      try {
        const idbTiming = idbPool ? await timePoolSequential(idbPool, SQL_STANDARD, QUERY_COUNT) : null;
        const mapTiming = await timePoolSequential(mapPool, SQL_STANDARD, QUERY_COUNT);

        printResults(
          `Pool (init:${POOL_INITIAL_SIZE} max:${POOL_MAX_SIZE}) — Sequential (${SQL_STANDARD})`,
          idbTiming ? calcStats(idbTiming) : null,
          calcStats(mapTiming),
        );
      } finally {
        await Promise.all([idbPool ? idbPool.close() : Promise.resolve(), mapPool.close()]);
      }
    });
  });

  // -------------------------------------------------------------------
  // 5. Pool — Promise.all (concurrent burst)
  // -------------------------------------------------------------------
  describe('Pool — Promise.all', () => {
    it('standard query', async () => {
      const opts = poolOpts();
      const idbPool = opts.idb
        ? new RmPool({ id: 'idb-perf', config: { id: 'idb-perf', PoolOptions: opts.idb } }, 'none')
        : null;
      const mapPool = new RmPool({ id: 'map-perf', config: { id: 'map-perf', PoolOptions: opts.mapepire } }, 'none');
      await Promise.all([idbPool ? idbPool.init() : Promise.resolve(), mapPool.init()]);

      try {
        const idbTiming = idbPool ? await timePoolConcurrent(idbPool, SQL_STANDARD, QUERY_COUNT) : null;
        const mapTiming = await timePoolConcurrent(mapPool, SQL_STANDARD, QUERY_COUNT);

        printResults(
          `Pool (init:${POOL_INITIAL_SIZE} max:${POOL_MAX_SIZE}) — Promise.all (${SQL_STANDARD})`,
          idbTiming ? calcStats(idbTiming) : null,
          calcStats(mapTiming),
        );
        if (idbTiming && idbTiming.failures > 0) {
          println(
            `  ⚠️  idb: ${idbTiming.failures}/${QUERY_COUNT} queries failed (pool exhaustion)`,
          );
        }
        if (mapTiming.failures > 0) {
          println(
            `  ⚠️  mapepire: ${mapTiming.failures}/${QUERY_COUNT} queries failed (pool exhaustion)`,
          );
        }
      } finally {
        await Promise.all([idbPool ? idbPool.close() : Promise.resolve(), mapPool.close()]);
      }
    });
  });

  // -------------------------------------------------------------------
  // 6. Parameterized Queries
  // -------------------------------------------------------------------
  describe('Parameterized queries — sequential', () => {
    it('measures parameterized query performance', async () => {
      const opts = baseOpts();
      const idbConn = opts.idb ? new RmConnection(opts.idb) : null;
      const mapConn = new RmConnection(opts.mapepire);
      await Promise.all([idbConn ? idbConn.init(true) : Promise.resolve(), mapConn.init(true)]);

      const sql = 'SELECT * FROM QIWS.QCUSTCDT WHERE STATE = ?';
      const queryOpts = { parameters: ['TX'] };

      try {
        const idbTimes: number[] = [];
        const mapTimes: number[] = [];

        // Warm-up
        for (let i = 0; i < WARMUP_COUNT; i++) {
          if (idbConn) await idbConn.execute(sql, queryOpts);
          await mapConn.execute(sql, queryOpts);
        }

        let idbWallClock = 0;
        if (idbConn) {
          const idbWallStart = performance.now();
          for (let i = 0; i < QUERY_COUNT; i++) {
            const start = performance.now();
            await idbConn.execute(sql, queryOpts);
            idbTimes.push(performance.now() - start);
          }
          idbWallClock = performance.now() - idbWallStart;
        }

        const mapWallStart = performance.now();
        for (let i = 0; i < QUERY_COUNT; i++) {
          const start = performance.now();
          await mapConn.execute(sql, queryOpts);
          mapTimes.push(performance.now() - start);
        }
        const mapWallClock = performance.now() - mapWallStart;

        printResults(
          `Parameterized Query — Sequential`,
          idbConn ? calcStats({ times: idbTimes, wallClock: idbWallClock }) : null,
          calcStats({ times: mapTimes, wallClock: mapWallClock }),
        );
      } finally {
        await Promise.all([idbConn ? idbConn.close() : Promise.resolve(), mapConn.close()]);
      }
    });
  });
});
