/**
 * Remote Mapepire Multiplex Comparison
 *
 * Three-way comparison from a REMOTE machine (real network latency) of:
 *   1. rm-connector-js RmPool (serialized, default behavior)
 *   2. rm-connector-js RmPool with multiplex: true
 *   3. native @ibm/mapepire-js Pool (multiplexed)
 *
 * Purpose: confirm that enabling multiplex on rm-connector-js recovers most
 * of the throughput gap against the native pool over a real network. The
 * docs already establish that native multiplexing is 16-26x faster than
 * serialized rm-connector-js remotely; this test validates that the new
 * opt-in mode closes that gap without users having to drop down to the
 * native API.
 *
 * Run with: IBMI_HOST=myibmi.com IBMI_USER=MYUSER IBMI_PASSWORD=MYPASS \
 *   npx jest --config jest.perf.config.js remote-mapepire-multiplex
 */

import { performance } from 'perf_hooks';
import { Pool as MapepirePool } from '@ibm/mapepire-js';
import RmPool from '../../src/rmPool';
import { PoolOptions } from '../../src/types';

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

function calcStats(timing: TimingResult): Stats {
  const sorted = [...timing.times].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, t) => acc + t, 0);
  const mid = Math.floor(sorted.length / 2);
  const median = sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
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

function fmt(ms: number): string {
  return ms.toFixed(2) + 'ms';
}

// Bypass Jest's console.log decoration by writing directly to stdout.
const println = (s: string = ''): void => {
  process.stdout.write(s + '\n');
};

function printThreeWay(label: string, serialized: Stats, multiplex: Stats, native: Stats): void {
  println('');
  println(`  ┌───────────────────────────────────────────────────────────────────────────────┐`);
  println(`  │ ${label.padEnd(77)} │`);
  println(`  ├──────────────────┬──────────────────┬──────────────────┬──────────────────────┤`);
  println(`  │ Metric           │ rm serialized    │ rm multiplex     │ native mapepire      │`);
  println(`  ├──────────────────┼──────────────────┼──────────────────┼──────────────────────┤`);
  const rows: [string, number, number, number][] = [
    ['Min', serialized.min, multiplex.min, native.min],
    ['Max', serialized.max, multiplex.max, native.max],
    ['Avg', serialized.avg, multiplex.avg, native.avg],
    ['Median', serialized.median, multiplex.median, native.median],
    ['Wall clock', serialized.wallClock, multiplex.wallClock, native.wallClock],
  ];
  for (const [m, s, mu, n] of rows) {
    println(
      `  │ ${m.padEnd(16)} │ ${fmt(s).padStart(16)} │ ${fmt(mu).padStart(16)} │ ${fmt(n).padStart(20)} │`,
    );
  }
  println(`  └──────────────────┴──────────────────┴──────────────────┴──────────────────────┘`);
  const muVsNative = multiplex.wallClock / native.wallClock;
  const muVsSerial = serialized.wallClock / multiplex.wallClock;
  println(`  Multiplex recovers ${muVsSerial.toFixed(1)}x over serialized; ${muVsNative.toFixed(2)}x of native wall clock`);
  println(`  Queries: ${serialized.count}, Warm-up: ${WARMUP_COUNT}, Pool size: ${POOL_SIZE}`);
}

const skip = !process.env.IBMI_HOST || !process.env.IBMI_USER || !process.env.IBMI_PASSWORD;
const describeIf = skip ? describe.skip : describe;

describeIf('Remote Mapepire: rm-connector-js multiplex vs serialized vs native', () => {
  jest.setTimeout(600_000);

  async function buildPools() {
    const baseOpts = (multiplex: boolean): PoolOptions => ({
      backend: 'mapepire',
      creds: MAPEPIRE_CREDS,
      logLevel: 'none',
      maxSize: POOL_SIZE,
      initialConnections: { size: POOL_SIZE },
      multiplex,
    });

    const serialized = new RmPool(
      { id: 'rm-serial', config: { id: 'rm-serial', PoolOptions: baseOpts(false) } },
      'none',
    );
    const multiplex = new RmPool(
      { id: 'rm-mux', config: { id: 'rm-mux', PoolOptions: baseOpts(true) } },
      'none',
    );
    const native = new MapepirePool({
      creds: MAPEPIRE_CREDS,
      maxSize: POOL_SIZE,
      startingSize: POOL_SIZE,
    });

    await Promise.all([serialized.init(), multiplex.init(), native.init()]);
    return { serialized, multiplex, native };
  }

  async function runConcurrent(count: number, fn: () => Promise<unknown>): Promise<TimingResult> {
    const times: number[] = [];
    const wallStart = performance.now();
    await Promise.all(
      Array.from({ length: count }, async () => {
        const start = performance.now();
        await fn();
        times.push(performance.now() - start);
      }),
    );
    return { times, wallClock: performance.now() - wallStart };
  }

  it('Promise.all (concurrent burst)', async () => {
    const { serialized, multiplex, native } = await buildPools();
    try {
      for (let i = 0; i < WARMUP_COUNT; i++) {
        await serialized.query(SQL_STANDARD);
        await multiplex.query(SQL_STANDARD);
        await native.execute(SQL_STANDARD);
      }

      const serializedTiming = await runConcurrent(QUERY_COUNT, () => serialized.query(SQL_STANDARD));
      const multiplexTiming = await runConcurrent(QUERY_COUNT, () => multiplex.query(SQL_STANDARD));
      const nativeTiming = await runConcurrent(QUERY_COUNT, () => native.execute(SQL_STANDARD));

      printThreeWay(
        'Promise.all (concurrent burst)',
        calcStats(serializedTiming),
        calcStats(multiplexTiming),
        calcStats(nativeTiming),
      );
    } finally {
      await serialized.close();
      await multiplex.close();
      native.end();
    }
  });

  it('High concurrency burst', async () => {
    const highCount = QUERY_COUNT * 2;
    const { serialized, multiplex, native } = await buildPools();
    try {
      for (let i = 0; i < WARMUP_COUNT; i++) {
        await serialized.query(SQL_STANDARD);
        await multiplex.query(SQL_STANDARD);
        await native.execute(SQL_STANDARD);
      }

      const serializedTiming = await runConcurrent(highCount, () => serialized.query(SQL_STANDARD));
      const multiplexTiming = await runConcurrent(highCount, () => multiplex.query(SQL_STANDARD));
      const nativeTiming = await runConcurrent(highCount, () => native.execute(SQL_STANDARD));

      printThreeWay(
        `High concurrency burst (${highCount} queries)`,
        calcStats(serializedTiming),
        calcStats(multiplexTiming),
        calcStats(nativeTiming),
      );
    } finally {
      await serialized.close();
      await multiplex.close();
      native.end();
    }
  });
});
