# Tests

This directory contains the test suite for rm-connector-js.

## Running Tests

```bash
# Unit tests (hermetic, mocked — no IBM i needed)
npm test

# Unit tests in watch mode
npm run test:watch

# Unit tests with coverage
npm run test:coverage

# Performance benchmarks (requires a real IBM i; see below)
IBMI_HOST=myibmi.com IBMI_USER=MYUSER IBMI_PASSWORD=MYPASS npm run test:performance

# Backend parity suite (requires a real IBM i)
IBMI_HOST=myibmi.com IBMI_USER=MYUSER IBMI_PASSWORD=MYPASS npm run test:parity
```

The default `npm test` only runs the hermetic unit suites in this directory. `tests/performance/` and `tests/parity/` are excluded from the default run via `testPathIgnorePatterns` in `jest.config.js` — they use separate Jest configs (`jest.perf.config.js` and `jest.parity.config.js`) that deliberately bypass the manual mocks in `tests/__mocks__/` so they can talk to real `@ibm/mapepire-js` and `idb-pconnector`.

## Test Structure

### Unit tests (this directory)

Hermetic, fully mocked — run via `npm test` on any platform.

- `setup.ts` — Jest setup and global test configuration
- `__mocks__/` — Manual mocks for `@ibm/mapepire-js` and `idb-pconnector`
- `rmConnection.test.ts` — Unit tests for the standalone `RmConnection` class
- `rmPoolConnection.test.ts` — Unit tests for pool-member connection lifecycle
- `rmPool.test.ts` — Unit tests for the pool (attach, detach, retire, expiry, health check)
- `rmPools.test.ts` — Unit tests for multi-pool management
- `multiplex.test.ts` — Unit tests for opt-in `multiplex: true` behaviour (round-robin dispatch, no-op detach, N-concurrent-queries-on-pool-of-1, idb rejection)
- `idbBackend.test.ts` — Unit tests for the idb backend's `applyJDBCOptions` mapping
- `integration.test.ts` — End-to-end workflow tests through the mocked stack
- `logLevel.test.ts` — Unit tests for the `RmLogger` log-level filter
- `debug.test.ts` — Unit tests for debug-level logging behaviour

### Performance benchmarks (`tests/performance/`)

Require a real IBM i. Run via `npm run test:performance`.

- `rm-backend-performance.test.ts` — idb vs mapepire benchmark matrix through the rm-connector-js wrapper (connection creation, sequential/Promise.all, pool, parameterized queries, large result sets). Honours `RM_ON_ATTACH`, `RM_MULTIPLEX`, `RM_KEEPALIVE` env vars to switch between scenarios — see `docs/PERFORMANCE-COMPARISON.md` Section 3f.
- `native-backend-performance.test.ts` — same matrix run against the native drivers directly (no rm-connector-js on the data path), for the wrapper-overhead baseline.
- `pool-contention-proof.test.ts` — proves the health-check gate rate-limits concurrent `attach()` calls.
- `bench-runs.sh` — orchestrates 3 runs × 3 query counts (50/200/1000) per phase across the five scenarios, output to `$HOME/bench-results/`.

### Backend parity (`tests/parity/`)

Require a real IBM i. Run via `npm run test:parity`. These run the same operations through both backends and compare the results to ensure semantic equivalence.

## Mocking Strategy

The unit suite mocks `@ibm/mapepire-js` (`tests/__mocks__/@ibm/mapepire-js.ts`) and `idb-pconnector` (`tests/__mocks__/idb-pconnector.ts`) so tests can exercise pool and connection logic without a real database. Because `jest.config.js` sets `moduleDirectories: ['node_modules', '<rootDir>/tests/__mocks__']`, these mocks are resolved automatically for every test in this directory.

For performance and parity tests where that would be counterproductive, the dedicated Jest configs set `roots: ['<rootDir>/tests/performance']` (or `tests/parity`) so the mock directory is not on the module resolution path, and the real packages load instead.