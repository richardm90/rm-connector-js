# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Performance benchmark suite restructured around five scenarios. Renamed `tests/performance/backend-performance.test.ts` → `tests/performance/rm-backend-performance.test.ts` and added additional env vars so a single test file covers every rm-connector-js scenario. Removed three redundant benchmark files — their data is now produced from the consolidated test plus the existing native baseline. Moved `bench-runs.sh` into `tests/performance/` and rewrote phases. Output filenames now include a `loopback` / `remote` location label. See `docs/PERFORMANCE-COMPARISON.md` for the new scenario layout.

### Added

- Optional `creds` on the idb backend (new `IdbCredentials` interface: `{ user?, password?, database? }`) for profile swap on `*LOCAL` and remote RDB connections via directory entries (`WRKRDBDIRE`). Exported `isDaemonServer` type guard.
- `backend` field on `RmConnection`/`RmPool`/`RmPoolConnection` `getInfo()` and in `RmPool.debug()` output.
- Backend selection now logged at `RmPool` construction and `RmPoolConnection` init.
- `logPrefix` option on `RmConnectionOptions`; `RmPoolConnection` passes `Pool: <id> Connection: <index>`.
- `examples/backend-connections.js` and `examples/pool-diagnostics.js`.

### Changed

- Log messages: `Connected (mapepire-js)` → `Connected (mapepire)`, `Connected (idb-pconnector)` → `Connected (idb)`.
- Clearer mapepire error when `creds` is missing or has the idb shape.
- `RmConnection.backend` is overwritten with the resolved value at init (`'auto'` → `'mapepire'`/`'idb'`).
- `RmPool` no longer synthesizes placeholder empty-string creds when `PoolOptions`/`creds` are omitted.
- `BINARY`, `VARBINARY`, and `BLOB` columns now return Node `Buffer` on both backends (mapepire previously returned an uppercase hex string). Node `Buffer` is also accepted as a parameter for binary columns on both backends; the mapepire wrapper converts it to a lowercase hex binding transparently. The idb backend now unconditionally copies `Buffer` parameters before binding as a workaround for a native memory-safety bug in idb-connector ([IBM/nodejs-idb-connector#202](https://github.com/IBM/nodejs-idb-connector/issues/202); fix in progress). See `docs/BACKEND-DIFFERENCES.md` for measured size ceilings (idb round-trips up to at least 128 MiB; mapepire tops out at ~24 MiB due to a 50 MiB server-side WebSocket message cap and the hex-encoding 2× overhead).

## [1.0.4] - 2026-04-16

### Added

- Four new JDBCOptions are now mapped onto the idb backend: `date format`, `date separator`, `time format`, and `time separator`. Values follow the JT400 property spellings (e.g. `iso`, `mdy`, `/`, `:`) and are translated to `setConnAttr(SQL_ATTR_DATE_FMT / SQL_ATTR_DATE_SEP / SQL_ATTR_TIME_FMT / SQL_ATTR_TIME_SEP)` using the matching `SQL_FMT_*` / `SQL_SEP_*` constants exported by `idb-connector`.
- Both backends now inject ISO-leaning defaults for the four date/time format options when the caller doesn't supply them: `date format=iso`, `date separator=/`, `time format=iso`, `time separator=:`. Caller-supplied values always win. This is the first case where rm-connector-js injects a default on the mapepire side — callers who relied on the QZDASOINIT prestart job's `DATFMT`/`DATSEP`/`TIMFMT` (which on US-locale systems typically resolves to `*MDY` via `QDATFMT`) must now pass explicit format values to restore the previous behaviour. The motivation is backend parity: a pool with no formatting options now returns DATE and TIME columns identically on both backends (`"2024-06-15"` for DATE, `"13.45.30"` for TIME) instead of differing values. Note that DB2's `SQL_FMT_ISO` / JT400 `"iso"` time format uses `.` as the separator, not `:` — this is a DB2 CLI / JT400 quirk (`SQL_FMT_JIS` or `SQL_FMT_HMS` with an explicit `time separator` give colons). The `time separator` default of `:` is therefore applied but has no effect when `time format=iso`.

- Mapepire backend now normalises `TIMESTAMP` values from JT400 format (`"2024-06-15 13:45:30.123456"`) to DB2 native format (`"2024-06-15-13.45.30.123456"`) via per-row post-processing using column type metadata. Both backends now return identical timestamp strings.

### Known limitations

- When `date format` is `iso`, `usa`, `eur`, or `jis`, DB2 hard-codes the separator and the `date separator` option has no effect (same for `time format` / `time separator` under those values). This is a DB2 CLI / JT400 limitation — the options are still honoured on MDY/DMY/YMD/JUL formats.

## [1.0.3] - 2026-04-15

### Added

- Opt-in `multiplex: true` flag on `PoolOptions` and `RmConnectionOptions` (mapepire backend only). When enabled, each `RmPoolConnection` is shared: multiple callers can hold it simultaneously and concurrent `pool.query()` calls map directly to mapepire-js's parallel `job.execute()` calls on the same `SQLJob`. `RmPool` uses round-robin dispatch across pool members instead of exclusive attach; `detach()` becomes a no-op; per-attach health checks are skipped (use `healthCheck.keepalive` for periodic background checks instead). `connection.expiry` still applies as max age from creation — retirement is deferred if `inFlight > 0` and the pool auto-creates a replacement.
- `RmPoolConnection` now exposes an `inFlight` counter and `multiplex` flag in `getInfo()` for visibility when `available`/`busy` no longer carry their usual meaning.
- Rejects `multiplex: true` combined with the idb backend at construction/init with a clear error — idb is shared-memory IPC and cannot multiplex.
- `tests/multiplex.test.ts` — 7 unit tests covering idb rejection, round-robin behaviour, no-op detach, N-concurrent-queries-on-pool-of-1 sharing one job, and `inFlight` visibility.
- `tests/performance/remote-mapepire-multiplex.test.ts` — three-way benchmark comparing rm-connector-js serialized, rm-connector-js multiplex, and the native mapepire Pool. Prints a comparison table with "multiplex recovers Nx over serialized" summary for remote and loopback use.
- `examples/multiplex.js` — runnable example demonstrating a 50-query `Promise.all` burst through a pool of 5 connections in multiplex mode, with inline comments explaining the fixed-size-pool semantics. Includes a secondary `idbRejectionExample()` showing the init-time rejection when combining `multiplex: true` with `backend: 'idb'`.

### Changed

- `docs/PERFORMANCE-COMPARISON.md` refreshed with three-way loopback and remote benchmark data across 50/100/200/400/1000/2000-query scales. The "multiplexing hurts on loopback" claim has been revised: native mapepire's `Pool.getJob()` biases dispatch toward the first ready job, which is the real cause of its loopback underperformance — round-robin multiplexing (as in `multiplex: true`) is 2.2x-7.8x faster than serialized rm-connector-js and 2.6x-5.4x faster than native mapepire across every scale tested on loopback. Remote: 21x faster than serialized at 50 concurrent queries, 29x at 100, matching or slightly beating the native pool. The `idb-connector` vs `idb-pconnector` package name is now consistently correct throughout — 22 prior references to `idb-connector` corrected to `idb-pconnector` (the npm package rm-connector-js actually imports), and the Architecture Overview now explicitly explains that `idb-pconnector` is a Promise wrapper around the `idb-connector` N-API addon and clarifies which of the two is doing the real work.
- `README.md` updated with a new "Optional Multiplex Mode" feature bullet, a new `multiplex` entry in the Pool Options list, a `multiplex: false` line in the `RmConnection` constructor example, and a new "Pool sizing under multiplex" subsection explaining how the fixed-size pool behaves (`initialConnections.size` is the pool size, `maxSize` is only a safety cap on initial creation, `incrementConnections` is ignored, `expiry` is rarely useful, `healthCheck.onAttach` is skipped). The subsection also includes a 6-step in-flight queue diagram showing where queries live as they traverse the mapepire-js `globalQueryList`, WebSocket send buffer, wire, server receive buffer, Mapepire Java session queue, and JDBC → QZDASOINIT → Db2.
- `tests/README.md` refreshed. Previously listed 4 test files when the directory now has 14 (9 unit + 5 performance + the parity suite). Now documents the three separate test runs (`npm test` / `test:performance` / `test:parity`), explains why `tests/performance/` and `tests/parity/` are excluded from the default run, and describes the module-resolution trick (`moduleDirectories` vs `roots`) that keeps the manual mocks in `tests/__mocks__/` active for unit tests but transparent for perf/parity suites.
- `jest.perf.config.js` testMatch now includes `remote-mapepire-multiplex.test.ts`.
- Performance test print helpers write directly to `process.stdout` (via a small `println` helper) instead of `console.log`, so Jest's source-location decoration no longer interleaves with the comparison tables.
- **Terminology correction: "ODBC" → "DB2 CLI" throughout the project.** The idb backend was previously described as using ODBC in README, BACKEND-DIFFERENCES, PERFORMANCE-COMPARISON, code comments, and two historical CHANGELOG entries (1.0.2 and 1.0.0). In reality, `idb-connector` is a native N-API addon that links directly against IBM i's Db2 SQL Call Level Interface (the `QSQCLI` service program on-box), not an ODBC driver — the two share the X/Open CLI function namespace (`SQLConnect`, `SQLPrepare`, `SQLExecute`, etc.) which is the source of the historical confusion, but DB2 CLI is a native in-process API with no driver manager, while ODBC on IBM i is a separate IBM i Access driver typically used over the network from off-box clients. Liam's benchmark references (which genuinely did use the IBM i Access ODBC driver from a Mac) are preserved as "ODBC" where appropriate.

### Fixed

- `jest.config.js` now excludes `/tests/performance/` via `testPathIgnorePatterns` so `npm run test` no longer tries to run performance suites. Previously, with credentials set, Jest would pick up the perf files under the default config and hit the manual `@ibm/mapepire-js` mock (which has no `Pool.init()`), causing `TypeError: mapPool.init is not a function`. Performance suites must now go through `npm run test:performance`, which uses `jest.perf.config.js` and loads the real modules.

## [1.0.2] - 2026-04-02

### Added

- Mapepire backend now logs `Connected (mapepire-js)` (was `Connected`)
- New examples: `standalone-connection.js`, `custom-logger.js`, `error-handling.js`
- `docs/BACKEND-DIFFERENCES.md` — comprehensive comparison of idb vs mapepire backend behaviour
- Test for per-pool logger override
- IDB backend: `transaction isolation` JDBCOption mapped to `setConnAttr(SQL_ATTR_COMMIT)` — supports `none`, `read uncommitted`, `read committed`, `repeatable read`, `serializable`
- IDB backend: `auto commit` JDBCOption mapped to `setConnAttr(SQL_ATTR_AUTOCOMMIT)`
- IDB backend: `setLibraryList()` replaces `SET PATH` SQL for setting library list (native DB2 CLI call)
- IDB backend unit tests covering all `applyJDBCOptions` mappings
- `idb-pconnector` test mock — enables test suite to pass on IBM i
- Tests now explicitly set `backend: 'mapepire'` to prevent idb backend being selected on IBM i
- Backend parity test suite (53 tests) covering data types, commitment control, JDBCOptions, DML, and error handling across idb and mapepire backends
  - Multi-library parity tests with PARITYTEST schema setup/teardown
  - DML parity tests with commitment control scenarios (INSERT, UPDATE, DELETE)
  - COMMIT, ROLLBACK and error recovery parity tests for transactional guarantees
  - DB2 for i SQL data type parity tests covering SMALLINT, INTEGER, BIGINT, DECIMAL, NUMERIC, REAL, DOUBLE, CHAR, VARCHAR, CLOB, DATE, TIME, TIMESTAMP, BINARY, VARBINARY, BLOB, GRAPHIC, VARGRAPHIC, BOOLEAN, and DECFLOAT
  - Documented backend differences: BIGINT (string vs number), DOUBLE precision (~6 vs ~15 digits), DECFLOAT (string vs number), BOOLEAN (string vs native), NULL CLOB (empty string vs null), raw date/time formats, default commitment control

### Fixed

- IDB backend: import `SQL_ATTR_COMMIT`/`SQL_TXN_NO_COMMIT` constants from `idb-pconnector` instead of hardcoding wrong values
- IDB backend: handle `fetchAll` "no result set" error for CALL statements (e.g. `QCMDEXC`)
- IDB backend: use `setConnAttr` for naming option (`SET OPTION` not allowed in `idb-pconnector`)
- IDB backend: use `prepare`/`execute` path for `SET PATH` statements
- IDB backend: change string trimming from `trim()` to `trimEnd()` to preserve leading whitespace
- IDB backend: capture output parameters from stored procedures via `stmt.execute()` return value
- Per-pool logger override now correctly takes precedence over the global logger
- Tests failing on IBM i due to `resolveBackend()` selecting idb path without a mock

## [1.0.1] - 2026-03-05

### Fixed

- Fix npm registry publish issue (ghost 1.0.0 publish)
- Fix `package-lock.json` still referencing old `rm-mapepire-js` name

## [1.0.0] - 2026-03-05

### Added

- **Dual-backend support**: `mapepire` (remote, WebSocket) and `idb-pconnector` (native IBM i, DB2 CLI)
- `BackendConnection` interface (`src/backends/types.ts`) for pluggable backends
- `MapepireBackend` (`src/backends/mapepire.ts`) — wraps `@ibm/mapepire-js` SQLJob
- `IdbBackend` (`src/backends/idb.ts`) — wraps `idb-pconnector`, dynamically loaded
- `backend` option on `RmConnectionOptions`: `'auto'` | `'mapepire'` | `'idb'` (default: `'auto'`)
- Auto-detection: `'auto'` selects `idb` on IBM i (`process.platform === 'os400'`), `mapepire` elsewhere
- `idb-pconnector` added to `optionalDependencies`
- IDB backend: automatic numeric type conversion, string trimming to match mapepire behaviour
- IDB backend: JDBC options mapping for `libraries` and `naming`
- Parallel pool connection creation in both `init()` and `_attach()` increment paths

### Changed

- **Breaking:** `RmConnection` now takes a single `RmConnectionOptions` object instead of positional arguments
- **Breaking:** Package renamed from `rm-mapepire-js` to `rm-connector-js`
- Pool connections are now created in parallel (previously sequential)

### Deprecated

- `rm-mapepire-js` npm package — use `rm-connector-js` instead

## [0.4.0] - 2026-03-03

### Added

- `logLevel` option on `PoolsConfig` and `PoolOptions` — configurable log level threshold: `'error'` | `'info'` | `'debug'` | `'none'` (default: `'info'`)
- `RmLogger` centralized logger class with log level filtering and context-aware message formatting
- Per-pool `logLevel` override via `PoolOptions.logLevel`

### Changed

- Logging centralized into `RmLogger` — removes duplicated `log()` methods from all classes

### Removed

- **Breaking:** `debug` option removed from `PoolsConfig`. Use `logLevel: 'debug'` instead.
- **Breaking:** `dbConnectorDebug` option removed from `PoolOptions`. Use `logLevel: 'debug'` instead.
- **Breaking:** `debug` property removed from `RmConnection`, `RmPoolConnection`, `RmPool`, and `RmPools` classes.

## [0.3.0] - 2026-03-03

### Added

- `healthCheck.keepalive` option — configurable interval (in minutes) to send periodic keepalive pings (`VALUES 1`) on idle connections, preventing WebSocket connections from being silently dropped by firewalls or network intermediaries
- Keepalive timer automatically resets when real queries are executed, avoiding unnecessary traffic on active connections
- Keepalive timer stops gracefully on connection close or ping failure

## [0.2.0] - 2026-02-12

### Added

- `RmQueryResult<T>` type that extends `QueryResult<T>` with a `job` property containing the IBM i job name
- `execute()` and `query()` on `RmConnection`, `RmPoolConnection`, and `RmPool` now return typed `RmQueryResult<any>` instead of `Promise<any>`

## [0.1.0] - 2026-02-10

### Added

- `RmConnection` class wrapping `SQLJob` from `@ibm/mapepire-js`
- `RmPool` class with connection pooling, auto-scaling, health checks, and expiry timers
- `RmPoolConnection` class for managed pooled connections
- `RmPools` multi-pool manager
- Injectable logger support with configurable flow through class hierarchy
- `EventEmitter` events on `RmPool` for lifecycle hooks
- `initCommands` for flexible connection setup (SQL and CL commands)
- Pool health check on attach
- Pool shutdown (`close`) support
- Examples for basic queries, stored procedures, and pool usage
