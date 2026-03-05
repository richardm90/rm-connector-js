# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.2] - 2026-03-05

### Fixed

- IDB backend: import `SQL_ATTR_COMMIT`/`SQL_TXN_NO_COMMIT` constants from `idb-pconnector` instead of hardcoding wrong values
- IDB backend: handle `fetchAll` "no result set" error for CALL statements (e.g. `QCMDEXC`)
- IDB backend: use `setConnAttr` for naming option (`SET OPTION` not allowed in `idb-pconnector`)
- IDB backend: use `prepare`/`execute` path for `SET PATH` statements
- IDB backend: change string trimming from `trim()` to `trimEnd()` to preserve leading whitespace
- IDB backend: capture output parameters from stored procedures via `stmt.execute()` return value
- Per-pool logger override now correctly takes precedence over the global logger

### Added

- Mapepire backend now logs `Connected (mapepire-js)` (was `Connected`)
- New examples: `standalone-connection.js`, `custom-logger.js`, `error-handling.js`
- `BACKEND-DIFFERENCES.md` — comprehensive comparison of idb vs mapepire backend behaviour
- Test for per-pool logger override

## [1.0.1] - 2026-03-05

### Fixed

- Fix npm registry publish issue (ghost 1.0.0 publish)
- Fix `package-lock.json` still referencing old `rm-mapepire-js` name

## [1.0.0] - 2026-03-05

### Added

- **Dual-backend support**: `mapepire` (remote, WebSocket) and `idb-pconnector` (native IBM i, ODBC)
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
