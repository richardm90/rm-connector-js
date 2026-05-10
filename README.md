# rm-connector-js

An IBM i DB2 connector for Node.js, supporting both remote (mapepire/WebSocket) and native (idb-pconnector/DB2 CLI) backends with connection pooling and management.

## Why rm-connector-js?

**rm-connector-js** provides a production-ready DB2 connection layer for IBM i with two interchangeable backends:

- **mapepire** — Remote connections via WebSocket using `@ibm/mapepire-js`. Works from any platform.
- **idb-pconnector** — Native DB2 CLI (Call Level Interface) connections via `idb-pconnector`, a Promise wrapper around the `idb-connector` N-API addon that links directly against Db2 for i on-box. Runs directly on IBM i with no WebSocket overhead and no driver-manager layer.

Both backends share the same API, so your application code works unchanged regardless of where it runs.

On top of the backend abstraction, the library delivers:

- **Enterprise Connection Pooling** — Auto-scaling pools with configurable min/max sizing, on-demand growth, and parallel batch connection creation. Tiered connection management with separate policies for initial vs. overflow connections.

- **Automatic Connection Expiry** — Idle overflow connections are automatically retired after a configurable timeout, freeing IBM i jobs during low-activity periods. Initial connections can be set to persist indefinitely.

- **Health Checks with Transparent Retry** — Connections are validated before being handed out (`VALUES 1` probe). Unhealthy connections are silently retired and replaced, so your application code never sees a stale connection.

- **Thread-Safe Attach** — A promise-chain mutex serializes connection checkout, preventing race conditions when multiple callers request connections simultaneously.

- **Optional Multiplex Mode** — For mapepire workloads with concurrent queries, opt into `multiplex: true` and each pool connection will serve unlimited in-flight queries over mapepire's native ID-correlated WebSocket protocol, round-robin dispatched across pool members. Measured: up to ~200× faster than the serialized default for concurrent bursts over a network, and 3×–12× faster than the native mapepire Pool on loopback. See [docs/PERFORMANCE-COMPARISON.md](docs/PERFORMANCE-COMPARISON.md#3b-multiplexing) for when to enable it.

- **Multi-Pool Management** — Run up to 8 isolated pools (e.g., production, reporting, batch) under a single `RmPools` manager with independent configuration, credentials, and lifecycle.

- **Init Commands** — Automatically execute CL commands or SQL statements on every new connection (set library lists, environment variables, job attributes) so connections are ready to use immediately.

- **Event-Driven Lifecycle** — `RmPool` extends `EventEmitter` with 8 lifecycle events (`connection:created`, `connection:expired`, `pool:exhausted`, etc.) for monitoring, metrics, and orchestration.

- **Injectable Logging** — Plug in your own logger (Winston, Pino, etc.) via a simple interface. Logs flow hierarchically through the entire class chain with structured service metadata.

- **Hardened Error Handling** — Pool operations are wrapped with contextual error information (pool ID, connection index, job name) and graceful degradation, so a single bad connection doesn't take down your pool.

- **TypeScript-First** — Full type coverage with exported interfaces for all configuration, making misconfiguration a compile-time error rather than a runtime surprise.

## Installation

```bash
npm install rm-connector-js
```

### Prerequisites

- **Remote connections (mapepire):** Requires a Mapepire server running on your IBM i. See [@ibm/mapepire-js](https://www.npmjs.com/package/@ibm/mapepire-js) for details.
- **Native connections (idb):** Requires running on IBM i with `idb-pconnector` available (included as an optional dependency).

## Backend Selection

The `backend` option controls which driver is used:

| Value | Behaviour |
|-------|-----------|
| `'auto'` (default) | Uses `idb` on IBM i (`process.platform === 'os400'`), `mapepire` elsewhere |
| `'mapepire'` | Always use mapepire (remote WebSocket) |
| `'idb'` | Always use idb-pconnector (native IBM i DB2 CLI) |

Both backends produce identical query data. For a detailed comparison of result envelope differences, error message formats, and feature support, see [BACKEND-DIFFERENCES.md](docs/BACKEND-DIFFERENCES.md).

### Pooled connections

```typescript
const pools = new RmPools({
  pools: [{
    id: 'myPool',
    PoolOptions: {
      backend: 'auto',  // or 'mapepire' or 'idb'
      creds: { host: '...', user: '...', password: '...' },  // required for mapepire
    }
  }]
});
```

### Standalone connections

```typescript
// Remote (mapepire)
const conn = new RmConnection({
  backend: 'mapepire',
  creds: { host: '...', user: '...', password: '...' },
});

// Native (idb) — no creds needed, connects to *LOCAL as current user
const conn = new RmConnection({
  backend: 'idb',
});

// Native (idb) — profile swap on *LOCAL
const conn = new RmConnection({
  backend: 'idb',
  creds: { user: 'OTHERUSER', password: 'secret' },
});

// Native (idb) — remote RDB via directory entry (WRKRDBDIRE)
const conn = new RmConnection({
  backend: 'idb',
  creds: { database: 'REMOTERDB', user: 'REMOTEUSER', password: 'secret' },
});

// Auto-detect
const conn = new RmConnection({
  backend: 'auto',
  creds: { host: '...', user: '...', password: '...' },  // used if mapepire is selected
});
```

## Usage

### Basic Setup

```typescript
import { RmPools } from 'rm-connector-js';

const poolsConfig = {
  logLevel: 'debug',
  activate: true,
  pools: [
    {
      id: 'myPool',
      PoolOptions: {
        creds: {
          host: 'your-host',
          user: 'your-user',
          password: 'your-password',
          rejectUnauthorized: false,
        },
        maxSize: 20,
        initialConnections: {
          size: 8,
          expiry: 30 // minutes
        },
        JDBCOptions: {
          libraries: "RMDATA"
        },
        healthCheck: {
          keepalive: 5 // ping idle connections every 5 minutes
        }
      }
    }
  ]
};

const pools = new RmPools(poolsConfig);
await pools.init();
```

### Using a Connection

#### Direct Pool Query (Recommended)

The simplest way to execute queries - the pool automatically handles connection lifecycle:

```typescript
// Get a pool
const pool = await pools.get('myPool');

// Execute a query directly on the pool (auto attach/detach)
const result = await pool.query('SELECT * FROM MY_TABLE');

// With query options
const result = await pool.query('SELECT * FROM MY_TABLE WHERE id = ?', {
  parameters: [123]
});
```

#### Manual Connection Management

For more control, you can manually attach and detach connections:

```typescript
// Get a pool
const pool = await pools.get('myPool');

// Attach a connection
const connection = await pool.attach();

// Execute a query
const result = await connection.query('SELECT * FROM MY_TABLE');

// Detach the connection (return to pool)
await pool.detach(connection);
```

### Standalone Connection

For simple scripts or one-off queries without pooling:

```typescript
import { RmConnection } from 'rm-connector-js';

const conn = new RmConnection({
  creds: { host: '...', user: '...', password: '...' },
  initCommands: [{ command: 'ADDLIBLE MYLIB', type: 'cl' }],
});

await conn.init();
const result = await conn.execute('SELECT * FROM MY_TABLE');
await conn.close();
```

### Configuration Options

#### Pools Options

- `activate`: Auto-activate pools on registration (default: true)
- `logLevel`: Log level threshold: `'error'` | `'info'` | `'debug'` | `'none'` (default: `'info'`). Use `'error'` to suppress informational messages, `'debug'` for verbose output, or `'none'` to suppress all logging.
- `pools`: Array of pool configurations
- `logger`: Custom logger object implementing the `Logger` interface. Flows down to all pools and connections. Defaults to a built-in console logger.

#### Pool Options

- `backend`: Backend driver: `'auto'` | `'mapepire'` | `'idb'` (default: `'auto'`)
- `creds`: Database credentials object - a standard Mapepire DaemonServer object (required for mapepire backend)
- `maxSize`: Maximum number of connections in the pool (default: 20)
- `initialConnections`: Initial connection settings
  - `size`: Number of connections to create on initialization (default: 8)
  - `expiry`: Connection expiry time in minutes (default: null). Set to `null` or omit for connections that never expire. A value of `0` is treated the same as `null` (no expiry). Only positive values start an expiry timer.
- `incrementConnections`: Settings for dynamically added connections
  - `size`: Number of connections to add when pool is exhausted (default: 8)
  - `expiry`: Expiry time for new connections in minutes (same rules as above)
- `logLevel`: Log level threshold for this pool: `'error'` | `'info'` | `'debug'` | `'none'` (overrides the global `logLevel` from Pools Options)
- `JDBCOptions`: JDBC options object. For mapepire backend, this is a standard Mapepire JDBCOptions object. For idb backend, `libraries`, `naming`, `transaction isolation`, and `auto commit` are supported (mapped to native DB2 CLI calls — `SQLSetConnectAttr`, `setLibraryList()`, etc.). See [BACKEND-DIFFERENCES.md](docs/BACKEND-DIFFERENCES.md) for details.
- `initCommands`: Array of commands to execute when each connection is initialized. Each entry is an object with `command` (string) and optional `type` (`'cl'` or `'sql'`, defaults to `'cl'`). CL commands are executed via `QCMDEXC` with parameterised input; SQL commands are executed directly without parameterisation. **Security note:** SQL-type init commands must be trusted, developer-supplied strings — never pass unsanitised user input as an init command.
- `healthCheck`: Health check settings
  - `onAttach`: Verify connections are alive before returning from `attach()` by executing a lightweight query (`VALUES 1`). Unhealthy connections are automatically retired and replaced. (default: `true`). Set to `false` to disable. **Automatically skipped when `multiplex: true` is set** — running a probe on every `pool.query()` call would defeat the throughput benefit of multiplexing. Use `keepalive` instead for health-check coverage in multiplex pools.
  - `keepalive`: Interval in minutes to send keepalive pings (`VALUES 1`) on idle connections, preventing WebSocket connections from being dropped by firewalls or network intermediaries. The timer resets whenever a real query is executed. If a keepalive ping fails, the timer stops and the connection will be retired on the next `attach()` health check. (default: `null` = disabled). Recommended: `5` for most environments. **In multiplex mode this is the only health-check mechanism that runs** — per-attach checks are skipped, so configure `keepalive` if you need any health-check coverage. Note: automatically disabled for the idb backend (no WebSocket to keep alive).
- `multiplex`: Enable shared-connection multiplex mode (default: `false`, **mapepire backend only**). When `true`, each pool connection accepts unlimited concurrent in-flight queries via mapepire-js's native ID-correlated WebSocket protocol and the pool dispatches round-robin across its members. `attach()` no longer claims exclusive ownership; `detach()` becomes a no-op; per-attach health checks are skipped (use `healthCheck.keepalive` instead for periodic background checks). Rejected with an error if combined with `backend: 'idb'`. Best for concurrent/burst workloads against a remote IBM i — leave off for purely sequential traffic. See [Pool sizing under multiplex](#pool-sizing-under-multiplex) below for how the other pool options change behaviour, and [docs/PERFORMANCE-COMPARISON.md](docs/PERFORMANCE-COMPARISON.md#3b-multiplexing) for measurements.
- `logger`: Custom logger object (per-pool override, see Logger below)

#### Pool sizing under multiplex

When `multiplex: true` is set, several of the pool options above behave differently from the default serialized mode. The pool is **fixed-size** — there is no auto-growth — and concurrency is handled by fanning multiple queries through the same connections rather than by opening more. This is a significant departure from the serialized behaviour, so read carefully before setting `multiplex: true` on an existing pool config.

| Option | Behaviour under `multiplex: true` |
|---|---|
| `initialConnections.size` | **This is your pool size.** The number of WebSocket/`SQLJob` connections created at `RmPool.init()` time is the number that stays throughout the pool's lifetime. Set it to the number of parallel Mapepire connections (and therefore QZDASOINIT jobs on the IBM i side) you want serving the workload. |
| `maxSize` | **Used only as a safety cap on initial creation**, via `Math.min(initialConnections.size, maxSize)`. After `init()`, the multiplex attach path never looks at `maxSize` again — it is **not** an elastic ceiling under load. Think of it as "don't let a bad config accidentally create hundreds of initial connections" rather than as a growth limit. |
| `incrementConnections.size` / `.expiry` | **Ignored.** These options drive on-demand pool growth in serialized mode (add more connections when all existing ones are busy). Multiplex mode has no concept of "all connections are busy" because every connection is always shared, so there is nothing to trigger growth. You can leave these at their defaults — they will have no effect. |
| `initialConnections.expiry` | **Rarely useful in multiplex.** The option is still honoured as max-age-from-creation (if the timer fires while `inFlight > 0`, retirement is deferred until in-flight queries finish, and a replacement is auto-created so subsequent attaches still find a connection). But the original purpose of `expiry` was retiring *idle overflow connections* created by `incrementConnections` during spikes — since multiplex has no overflow and no idle/busy distinction, `expiry` has almost nothing to do here. The one narrow use is periodic WebSocket hygiene (force a reconnect every N minutes to dodge long-running Java sessions or NAT/proxy timeouts). For most workloads, set `expiry: null` in multiplex mode. |
| `healthCheck.onAttach` | **Skipped.** Running a `VALUES 1` probe on every `pool.query()` call would defeat the throughput benefit of multiplexing. |
| `healthCheck.keepalive` | **Still active**, and is the recommended replacement for per-attach health checks — it runs a periodic background probe on each connection regardless of mode. |

**Where the in-flight queue lives.** Multiplex does not eliminate queuing — it moves most of it off the client and onto the IBM i, and overlaps client-side serialization with server-side execution. When you fire 500 concurrent queries at a pool of 5 connections (100 in flight per connection), those queries span several buffers:

1. **mapepire-js `globalQueryList`** (client, Node.js) — every `SQLJob.execute()` creates a `Query` object with a unique correlation ID and pushes it into `Query.globalQueryList`, which is how responses are routed back to the right caller. Objects stay until the response arrives.
2. **WebSocket send buffer** (client, Node.js) — the `ws` library buffers serialized JSON frames in user space, then in the kernel TCP socket send buffer. Fast pipelining + slow TCP drain = requests briefly queueing here.
3. **Wire** — whatever TCP segments are in flight.
4. **WebSocket receive buffer** (Mapepire Java server on IBM i) — incoming JSON frames the server has not yet parsed.
5. **Mapepire Java session queue** (Mapepire Java server on IBM i) — each WebSocket session is handled by a Java thread running a message loop: pull a message, execute it via JDBC, send the response. Messages beyond the currently-executing one wait in this queue.
6. **JDBC → QZDASOINIT → Db2** (IBM i) — the Java session hands each query to JDBC/JTOpen, which talks to the QZDASOINIT prestart job backing that mapepire session. QZDASOINIT processes queries sequentially per connection — this is the real throughput bottleneck.

The bulk of deep queuing lives **server-side in steps 4-6**, not in your Node.js process. Client-side (steps 1-2) is cheap — a list of small objects and some JSON bytes. Multiplex wins on latency because steps 1, 3, and 6 run in parallel: while query N is executing on the server, query N+1 is already on the wire and query N+2 is being serialized in Node. That pipeline overlap is the whole reason multiplex is faster than the serialized default, especially over a network where round-trip time is large.

#### Logger

All classes accept an optional `logger` that implements the `Logger` interface:

```typescript
interface Logger {
  log(level: string, message: string, meta?: any): void;
}
```

The logger flows down from `RmPools` → `RmPool` → `RmPoolConnection` → `RmConnection`. You can set it at the top level or per-pool:

```typescript
import { RmPools } from 'rm-connector-js';

// Custom logger (e.g. winston, pino, or any object with a log method)
const myLogger = {
  log(level, message, meta) {
    // Send to your logging infrastructure
    console.log(`[${level}] ${message}`, meta);
  }
};

const pools = new RmPools({
  logger: myLogger,  // All pools and connections use this logger
  pools: [{ id: 'myPool', PoolOptions: { creds: { ... } } }]
});
```

For standalone connections (without pools):

```typescript
import { RmConnection } from 'rm-connector-js';

const conn = new RmConnection({
  creds: { host: '...', user: '...', password: '...' },
  logger: myLogger,
  logLevel: 'info',
});
await conn.init();
```

#### Quick Start: Winston

Winston's `log(level, message, meta)` method matches the `Logger` interface directly, so a Winston logger can be passed in as-is:

```typescript
import winston from 'winston';
import { RmPools } from 'rm-connector-js';

const logger = winston.createLogger({
  level: 'debug',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'db.log' })
  ]
});

const pools = new RmPools({
  logLevel: 'debug',
  logger,
  pools: [
    {
      id: 'myPool',
      PoolOptions: { creds: { host: '...', port: 8076, user: '...', password: '...' } }
    }
  ]
});

await pools.init();
```

All pool and connection activity will now flow through Winston — no adapter needed.

## API Reference

### RmPools

Main class for managing multiple connection pools.

#### Methods

- `init()`: Initialize all registered pools
- `register(poolConfig)`: Register a new pool configuration
- `get(poolId?)`: Get a pool by ID (returns first pool if ID not provided)
- `attach(pool)`: Attach a connection from the pool
- `close()`: Close all active pools and mark them inactive
- `connectionDiag(poolId, connection, sql)`: Log connection diagnostics
- `getInfo()`: Get information about all pools for debugging
- `printInfo()`: Print all pools info to console
- `printStats()`: Print summary statistics for all pools

### RmPool

Manages a pool of database connections. Extends `EventEmitter`.

#### Methods

- `init()`: Initialize the pool with initial connections
- `query(sql, opts?)`: Execute a SQL query using a connection from the pool (automatically handles attach/detach)
- `attach()`: Get an available connection from the pool
- `detach(connection)`: Return a connection to the pool
- `retire(connection)`: Remove a connection from the pool permanently
- `detachAll()`: Return all connections to the pool
- `retireAll()`: Remove all connections from the pool
- `close()`: Close all connections in the pool (alias for `retireAll()`)
- `getInfo()`: Get detailed pool information for debugging
- `getStats()`: Get pool statistics summary
- `printInfo()`: Print detailed pool information to console
- `printStats()`: Print pool statistics to console

#### Events

- `pool:initialized` — Pool init complete. Payload: `{ poolId, connections }`
- `connection:created` — New connection added. Payload: `{ poolId, poolIndex, jobName }`
- `connection:attached` — Connection handed to consumer. Payload: `{ poolId, poolIndex }`
- `connection:detached` — Connection returned to pool. Payload: `{ poolId, poolIndex }`
- `connection:retired` — Connection removed from pool. Payload: `{ poolId, poolIndex }`
- `connection:expired` — Expiry timer fired. Payload: `{ poolId, poolIndex }`
- `connection:healthCheckFailed` — Health check failed before retire. Payload: `{ poolId, poolIndex }`
- `pool:exhausted` — Max connections reached. Payload: `{ poolId, maxSize }`

```typescript
const pool = await pools.get('myPool');
pool.on('connection:created', ({ poolId, poolIndex, jobName }) => {
  console.log(`New connection ${poolIndex} created (job: ${jobName})`);
});
pool.on('pool:exhausted', ({ poolId, maxSize }) => {
  console.warn(`Pool ${poolId} exhausted at ${maxSize} connections`);
});
```

### RmPoolConnection

Represents a single pooled database connection.

#### Methods

- `query(sql, opts?)`: Execute a SQL query
- `detach()`: Mark the connection as available and return it
- `retire()`: Close and retire the connection
- `isAvailable()`: Check if the connection is available
- `isHealthy()`: Check if the underlying connection is still alive (executes `VALUES 1`)
- `getStatus()`: Get the current status of the underlying job
- `getInfo()`: Get connection information for debugging
- `printInfo()`: Print connection info to console

### RmConnection

Represents a standalone database connection (not pooled).

#### Constructor

```typescript
const conn = new RmConnection({
  backend: 'auto',           // 'auto' | 'mapepire' | 'idb'
  creds: { ... },            // DaemonServer object (required for mapepire)
  JDBCOptions: { ... },      // JDBC options
  initCommands: [],           // Init commands array
  logLevel: 'info',           // Log level
  logger: myLogger,           // Custom logger
  keepalive: 5,               // Keepalive interval in minutes (mapepire only)
  multiplex: false,           // Allow concurrent in-flight queries on this connection (mapepire only)
});
```

#### Methods

- `init()`: Initialize the connection and connect to the database
- `execute(sql, opts?)`: Execute a SQL statement
- `query(sql, opts?)`: Execute a SQL query (alias for `execute`)
- `close()`: Close the connection
- `getStatus()`: Get the current status of the underlying job
- `getInfo()`: Get connection information for debugging
- `printInfo()`: Print connection info to console

## Testing

### Unit tests

Run the standard test suite (works on any platform, uses mocks):

```bash
npm test
```

### Backend parity tests

These integration tests run the same operations against both the mapepire and idb backends on a real IBM i system, then compare the results to ensure they produce equivalent output. They cover:

- Simple and parameterized queries
- Data types, string trimming, column names
- CL and SQL init commands
- Error handling (invalid SQL, syntax errors)
- JDBCOptions: `libraries`, `naming`, `transaction isolation`, `auto commit`
- Combined JDBCOptions

The parity tests require:
- Running on IBM i (for the idb backend)
- A running mapepire server on the target system
- Environment variables for mapepire credentials

```bash
IBMI_HOST=myibmi.com IBMI_USER=MYUSER IBMI_PASSWORD=MYPASS npm run test:parity

# or ...
export IBMI_HOST=myibmi.com IBMI_USER=MYUSER IBMI_PASSWORD=MYPASS
npm run test:parity

# To measure the practical ceiling for BLOB round-trips on each backend
RM_RUN_SIZING=1 npm run test:parity -- --testNamePattern="BLOB-sizing"
```

When credentials are not set, the tests are automatically skipped:

```bash
npm run test:parity
# Test Suites: 1 skipped, 0 of 1 total
```

## License

ISC

## Author

Richard Moulton

## Updating to the latest version

```bash
npm update rm-connector-js
```

## Releasing

This project uses [npm version](https://docs.npmjs.com/cli/v10/commands/npm-version) for versioning. Versions follow [semver](https://semver.org/).

```bash
# 1. Merge dev into main
git checkout main
git merge dev

# 2. Bump version (choose one)
npm version patch   # 0.1.0 → 0.1.1 (bug fixes)
npm version minor   # 0.1.0 → 0.2.0 (new features)
npm version major   # 0.1.0 → 1.0.0 (breaking changes)

# 3. Push with tags
git push origin main --follow-tags

# 4. Publish to npm
npm login
npm publish

# 5. Return to dev
git checkout dev
git merge main
git push origin dev
```

## Useful References

* [IBM Toolbox for Java JDBC properties](https://javadoc.io/static/net.sf.jt400/jt400/21.0.0/com/ibm/as400/access/doc-files/JDBCProperties.html)
* [SQL CLI: SQLSetConnectAttr - Set a connection attribute](https://www.ibm.com/docs/en/i/7.6.0?topic=functions-sqlsetconnectattr-set-connection-attribute)

