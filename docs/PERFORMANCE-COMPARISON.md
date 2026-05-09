# Performance Comparison: idb-pconnector vs Mapepire

## 1. Introduction

One of the primary motivations behind rm-connector-js is enabling a dual-environment development workflow: develop locally using Mapepire (which can connect to IBM i from various platforms) and deploy to production on IBM i using idb-pconnector for superior performance. This document examines the architectural and performance differences between the two database connectors to validate that assumption with technical evidence.

## 2. Architecture Overview

### idb-pconnector

`idb-pconnector` is a promise-based wrapper around `idb-connector`, the native C++ Node.js addon (N-API) that actually calls the DB2 SQL CLI API directly on IBM i. `idb-connector` is the piece doing the real work — the N-API bridge, the C buffers, the CLI calls into the QSQSRVR job. `idb-pconnector` re-exports its classes and methods, adding `Promise` semantics on top of the original callback-based API so modern `async`/`await` code can use it directly. rm-connector-js imports `idb-pconnector`, so everywhere this document says "idb-pconnector" the behaviour, performance characteristics, and data path are really those of the underlying `idb-connector` addon.

The data path is:

```
Node.js -> idb-pconnector (Promise wrapper) -> idb-connector N-API C++ addon -> DB2 SQL CLI -> QSQSRVR job (shared memory/IPC)
```

- Runs only on IBM i
- No network layer involved; communication with the database is via OS-level IPC
- No server component to install or manage
- Jobs run under the `QSQSRVR` subsystem

**Notes:**
- N-API (Node-API) is Node.js's stable C/C++ interface for building native addons — modules written in C or C++ that can be called directly from JavaScript. In the case of `idb-connector`, the addon is the bridge between your JavaScript code and the IBM i operating system's DB2 SQL CLI (Call Level Interface). `idb-pconnector` sits one level above, translating that addon's callback API into Promises; it does not touch the data path.
- When `idb-connector` calls the DB2 SQL CLI, it doesn't talk to the database engine directly in the same process. Instead, the SQL CLI communicates with a QSQSRVR job — a separate prestart job running on IBM i that handles the actual database work. IPC (Inter-Process Communication) refers to how these two processes talk to each other. On IBM i, this happens through OS-level mechanisms like shared memory segments rather than network sockets. The key point is that this communication stays entirely within the machine's memory — no TCP/IP stack, no serialization to a wire format, no encryption overhead.

### Mapepire

Mapepire is a client-server architecture. A Java-based server runs on IBM i and listens for Secure WebSocket connections. Clients (available for Node.js, Python, etc) communicate with the server over this WebSocket channel. Its data path is:

```
Node.js client -> WebSocket (TLS) over TCP -> Java server -> JDBC/JTOpen -> QZDASOINIT job -> Db2
```

- Can connect from various platforms (local dev machines, cloud, containers)
- Requires the Mapepire server component to be installed and running on IBM i
- Uses JSON-over-WebSocket as its wire protocol
- Jobs run under the `QZDASOINIT` subsystem

## 3. Benchmark Introduction

### 3a. Test Environment

The benchmarks are run from two locations: locally on the IBM i (loopback) and from a remote development workstation over a real network. Each scenario in [Section 4](#4-benchmark-results) is measured at every applicable (backend, location) combination.

**Local — IBM i**

- Platform: IBM i 7.5 on IBM Cloud PowerVS LPAR (POWER10, processor feature `EDP2`)
- Resources: 2 active CPUs, 4 GB RAM active
- Node.js: v22.22.1
- Connection: loopback (`localhost`)

**Remote — development workstation**

- Hardware: Star Labs StarBook VI
- OS: Linux Mint 22.3
- CPU: AMD Ryzen 7 5800U (8 cores)
- RAM: 32 GiB
- Node.js: v22.21.1
- Network: residential broadband to the IBM Cloud PowerVS endpoint, ~99 ms RTT (median, 4-ping sample)

**Common to all runs**

- Pool size: 5 connections (50 in Scenario 2 — see [3f](#3f-benchmark-scenarios))
- Standard query: `SELECT * FROM SAMPLE.DEPARTMENT`
- Large result set query: `SELECT * FROM SAMPLE.EMPLOYEE CROSS JOIN (VALUES 1,2,3,4,5,6,7,8,9,10) AS T(N)` (~420 rows)
- Queries per scenario: 50, 200, 1000 (3 warm-up queries excluded from measurement)
- Runs per scenario: 3 (values reported are the median of those 3 runs)

### 3b. Multiplexing

A pool connection can operate in one of two modes: **serialized** (one query at a time) or **multiplex** (many queries at once on the same connection).

**Serialized mode.** Each pool connection handles a single query end-to-end before accepting another. If 100 callers want to query at the same time and the pool has 5 connections, queries 6 onwards have to wait their turn. **idb-pconnector only supports serialized mode** — its DB2 SQL CLI handle can only process one query at a time, so the only way to get concurrency on the idb side is to grow the pool.

**Multiplex mode.** A single connection can carry many queries at once. mapepire supports this natively: its WebSocket protocol gives each query a correlation ID, sends it immediately, and matches each response back to the right caller. The connection isn't tied up while a query runs, so multiple callers can share it.

By default, rm-connector-js puts both backends in serialized mode — the lowest common denominator that works for either side. For mapepire pools you can opt in to multiplexing by setting `multiplex: true` on the pool config. With multiplex enabled:

- Pool connections become **shared**: multiple `pool.query()` calls on the same connection run in parallel, with mapepire handling the response routing.
- The pool round-robins requests across its connections, so traffic is spread evenly.
- Per-attach health checks are skipped (use `keepalive` instead — see [3c](#3c-health-check)).
- A connection's expiry is still honoured, but if queries are still in flight when it fires, retirement waits until those finish.
- Pool stats expose an in-flight counter; the usual "available / busy" counts no longer carry their normal meaning.

`multiplex: true` is rejected for the idb backend — DB2 SQL CLI doesn't support multiplexing.

**One subtle gotcha worth knowing.** The native mapepire pool also multiplexes, but it dispatches each new query to the first job that's ready. On fast loopback this funnels most of the work onto the earliest connection and leaves the rest underused. rm-connector-js's multiplex implementation uses round-robin instead, which spreads work evenly across all the pool's connections. Section 4 quantifies the difference.

### 3c. Health Check

The pool offers two health-check options:

1. **`onAttach`** (default `true`) — Each time the pool hands a connection to a caller, that connection first runs a quick `VALUES 1` query to confirm it's still alive. If the check fails, the connection is dropped and the pool looks for another. This applies whenever the pool is in **serialized mode**: all idb pools (idb can't multiplex) and any mapepire pool that hasn't opted in to `multiplex: true`. Multiplex mode skips per-attach checks unconditionally.
2. **`keepalive`** (default off) — A background timer probes idle connections at the configured interval, expressed in minutes. The timer only fires on connections that are *idle*, so during a sustained burst of queries it may not run at all. Useful for long-lived idle connections that might otherwise be silently dropped by something in between (firewall, load balancer, mapepire server timeout).

The two are independent — both can be on, both can be off. In multiplex mode `onAttach` is forced to a no-op (every connection is shared and may have multiple queries in flight, so running `VALUES 1` on every `pool.query()` call would defeat the point of multiplexing). For multiplex pools that need any health-check coverage, configure `keepalive`.

**`onAttach` doubles as an implicit rate-limiter under contention.** When all connections are busy and a new caller asks for one, the pool **does not wait** — it throws `Maximum number of connections reached` straight back at the caller. There's no FIFO of waiting callers. What stops this from happening under normal concurrency is that calls to `attach()` themselves are serialized — only one runs at a time, in order. With `onAttach: true`, every successful attach has to await a `VALUES 1` round-trip before it returns: a few tens of microseconds on loopback, more on a remote network. That tiny delay between successive attach calls is what gives earlier queries time to call `detach()` and release their connections before the next attach asks "is anything available?". With `onAttach: false`, attach calls run back-to-back at memory speed, and the first one to find every connection still busy at the pool's max size gets the throw. This dependency is verified by [`tests/performance/pool-contention-proof.test.ts`](../tests/performance/pool-contention-proof.test.ts), which shows the same workload succeeding with `onAttach: true` and failing with `onAttach: false`.

This is why Scenario 2 (`onAttach: false`) raises the pool size to 50 — without the implicit rate-limiter, pool=5 with high concurrency would just measure pool-exhaustion failures, not throughput.

### 3d. Per Query vs Wall Clock Times

The benchmarks report two distinct timings:

- **Per-query median** — Within one run of a scenario, the test executes N queries (50, 200, or 1000) and records each query's elapsed time individually. The median of those N times is the run's value. Each (scenario, query-count) combination runs 3 independent times; the reported cell is the **median of those 3 medians**. Best for "what does one query cost" in sequential scenarios.
- **Wall clock** — The total elapsed time to process the entire batch of N queries end-to-end, from before the first query is fired to after the last one resolves. Reported as median of 3 runs. Best for "how long does this batch take".

⚠️ **Per-query medians can be misleading for `Promise.all` rows.** Calls to `attach()` are serialized (one at a time, in order — see [3c](#3c-health-check)), so when many queries fire concurrently the later ones spend time waiting their turn before they can start executing. The per-query timer starts when the query is *created*, not when it begins executing, so for later callers the recorded "per-query time" includes attach-queue time on top of the actual execution time. In multiplex mode this effect is small — attach is essentially instant once it's the caller's turn, and queries then run in parallel on the shared connections. In serialized mode with high concurrency it can dominate. **Wall clock** is the right metric for `Promise.all` rows when comparing backends or talking about throughput.

### 3e. What each scenario measures

Each scenario in [Section 4](#4-benchmark-results) runs the same seven sub-tests, defined in [`tests/performance/rm-backend-performance.test.ts`](../tests/performance/rm-backend-performance.test.ts) (and mirrored in [`tests/performance/native-backend-performance.test.ts`](../tests/performance/native-backend-performance.test.ts) for Scenario 5). What each one measures:

- **Connection creation** — Constructs a fresh connection, initialises it (the timed step), then closes it. Repeats 10 times in a loop. The cell value is the median per-iteration initialise connection time. For idb the *first* iteration in any session pays a cold start penalty (visible in the max but not the median); subsequent iterations drop to single-digit ms. Reported in its own table per scenario, since the figure is consistent across query counts.
- **Single sequential** — Opens one connection, then runs N queries serially in a `for` loop, awaiting each query response. Each query is timed individually; the cell is the median single-query execution time on a warm connection. Closest representation of "the cost of one query in a long-running script".
- **Single sequential (large)** — Same shape as Single sequential but with the larger SQL result set (~420 rows). Shows how per-query cost shifts when DB execution and result-set transfer dominate over protocol overhead.
- **Single Promise.all** — Opens one connection, fires N queries concurrently. On the mapepire side, multiplexing handles the concurrency natively — all N queries are in flight at once on the same connection. On the idb side, the queries serialize through the single connection (idb can't multiplex). ⚠️ Per-query median is misleading — see [3d](#3d-per-query-vs-wall-clock-times); use Wall Clock.
- **Pool sequential** — Creates a pool, runs N queries serially (each call grabs a connection, runs the query, releases it). Represents a single user (or single request handler) making queries one after another against a shared pool — only one connection is in use at any moment, and the rest sit idle. Closest match to typical production traffic. Every query incurs an attach + (when `onAttach=true`) per-attach health check + detach on top of the SQL work.
- **Pool Promise.all** — Same pool, but fires N queries concurrently. Represents a peak of concurrent query demand — the moment many requests hit the pool at once. This is where the pool's concurrency handling gets stressed: the attach mutex, the implicit rate-limiter, multiplex sharing — see [3b](#3b-multiplexing) and [3c](#3c-health-check). ⚠️ Per-query median is misleading; use Wall Clock.
- **Parameterized sequential** — Opens one connection, runs N parameterised queries serially. Same shape as Single sequential but with a bound parameter; confirms whether the per-query overhead is sensitive to query shape (it isn't — protocol overhead dominates either way).

### 3f. Benchmark Scenarios

Five scenarios capture the realistic configurations of `rm-connector-js` plus a native-driver baseline. Each scenario produces results for every applicable (backend, location) combination listed.

1. **Scenario 1 — rm-connector-js defaults** (health check, multiplex off)
   - idb local
   - mapepire local
   - mapepire remote
2. **Scenario 2 — rm-connector-js without health check** (multiplex off)
   - idb local
   - mapepire local
   - mapepire remote
3. **Scenario 3 — rm-connector-js with multiplex** (no health check)
   - mapepire local
   - mapepire remote
4. **Scenario 4 — rm-connector-js with multiplex and keepalive** (background health check on idle connections)
   - mapepire local
   - mapepire remote
5. **Scenario 5 — native drivers** (baseline - no rm-connector-js wrapper)
   - idb local
   - mapepire local
   - mapepire remote

**Scenario configurations and rationale:**

| # | Scenario | Configuration | Why this scenario |
|---|---|---|---|
| 1 | rm default | onAttach=true, multiplex=false, pool=5 | The default user experience — the configuration most apps will land on |
| 2 | rm onAttach=false | onAttach=false, multiplex=false, **pool=50** | Isolates the cost of the per-attach health check. Pool raised to 50 because removing `onAttach` removes the implicit rate-limiter (see [3c](#3c-health-check)) |
| 3 | rm multiplex=true | multiplex=true, pool=5 | Multiplex code path with no health checks. mapepire only — `multiplex: true` is rejected for idb (DB2 SQL CLI can't multiplex) |
| 4 | rm multiplex + keepalive | multiplex=true, keepalive=0.05 min, pool=5 | Realistic production configuration for a multiplex pool that needs health-check coverage |
| 5 | Native drivers (baseline) | (no rm-connector-js wrapper on the data path) | Comparing against Scenarios 1–4 isolates the rm-connector-js wrapper overhead |

### 3g. Reproducing These Benchmarks

The benchmark suite is in [`tests/performance/`](../tests/performance/). All tests require the SAMPLE schema on the target IBM i and the env vars `IBMI_HOST`, `IBMI_USER`, `IBMI_PASSWORD`.

Create the SAMPLE schema once if it doesn't already exist:

```sql
CALL QSYS.CREATE_SQL_SAMPLE('SAMPLE');
```

Ensure that the Mapepire server is installed and running.

```shell
yum install sc
yum install mapepire-server
sc start mapepire
```

The simplest way to reproduce is via [`tests/performance/bench-runs.sh`](../tests/performance/bench-runs.sh), which orchestrates 3 runs × 3 query counts (50, 200, 1000) per phase and saves output to `$HOME/bench-results/`:

```bash
export IBMI_HOST=<host> IBMI_USER=<user> IBMI_PASSWORD=<pass>

# Scenario 5 — Native baseline
bash tests/performance/bench-runs.sh native

# Scenario 1 — rm default (onAttach=true, pool=5)
bash tests/performance/bench-runs.sh rm

# Scenario 2 — rm onAttach=false, pool=50
bash tests/performance/bench-runs.sh rm-no-onattach

# Scenario 3 — rm multiplex=true (mapepire only)
bash tests/performance/bench-runs.sh rm-mux

# Scenario 4 — rm multiplex=true with keepalive=0.05 min
bash tests/performance/bench-runs.sh rm-mux-keepalive
```

For local-on-IBM-i runs, set `IBMI_HOST=localhost`. For remote runs (workstation → IBM i), set `IBMI_HOST` to the IBM i hostname.

To run a single phase manually with `jest`, set the relevant env vars and call `npm run test:performance`:

```bash
export IBMI_HOST=<host> IBMI_USER=<user> IBMI_PASSWORD=<pass>

# Scenario 1, single run
npm run test:performance -- --testPathPatterns=rm-backend-performance

# Scenario 5, single run
npm run test:performance -- --testPathPatterns=native-backend-performance
```

The rm-connector-js test (`rm-backend-performance.test.ts`) honours four extra env vars used by `bench-runs.sh` to switch between scenarios:

- `RM_ON_ATTACH` (default `true`) — health-check on each `attach()` call.
- `RM_MULTIPLEX` (default `false`) — enable multiplex mode (mapepire only; skips the idb branch).
- `RM_KEEPALIVE` (default unset / disabled) — keepalive interval in minutes; fractional values allowed.
- `RM_POOL_SIZE` (default `5`) — `maxSize` and `initialConnections.size` on the pool.

`QUERY_COUNT` (default 50) and `SAMPLE_SCHEMA` (default `SAMPLE`) are accepted by both test files.

## 4. Benchmark Results

*Results pending — to be populated once the benchmark phases are run (see [3g](#3g-reproducing-these-benchmarks)). Each scenario sub-section will contain three tables: Connection creation (a single value per backend), Per Query (median per-query times for the non-Promise.all sub-scenarios at 50/200/1000 queries), and Wall Clock (Promise.all sub-scenarios at 50/200/1000 queries).*

### 4.1 Scenario 1 — rm-connector-js defaults

*Pending.*

### 4.2 Scenario 2 — rm-connector-js without health check

*Pending.*

### 4.3 Scenario 3 — rm-connector-js with multiplex

*Pending.*

### 4.4 Scenario 4 — rm-connector-js with multiplex and keepalive

*Pending.*

### 4.5 Scenario 5 — native drivers

*Pending.*

## 5. Conclusion

*Pending — to be written after the results in [Section 4](#4-benchmark-results) are in. Will cover: rm-connector-js vs native, idb vs mapepire, local vs remote, multiplexing, health check, and a closing summary of preferred options.*

## 6. References

- [IBM/nodejs-idb-connector (GitHub)](https://github.com/IBM/nodejs-idb-connector)
- [IBM/nodejs-idb-pconnector (GitHub)](https://github.com/IBM/nodejs-idb-pconnector)
- [Mapepire-IBMi/mapepire-server (GitHub)](https://github.com/Mapepire-IBMi/mapepire-server)
- [Mapepire documentation](https://mapepire-ibmi.github.io/)
- [Mapepire: A new IBM i database client (Liam)](https://github.com/worksofliam/blog/issues/68)
- [Mapepire: Node.js performance testing against ODBC (Liam)](https://github.com/worksofliam/blog/issues/69)
- [IBM Introduces Mapepire (IT Jungle)](https://www.itjungle.com/2024/09/09/ibm-introduces-mapepire-the-new-db2-for-i-client/)
