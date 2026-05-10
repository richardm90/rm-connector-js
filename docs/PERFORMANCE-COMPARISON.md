# Performance Comparison: idb-pconnector vs Mapepire

## Contents

- [1. Introduction](#1-introduction)
- [2. Architecture Overview](#2-architecture-overview)
- [3. Benchmark Introduction](#3-benchmark-introduction)
  - [3a. Test Environment](#3a-test-environment)
  - [3b. Multiplexing](#3b-multiplexing)
  - [3c. Health Check](#3c-health-check)
  - [3d. Per Query vs Wall Clock Times](#3d-per-query-vs-wall-clock-times)
  - [3e. What each scenario measures](#3e-what-each-scenario-measures)
  - [3f. Benchmark Scenarios](#3f-benchmark-scenarios)
  - [3g. Reproducing These Benchmarks](#3g-reproducing-these-benchmarks)
- [4. Benchmark Results](#4-benchmark-results)
  - [Connection creation (across all scenarios)](#connection-creation-across-all-scenarios)
  - [4.1 Scenario 1 — rm-connector-js defaults](#41-scenario-1--rm-connector-js-defaults)
  - [4.2 Scenario 2 — rm-connector-js without health check](#42-scenario-2--rm-connector-js-without-health-check)
  - [4.3 Scenario 3 — rm-connector-js with multiplex](#43-scenario-3--rm-connector-js-with-multiplex)
  - [4.4 Scenario 4 — rm-connector-js with multiplex and keepalive](#44-scenario-4--rm-connector-js-with-multiplex-and-keepalive)
  - [4.5 Scenario 5 — native drivers](#45-scenario-5--native-drivers)
- [5. Conclusion](#5-conclusion)
  - [5a. rm-connector-js vs native drivers (wrapper overhead)](#5a-rm-connector-js-vs-native-drivers-wrapper-overhead)
  - [5b. idb vs mapepire](#5b-idb-vs-mapepire)
  - [5c. Local vs remote](#5c-local-vs-remote)
  - [5d. Connection creation](#5d-connection-creation)
  - [5e. Multiplexing](#5e-multiplexing)
  - [5f. Health check (onAttach and keepalive)](#5f-health-check-onattach-and-keepalive)
  - [5g. Preferred options — summary](#5g-preferred-options--summary)
- [6. References](#6-references)
- [Appendix A: Pool growth under burst — `DBPool` vs `RmPool`](#appendix-a-pool-growth-under-burst--dbpool-vs-rmpool)

## 1. Introduction

One of the primary motivations behind rm-connector-js is enabling a dual-environment development workflow: develop locally using Mapepire (which can connect to IBM i from various platforms) and deploy to production on IBM i using idb-pconnector for superior performance. This document examines the architectural and performance differences between the two database connectors to validate that assumption with technical evidence.

[↑ Back to contents](#contents)

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

[↑ Back to contents](#contents)

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
- Network: residential broadband to the IBM Cloud PowerVS endpoint, ~99 ms round-trip time (abbreviated to RTT going forward)

**Common to all runs**

- Standard query: `SELECT * FROM SAMPLE.DEPARTMENT`
- Large result set query: `SELECT * FROM SAMPLE.EMPLOYEE CROSS JOIN (VALUES 1,2,3,4,5,6,7,8,9,10) AS T(N)` (~420 rows)
- Queries per scenario: 50, 200, 1000 (3 warm-up queries excluded from measurement)
- Runs per scenario: 3 (values reported are the median of those 3 runs)

### 3b. Multiplexing

A pool connection can operate in one of two modes: **serialized** (one query at a time) or **multiplex** (many queries at once on the same connection).

**Serialized mode.** Each pool connection handles a single query end-to-end before accepting another. If 100 callers want to query at the same time and the pool has only 5 connections, only 5 queries can be in flight at once — the rest have to wait until the pool can give them a connection (either an existing one freeing up, or a new one being created if the pool is configured to grow). **idb-pconnector only supports serialized mode** — its DB2 SQL CLI handle can only process one query at a time, so the only way to get concurrency on the idb side is via a pool of separate connections.

**Multiplex mode.** A single connection can carry many queries at once. mapepire supports this natively: its WebSocket protocol gives each query a correlation ID, sends it immediately, and matches each response back to the right caller. The connection isn't tied up while a query runs, so multiple callers can share it.

By default, rm-connector-js puts both backends in serialized mode — the lowest common denominator that works for either side. For mapepire pools you can opt in to multiplexing by setting `multiplex: true` on the pool config. With multiplex enabled:

- Pool connections become **shared**: multiple `pool.query()` calls on the same connection run in parallel, with mapepire handling the response routing.
- The pool round-robins requests across its connections, so traffic is spread evenly.
- Per-attach health checks are skipped (use `keepalive` instead — see [3c](#3c-health-check)).
- A connection's expiry is still honoured, but if queries are still in flight when it fires, retirement waits until those finish.
- Pool stats expose an in-flight counter; the usual "available / busy" counts no longer carry their normal meaning.

`multiplex: true` is rejected for the idb backend — DB2 SQL CLI doesn't support multiplexing.

**One subtle dispatch difference worth knowing.** rm-connector-js's multiplex implementation uses blind round-robin across pool members. The native mapepire pool's `getJob()` is two-stage: it picks the first idle job (status `ready`) and falls back to the busy job with the fewest in-flight queries when all are busy. Both strategies spread work; they just do it differently. Section 4 measures the wall-clock gap between them.

### 3c. Health Check

The pool offers two health-check options:

1. **`onAttach`** (default `true`) — Each time the pool hands a connection to a caller, that connection first runs a quick `VALUES 1` query to confirm it's still alive. If the check fails, the connection is dropped and the pool looks for another. This applies whenever the pool is in **serialized mode**: all idb pools (idb can't multiplex) and any mapepire pool that hasn't opted in to `multiplex: true`. Multiplex mode skips per-attach checks unconditionally.
2. **`keepalive`** (default off) — A background timer probes idle connections at the configured interval, expressed in minutes. The timer only fires on connections that are *idle*, so during a sustained burst of queries it may not run at all. Useful for long-lived idle connections that might otherwise be silently dropped by something in between (firewall, load balancer, mapepire server timeout).

The two are independent — both can be on, both can be off. In multiplex mode `onAttach` is forced to a no-op (every connection is shared and may have multiple queries in flight, so running `VALUES 1` on every `pool.query()` call would defeat the point of multiplexing). For multiplex pools that need any health-check coverage, configure `keepalive`.

**`onAttach` doubles as an implicit rate-limiter under contention.** When all connections are busy and a new caller asks for one, the pool **does not wait** — it throws `Maximum number of connections reached` straight back at the caller. There's no FIFO of waiting callers. What stops this from happening under normal concurrency is that calls to `attach()` themselves are serialized — only one runs at a time, in order. With `onAttach: true`, every successful attach has to await a `VALUES 1` round-trip before it returns: a few tens of microseconds on loopback, more on a remote network. That tiny delay between successive attach calls is what gives earlier queries time to call `detach()` and release their connections before the next attach asks "is anything available?". With `onAttach: false`, attach calls run back-to-back at memory speed, and the first one to find every connection still busy at the pool's max size gets the throw. This dependency is verified by [`tests/performance/pool-contention-proof.test.ts`](../tests/performance/pool-contention-proof.test.ts), which shows the same workload succeeding with `onAttach: true` and failing with `onAttach: false`.

This is why all rm scenarios share the same growable pool config (see [3f](#3f-benchmark-scenarios)): `maxSize: 1000`, initial 5, increment 5. In scenarios 1, 3, 4 the rate-limiter behaviour means the pool effectively stays at 5 — earlier queries detach in time and growth is rare. In Scenario 2 (`onAttach: false`), attach is no longer gated by a `VALUES 1` round-trip, so under burst load the pool grows on demand toward the cap rather than throwing. The cap is high enough to absorb the test concurrency without exhaustion but low enough to be a sanity check against runaway growth.

### 3d. Per Query vs Wall Clock Times

The benchmarks report two distinct timings:

- **Per-query median** — Within one run of a scenario, the test executes N queries (50, 200, or 1000) and records each query's elapsed time individually. The median of those N times is the run's value. Each (scenario, query-count) combination runs 3 independent times; the reported cell is the **median of those 3 medians**. Best for "what does one query cost" in sequential scenarios.
- **Wall clock** — The total elapsed time to process the entire batch of N queries end-to-end, from before the first query is fired to after the last one resolves. Reported as median of 3 runs. Best for "how long does this batch take".

⚠️ **Per-query medians can be misleading for `Promise.all` rows.** Calls to `attach()` are serialized (one at a time, in order — see [3c](#3c-health-check)), so when many queries fire concurrently the later ones spend time waiting their turn before they can start executing. The per-query timer starts when the query is *created*, not when it begins executing, so for later callers the recorded "per-query time" includes attach-queue time on top of the actual execution time. In multiplex mode this effect is small — attach is essentially instant once it's the caller's turn, and queries then run in parallel on the shared connections. In serialized mode with high concurrency it can dominate. **Wall clock** is the right metric for `Promise.all` rows when comparing backends or talking about throughput.

### 3e. What each scenario measures

Each scenario in [Section 4](#4-benchmark-results) runs the same seven sub-tests, defined in [`tests/performance/rm-backend-performance.test.ts`](../tests/performance/rm-backend-performance.test.ts) (and mirrored in [`tests/performance/native-backend-performance.test.ts`](../tests/performance/native-backend-performance.test.ts) for Scenario 5). What each one measures:

- **Connection creation** — Constructs a fresh connection, initialises it (the timed step), then closes it. Repeats 10 times in a loop. The cell value is the median per-iteration initialise connection time. For idb the *first* iteration in any session pays a cold start penalty (visible in the max but not the median); subsequent iterations drop to single-digit ms. Reported in a single table at the top of [Section 4](#4-benchmark-results) since the figure doesn't vary meaningfully across query counts or across rm scenarios (the options being measured don't affect `init()`).
- **Single sequential** — Opens one connection, then runs N queries serially in a `for` loop on that one connection, awaiting each query response. The connection is then closed at the end. Each query is timed individually; the cell is the median single-query execution time on a warm connection. Closest representation of "the cost of one query in a long-running script".
- **Single sequential (large)** — Same shape as Single sequential but with the larger SQL result set (~420 rows). Shows how per-query cost shifts when DB execution and result-set transfer dominate over protocol overhead.
- **Single Promise.all** — Opens one connection, fires N queries concurrently on that one connection, closes the connection at the end. On the mapepire side, multiplexing handles the concurrency natively — all N queries are in flight at once on the same connection. On the idb side, the queries serialize through the single connection (idb can't multiplex). ⚠️ Per-query median is misleading — see [3d](#3d-per-query-vs-wall-clock-times); use Wall Clock.
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
| 1 | rm default | onAttach=true, multiplex=false | The default user experience — the configuration most apps will land on |
| 2 | rm onAttach=false | onAttach=false, multiplex=false | Isolates the cost of the per-attach health check. Pool grows under burst load since the implicit rate-limiter is gone (see [3c](#3c-health-check)) |
| 3 | rm multiplex=true | multiplex=true | Multiplex code path with no health checks. mapepire only — `multiplex: true` is rejected for idb (DB2 SQL CLI can't multiplex) |
| 4 | rm multiplex + keepalive | multiplex=true, keepalive=0.05 min | Realistic production configuration for a multiplex pool that needs health-check coverage |
| 5 | Native drivers (baseline) | (no rm-connector-js wrapper on the data path) | Comparing against Scenarios 1–4 isolates the rm-connector-js wrapper overhead |

**Scenario pool sizings:**

- **rm-connector-js (Scenarios 1–4)**: `maxSize: 1000`, `initialConnections.size: 5`, `incrementConnections.size: 5`. The pool starts at 5 and grows in increments of 5 up to 1000 only if demand forces it. The attach mutex naturally rate-limits growth (one growth round at a time), so the pool stays near 5 in scenarios with `onAttach: true` (1, 3, 4) and grows on demand in Scenario 2.
- **Native idb (Scenario 5)**: Pool — sequential uses `DBPool` with `incrementSize: 5` (sequential workload, no contention). Pool — Promise.all pre-allocates 50 raw `Connection` objects and dispatches queries round-robin — `DBPool.attach()` is synchronous and grows the pool unbounded under burst, inflating per-query medians dramatically (observed: ~25 s medians at q=1000). Pre-allocated raw connections measure the underlying driver's actual throughput floor at fixed concurrency 50. See [Appendix A](#appendix-a-pool-growth-under-burst--dbpool-vs-rmpool) for a source-level walkthrough of why `DBPool` and `RmPool` behave so differently here.
- **Native mapepire (Scenario 5)**: `Pool` pre-sized at `startingSize: 50, maxSize: 50` — fixed-size, no runtime growth. Unlike `RmPool`, the native mapepire `Pool` has no rate-limiter on growth; under high concurrency it tries to open many fresh WebSocket+TLS connections in parallel and overwhelms the mapepire server on a small LPAR. Pre-sizing avoids that. Pool 50 is large enough to absorb the test concurrency without queue artefacts; comparison with rm's growable pool on Pool Promise.all is therefore not perfectly apples-to-apples and is called out in [Section 5](#5-conclusion).

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

# Scenario 1 — rm default (onAttach=true)
bash tests/performance/bench-runs.sh rm

# Scenario 2 — rm onAttach=false
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

The rm-connector-js test (`rm-backend-performance.test.ts`) honours three extra env vars used by `bench-runs.sh` to switch between scenarios:

- `RM_ON_ATTACH` (default `true`) — health-check on each `attach()` call.
- `RM_MULTIPLEX` (default `false`) — enable multiplex mode (mapepire only; skips the idb branch).
- `RM_KEEPALIVE` (default unset / disabled) — keepalive interval in minutes; fractional values allowed.

Pool sizing is fixed at `maxSize: 1000`, `initialConnections.size: 5`, `incrementConnections.size: 5` across every rm scenario — see [3f](#3f-benchmark-scenarios).

`QUERY_COUNT` (default 50) and `SAMPLE_SCHEMA` (default `SAMPLE`) are accepted by both test files.

[↑ Back to contents](#contents)

## 4. Benchmark Results

Results from the benchmark phases described in [3g](#3g-reproducing-these-benchmarks). Each scenario sub-section below contains a Per Query table (median per-query times for the non-Promise.all sub-scenarios at 50/200/1000 queries) and a Wall Clock table (Promise.all sub-scenarios at 50/200/1000 queries).

### Connection creation (across all scenarios)

Connection creation is presented up front since it doesn't vary meaningfully across rm scenarios — the rm-connector-js options being measured (`onAttach`, `multiplex`, `keepalive`) don't affect `init()`. Native idb is faster than rm-connector-js idb by ~3 ms (the wrapper overhead on connection setup); mapepire local and remote are similar between native and rm (the wrapper's cost is dwarfed by JDBC init).

| Backend × location | Native (Sc 5) | rm-connector-js (Sc 1, representative) |
|---|---|---|
| idb (local) | 3.80 ms | 7.16 ms |
| mapepire (local) | 29.93 ms | 26.03 ms |
| mapepire (remote) | 414.09 ms | 470.12 ms |

Variation across rm scenarios (Sc 1–4) for the same (backend, location) is within session-to-session noise — e.g. mapepire local ranged 26.03–33.52 ms, mapepire remote ranged 425.64–485.25 ms.

### 4.1 Scenario 1 — rm-connector-js defaults

rm-connector-js defaults: `onAttach: true`, `multiplex: false`. Pool config as in [3f](#3f-benchmark-scenarios) — `maxSize: 1000`, initial 5, increment 5. Data from `bench-runs.sh rm` run on the IBM i (loopback) and from the workstation (remote). Each cell is the median across 3 runs at the given query count. The per-attach health check naturally rate-limits attach calls, so the pool stays at 5 connections in practice — growth doesn't fire under these workloads.

#### Per Query (median per-query time)

| Sub-scenario | Backend (location) | 50q | 200q | 1000q |
|---|---|---|---|---|
| Single sequential | idb (local) | **0.65 ms** ✓ | **0.63 ms** ✓ | **0.59 ms** ✓ |
|  | mapepire (local) | 1.29 ms | 1.18 ms | 1.12 ms |
|  | mapepire (remote) | 106.71 ms | 106.47 ms | 104.07 ms |
| Single sequential (large) | idb (local) | 23.89 ms | 24.39 ms | 23.97 ms |
|  | mapepire (local) | **7.04 ms** ✓ | **6.70 ms** ✓ | **6.62 ms** ✓ |
|  | mapepire (remote) | 223.95 ms | 210.35 ms | 213.91 ms |
| Pool sequential | idb (local) | **0.88 ms** ✓ | **0.79 ms** ✓ | **0.78 ms** ✓ |
|  | mapepire (local) | 2.14 ms | 1.98 ms | 1.84 ms |
|  | mapepire (remote) | 215.66 ms | 207.45 ms | 210.74 ms |
| Parameterized sequential | idb (local) | **0.44 ms** ✓ | **0.43 ms** ✓ | **0.42 ms** ✓ |
|  | mapepire (local) | 1.30 ms | 1.24 ms | 1.26 ms |
|  | mapepire (remote) | 104.76 ms | 104.52 ms | 103.86 ms |

(✓ marks the lowest value per column within each sub-scenario.)

#### Wall Clock (Promise.all batch end-to-end)

| Sub-scenario | Backend (location) | 50q | 200q | 1000q |
|---|---|---|---|---|
| Single Promise.all | idb (local) | 34.10 ms | 112.90 ms | **578.22 ms** ✓ |
|  | mapepire (local) | **30.31 ms** ✓ | **111.18 ms** ✓ | 645.49 ms |
|  | mapepire (remote) | 229.02 ms | 365.50 ms | 608.91 ms |
| Pool Promise.all | idb (local) | **32.97 ms** ✓ | **109.55 ms** ✓ | **517.11 ms** ✓ |
|  | mapepire (local) | 92.38 ms | 281.53 ms | 1294.65 ms |
|  | mapepire (remote) | 5686.95 ms | 22362.98 ms | 111075.92 ms |

(✓ marks the lowest value per column within each sub-scenario.)

### 4.2 Scenario 2 — rm-connector-js without health check

rm-connector-js with `onAttach: false`, `multiplex: false`. Pool config as in [3f](#3f-benchmark-scenarios) — `maxSize: 1000`, initial 5, increment 5. Data from `bench-runs.sh rm-no-onattach` run on the IBM i (loopback) and from the workstation (remote). Each cell is the median across 3 runs at the given query count.

#### Per Query (median per-query time)

| Sub-scenario | Backend (location) | 50q | 200q | 1000q |
|---|---|---|---|---|
| Single sequential | idb (local) | **0.65 ms** ✓ | **0.61 ms** ✓ | **0.57 ms** ✓ |
|  | mapepire (local) | 1.35 ms | 1.21 ms | 1.13 ms |
|  | mapepire (remote) | 107.37 ms | 102.08 ms | 102.47 ms |
| Single sequential (large) | idb (local) | 24.06 ms | 24.67 ms | 24.92 ms |
|  | mapepire (local) | **6.78 ms** ✓ | **6.78 ms** ✓ | **6.84 ms** ✓ |
|  | mapepire (remote) | 220.03 ms | 208.04 ms | 207.52 ms |
| Pool sequential | idb (local) | **0.58 ms** ✓ | **0.58 ms** ✓ | **0.56 ms** ✓ |
|  | mapepire (local) | 1.22 ms | 1.13 ms | 1.08 ms |
|  | mapepire (remote) | 111.90 ms | 99.84 ms | 100.89 ms |
| Parameterized sequential | idb (local) | **0.42 ms** ✓ | **0.42 ms** ✓ | **0.43 ms** ✓ |
|  | mapepire (local) | 1.33 ms | 1.21 ms | 1.24 ms |
|  | mapepire (remote) | 111.32 ms | 101.10 ms | 104.23 ms |

(✓ marks the lowest value per column within each sub-scenario.)

#### Wall Clock (Promise.all batch end-to-end)

| Sub-scenario | Backend (location) | 50q | 200q | 1000q |
|---|---|---|---|---|
| Single Promise.all | idb (local) | 35.32 ms | 116.59 ms | **589.39 ms** ✓ |
|  | mapepire (local) | **29.24 ms** ✓ | **108.43 ms** ✓ | 662.53 ms |
|  | mapepire (remote) | 250.48 ms | 334.41 ms | 628.19 ms |
| Pool Promise.all | idb (local) | **106.56 ms** ✓ | **288.87 ms** ✓ | **4184.43 ms** ✓ |
|  | mapepire (local) | 812.82 ms | 2073.04 ms | 6151.78 ms |
|  | mapepire (remote) | 2543.78 ms | 6494.81 ms | 22104.36 ms |

(✓ marks the lowest value per column within each sub-scenario.)

### 4.3 Scenario 3 — rm-connector-js with multiplex

rm-connector-js with `multiplex: true`. mapepire only — `multiplex: true` is rejected for idb. Pool config as in [3f](#3f-benchmark-scenarios) — `maxSize: 1000`, initial 5, increment 5. Per-attach health checks are skipped (no `keepalive` configured). Data from `bench-runs.sh rm-mux` run on the IBM i (loopback) and from the workstation (remote). Each cell is the median across 3 runs at the given query count.

#### Per Query (median per-query time)

| Sub-scenario | Backend (location) | 50q | 200q | 1000q |
|---|---|---|---|---|
| Single sequential | mapepire (local) | **1.31 ms** ✓ | **1.25 ms** ✓ | **1.15 ms** ✓ |
|  | mapepire (remote) | 106.77 ms | 104.11 ms | 102.76 ms |
| Single sequential (large) | mapepire (local) | **6.86 ms** ✓ | **6.71 ms** ✓ | **6.68 ms** ✓ |
|  | mapepire (remote) | 212.28 ms | 219.04 ms | 216.64 ms |
| Pool sequential | mapepire (local) | **1.26 ms** ✓ | **1.16 ms** ✓ | **1.10 ms** ✓ |
|  | mapepire (remote) | 103.84 ms | 111.95 ms | 105.98 ms |
| Parameterized sequential | mapepire (local) | **1.35 ms** ✓ | **1.24 ms** ✓ | **1.27 ms** ✓ |
|  | mapepire (remote) | 106.59 ms | 103.31 ms | 101.27 ms |

(✓ marks the lowest value per column within each sub-scenario.)

#### Wall Clock (Promise.all batch end-to-end)

| Sub-scenario | Backend (location) | 50q | 200q | 1000q |
|---|---|---|---|---|
| Single Promise.all | mapepire (local) | **32.80 ms** ✓ | **111.80 ms** ✓ | **661.28 ms** ✓ |
|  | mapepire (remote) | 231.87 ms | 380.02 ms | 685.17 ms |
| Pool Promise.all | mapepire (local) | **27.70 ms** ✓ | **84.46 ms** ✓ | **434.60 ms** ✓ |
|  | mapepire (remote) | 224.56 ms | 289.21 ms | 543.71 ms |

(✓ marks the lowest value per column within each sub-scenario.)

### 4.4 Scenario 4 — rm-connector-js with multiplex and keepalive

rm-connector-js with `multiplex: true` and `keepalive: 0.05` (3-second background probe interval). mapepire only. Pool config as in [3f](#3f-benchmark-scenarios) — `maxSize: 1000`, initial 5, increment 5. Data from `bench-runs.sh rm-mux-keepalive` run on the IBM i (loopback) and from the workstation (remote). Each cell is the median across 3 runs at the given query count.

#### Per Query (median per-query time)

| Sub-scenario | Backend (location) | 50q | 200q | 1000q |
|---|---|---|---|---|
| Single sequential | mapepire (local) | **1.29 ms** ✓ | **1.27 ms** ✓ | **1.18 ms** ✓ |
|  | mapepire (remote) | 104.48 ms | 101.42 ms | 105.96 ms |
| Single sequential (large) | mapepire (local) | **7.21 ms** ✓ | **6.84 ms** ✓ | **6.73 ms** ✓ |
|  | mapepire (remote) | 207.03 ms | 212.16 ms | 212.26 ms |
| Pool sequential | mapepire (local) | **1.30 ms** ✓ | **1.18 ms** ✓ | **1.11 ms** ✓ |
|  | mapepire (remote) | 100.08 ms | 100.95 ms | 106.85 ms |
| Parameterized sequential | mapepire (local) | **1.41 ms** ✓ | **1.28 ms** ✓ | **1.28 ms** ✓ |
|  | mapepire (remote) | 99.61 ms | 100.47 ms | 101.90 ms |

(✓ marks the lowest value per column within each sub-scenario.)

#### Wall Clock (Promise.all batch end-to-end)

| Sub-scenario | Backend (location) | 50q | 200q | 1000q |
|---|---|---|---|---|
| Single Promise.all | mapepire (local) | **32.71 ms** ✓ | **116.49 ms** ✓ | **669.59 ms** ✓ |
|  | mapepire (remote) | 220.56 ms | 334.12 ms | 696.01 ms |
| Pool Promise.all | mapepire (local) | **28.02 ms** ✓ | **84.70 ms** ✓ | **429.52 ms** ✓ |
|  | mapepire (remote) | 220.82 ms | 269.90 ms | 566.21 ms |

(✓ marks the lowest value per column within each sub-scenario.)

### 4.5 Scenario 5 — native drivers

Native drivers used directly, no rm-connector-js wrapper. Pool config as in [3f](#3f-benchmark-scenarios): native idb Pool — sequential uses `DBPool` with `incrementSize: 5`; native idb Pool — Promise.all uses 50 pre-allocated raw `Connection` objects round-robin (DBPool's attach() chokes under burst); native mapepire `Pool` is pre-sized fixed at 50. Data from `bench-runs.sh native` run on the IBM i (loopback) and from the workstation (remote). Each cell is the median across 3 runs at the given query count.

#### Per Query (median per-query time)

| Sub-scenario | Backend (location) | 50q | 200q | 1000q |
|---|---|---|---|---|
| Single sequential | idb (local) | **0.56 ms** ✓ | **0.55 ms** ✓ | **0.53 ms** ✓ |
|  | mapepire (local) | 1.35 ms | 1.22 ms | 1.11 ms |
|  | mapepire (remote) | 98.11 ms | 102.70 ms | 102.85 ms |
| Single sequential (large) | idb (local) | 23.25 ms | 23.05 ms | 23.01 ms |
|  | mapepire (local) | **19.30 ms** ✓ | **13.77 ms** ✓ | **13.57 ms** ✓ |
|  | mapepire (remote) | 216.21 ms | 208.10 ms | 211.98 ms |
| Pool sequential | idb (local) | **0.55 ms** ✓ | **0.53 ms** ✓ | **0.54 ms** ✓ |
|  | mapepire (local) | 1.31 ms | 1.12 ms | 1.07 ms |
|  | mapepire (remote) | 99.67 ms | 95.58 ms | 104.28 ms |
| Parameterized sequential | idb (local) | **0.42 ms** ✓ | **0.42 ms** ✓ | **0.48 ms** ✓ |
|  | mapepire (local) | 1.38 ms | 1.24 ms | 1.26 ms |
|  | mapepire (remote) | 96.32 ms | 99.86 ms | 100.04 ms |

(✓ marks the lowest value per column within each sub-scenario.)

#### Wall Clock (Promise.all batch end-to-end)

| Sub-scenario | Backend (location) | 50q | 200q | 1000q |
|---|---|---|---|---|
| Single Promise.all | idb (local) | 40.36 ms | 116.09 ms | **581.05 ms** ✓ |
|  | mapepire (local) | **29.17 ms** ✓ | **108.46 ms** ✓ | 639.73 ms |
|  | mapepire (remote) | 216.87 ms | 322.28 ms | 647.13 ms |
| Pool Promise.all | idb (local) | **14.99 ms** ✓ | **63.99 ms** ✓ | **341.19 ms** ✓ |
|  | mapepire (local) | 338.83 ms | 358.18 ms | 1479.86 ms |
|  | mapepire (remote) | 491.67 ms | 437.30 ms | 1164.31 ms |

(✓ marks the lowest value per column within each sub-scenario.)

[↑ Back to contents](#contents)

## 5. Conclusion

The data in [Section 4](#4-benchmark-results) supports a few headline conclusions, organised below by the dimension being compared. All numbers in this section are taken from the median-of-three loopback or remote runs at the indicated query count.

### 5a. rm-connector-js vs native drivers (wrapper overhead)

Comparing Scenario 1 (rm-connector-js defaults) against Scenario 5 (native drivers, no wrapper) at the same workload shape gives the wrapper overhead:

| Sub-test (q=1000) | Native | rm-connector-js | Δ per query |
|---|---|---|---|
| Single sequential idb local | 0.53 ms | 0.59 ms | +0.06 ms (within noise) |
| Single sequential mapepire local | 1.11 ms | 1.12 ms | within noise |
| Pool sequential idb local | 0.54 ms | 0.78 ms | **+0.24 ms** (attach + `VALUES 1` + detach) |
| Pool sequential mapepire local | 1.07 ms | 1.84 ms | **+0.77 ms** (`VALUES 1` over WebSocket) |
| Pool sequential mapepire remote | 104.28 ms | 210.74 ms | **+106.5 ms** (full RTT for `VALUES 1`) |
| Connection creation idb local | 3.80 ms | 7.16 ms | **+3.36 ms** (one-time per connection) |

The wrapper overhead on the **single-connection** paths is essentially zero — `RmConnection` is a thin pass-through to the underlying driver. The Pool path adds a real per-query cost that comes almost entirely from the per-attach health check (`VALUES 1` + the round-trip it takes). On loopback that's tens of microseconds for idb and ~0.8 ms for mapepire; on a remote network it's a full network RTT per query (~100 ms in our test environment).

For Pool Promise.all under burst, direct rm-vs-native comparisons confound two effects: the wrapper code path *and* the effective pool size. The rm scenarios (1–4) keep the pool near 5 connections under `onAttach: true` (the rate-limiter prevents growth), while the native baseline (Sc 5) pre-allocates 50. The Pool Promise.all wall clocks reflect both differences combined. The cleanest wrapper-overhead numbers are the per-query sequential rows in the table above; the Pool Promise.all wall-clock differences are best understood through the lens of dispatch strategy and pool size — see [5e](#5e-multiplexing).

In exchange for this overhead, `RmPool` provides automatic connection retirement, structured logging, EventEmitter hooks, a unified API across idb and mapepire, and the opt-in multiplex mode. For most production workloads the per-query overhead is negligible relative to application logic.

### 5b. idb vs mapepire

On the IBM i (loopback), the two backends have measurably different performance characteristics rooted in their architectures (see [Section 2](#2-architecture-overview)). idb's data path stays in shared memory (no network layer, no JSON serialisation, no Java server, 1 memory copy from CLI buffer to JS object). Mapepire's data path goes through a Java server, JSON-over-WebSocket, JDBC, and ~4–5 memory copies per result.

**Where idb wins** (loopback, all of Sc 5):

- Small-result sequential queries — ~2× faster (1.11 ms mapepire vs 0.53 ms idb at q=1000)
- Pool sequential — ~2× faster (1.07 ms vs 0.54 ms)
- Parameterised sequential — ~2.5× faster (1.26 ms vs 0.48 ms). The wider gap here vs Single sequential's ~2× reflects the test using a smaller result set (`QIWS.QCUSTCDT WHERE STATE = 'TX'` returns just a few rows, vs the full `SAMPLE.DEPARTMENT` table) rather than any parameterisation-specific advantage. Per-query medians for the same workload across rm scenarios 1–4 and native Sc 5 are within session noise (1.24–1.41 ms mapepire local, 0.42–0.48 ms idb local), confirming that parameter binding adds no measurable overhead on either backend.
- Pool Promise.all idb-via-RmPool is ~3× faster than mapepire-via-native-Pool (517 ms vs 1480 ms at q=1000). The comparison reflects each backend in its most natural code path; see [5e](#5e-multiplexing) for the dispatch-vs-pool-size analysis

**Where mapepire wins** (loopback):

- **Large result sets**: ~1.7× faster (13.57 ms vs 23.01 ms at q=1000). When DB execution and result-set transfer dominate over protocol overhead, JTOpen's JDBC bulk transfer beats CLI buffering. The ratio is consistent across query counts (~13 ms mapepire / ~23 ms idb at q=200/1000).
- **High-concurrency single-connection bursts**: Single Promise.all at q=50 / q=200, mapepire local edges idb (29 / 108 ms vs 40 / 116 ms). mapepire's native correlation-ID multiplexing on the `SQLJob` overlaps queries on the wire; idb has to serialise them through a single CLI handle. By q=1000 idb's faster per-query cost catches up.

**Mapepire is the only option remote**, since idb is a local-only driver.

### 5c. Local vs remote

Network RTT dominates everything on remote. Our test environment measures ~99 ms RTT median (workstation to IBM Cloud PowerVS over residential broadband). That number sets the floor on any per-query measurement that involves a single round-trip:

- Single sequential mapepire remote = 102 ms (≈ 1× RTT + driver work)
- Pool sequential mapepire remote in Sc 1 = 211 ms (≈ 2× RTT — `VALUES 1` health check + actual query)
- Pool sequential mapepire remote in Sc 2/3 = 100 ms (≈ 1× RTT — health check skipped)
- Single sequential (large) mapepire remote = 212 ms (1× RTT + significant transfer time for the bigger result set)
- Connection creation mapepire remote = 414 ms (full handshake — TCP + TLS + WebSocket upgrade + JDBC init, several round-trips)

Two practical implications:

1. **Sequential workloads on remote are RTT-bound.** No amount of wrapper or driver tuning can beat physics. Running 1000 sequential queries against a remote IBM i takes at least 1000 × (RTT + driver work). For our 99 ms RTT, that's ~100 seconds minimum.
2. **Concurrent workloads on remote can hide RTT.** Running 1000 concurrent queries via mapepire's multiplex protocol (one connection, queries pipelined on the wire) can complete in under a second — see [5e](#5e-multiplexing). The bottleneck shifts from RTT-per-query to "how fast can the server process the batch".

This is the dual-environment story rm-connector-js was built for: develop remotely with mapepire (either accepting sequential RTTs for simple queries or opting into multiplex for concurrent workloads), deploy locally on IBM i with idb for the lowest-overhead path.

### 5d. Connection creation

Connection creation cost varies by ~100× depending on backend and location — from ~4 ms (idb local) to ~410 ms (mapepire remote):

| Backend × location | Native | rm-connector-js |
|---|---|---|
| idb (local) | 3.80 ms | 7.16 ms |
| mapepire (local) | 29.93 ms | 26.03 ms |
| mapepire (remote) | 414.09 ms | 470.12 ms |

Three things to read out of these numbers:

1. **idb local is ~4 ms; mapepire remote is ~410 ms.** The gap is driven by what each path has to do at connection time. idb is in-process IPC to a pre-warmed `QSQSRVR` job — barely more than function-call overhead. Mapepire remote is TCP three-way handshake + TLS handshake + WebSocket upgrade + JDBC initialisation: at least four network round-trips of work before the first query can run.
2. **Wrapper overhead on connection setup is small.** About +3 ms for idb (creating the `RmConnection` object, setting up event listeners, configuring health-check timers) and within noise for mapepire (the wrapper's cost is dwarfed by JDBC init). For long-lived pools this cost amortises immediately.
3. **Connection creation cost mostly only matters at pool warm-up, recovery, and growth.** A pool of 5 created at app startup pays 5× the per-iteration time once and then never again — at most ~2 seconds of startup latency even on remote mapepire. Connection retirement (e.g. via `expiry`) pays the cost again on the replacement. The big exception is Scenario 2 under sustained burst on loopback, where the pool grows to dozens of connections and the cumulative growth cost dominates wall clock — see [5f](#5f-health-check-onattach-and-keepalive).

**The idb cold-start penalty.** The *first* idb connection in any Node.js process pays a one-off ~145–165 ms - believed to be the DB2 CLI loading or allocating buffer space. Subsequent connections in the same process don't pay this — they reuse the warm prestart pool. Visible in the per-iteration `Max` of the connection-creation test, not in the median. For long-running services this is invisible; for short-lived scripts or process restarts it's worth knowing about.

### 5e. Multiplexing

Multiplexing collapses the wall-clock time for concurrent workloads on mapepire because the wire protocol natively pipelines queries — many queries can be in flight on a single WebSocket simultaneously, with responses routed back by correlation ID.

**Sc 3 vs Sc 1 Pool Promise.all wall clock (mapepire only):**

| Location | q | Sc 1 (default) | **Sc 3 (multiplex)** | Speedup |
|---|---|---|---|---|
| Local | 50 | 92.38 ms | **27.70 ms** | 3.3× |
| Local | 1000 | 1294.65 ms | **434.60 ms** | 3.0× |
| Remote | 50 | 5686.95 ms | **224.56 ms** | **25×** |
| Remote | 1000 | 111075.92 ms | **543.71 ms** | **204×** |

The remote speedup is the headline finding. Without multiplex, every concurrent query in `RmPool` serialises through the attach mutex and pays a full RTT for `VALUES 1` plus another for the query itself. With multiplex, attach is round-robin across the pool's existing connections (no `VALUES 1`), `detach` is a no-op, and the queries pipeline on the wire.

**rm multiplex (5 connections) vs native pool (50 connections).** rm-connector-js's multiplex implementation does blind round-robin across pool members. Native mapepire's `Pool.getJob()` is two-stage: it picks the first idle job (status `ready`) and falls back to the busy job with the fewest in-flight queries when all are busy — a load-balanced strategy once the pool is saturated. Despite the more sophisticated dispatch and 10× more connections (50 vs 5), the Sc 5 Pool Promise.all mapepire numbers (~1480 ms loopback at q=1000) are noticeably worse than Sc 3's 435 ms for the same workload — and **even more striking when you account for pool size**. Sc 3 multiplex uses only 5 connections (multiplex doesn't grow the pool past `initialConnections.size`); Sc 5 native pre-sizes the pool at 50. **Five round-robin-multiplexed connections beat fifty native-pool connections by ~3× on Pool Promise.all wall clock.** The exact cause isn't fully explained by these benchmarks — possible contributors include per-connection overhead (50 WebSocket+TLS sessions vs 5), server-side Java thread or JDBC contention with more active connections, or differences in how queries pipeline on the wire. Whatever the mechanism, the empirical result is consistent: for mapepire pools serving concurrent workloads, **`multiplex: true` is meaningfully better than the native pool**.

Multiplexing offers no benefit on sequential workloads because there's no concurrency to overlap. Sc 3 sequential numbers match Sc 2's (both skip the `VALUES 1` round-trip) — the savings are real but credit goes to skipping the per-attach health check, not to multiplex.

### 5f. Health check (onAttach and keepalive)

The per-attach `VALUES 1` health check (`onAttach: true`, the default) does two jobs:

1. **It verifies the connection is alive** before handing it to the caller — catches stale connections (e.g. timed-out by an intermediary) and retires them.
2. **It implicitly rate-limits attach calls.** As documented in [3c](#3c-health-check), `RmPool` doesn't queue waiters; the `VALUES 1` await is what spaces successive attaches enough for earlier queries to detach.

Sc 2 vs Sc 1 isolates the health-check cost. For sequential workloads, the savings from `onAttach: false` are exactly the cost of the `VALUES 1` round-trip (~0.24 ms idb, ~0.77 ms mapepire local, ~100 ms mapepire remote — see [5a](#5a-rm-connector-js-vs-native-drivers-wrapper-overhead)). For Pool Promise.all the picture is different and surprising: turning `onAttach` off doesn't always help.

**Pool Promise.all wall clock comparison:**

| Location | q | Sc 1 (with onAttach) | Sc 2 (without onAttach) | Outcome |
|---|---|---|---|---|
| Local mapepire | 1000 | 1295 ms | 6152 ms | Sc 2 **slower** by 4.7× |
| Remote mapepire | 1000 | 111 076 ms | 22 104 ms | Sc 2 **faster** by 5× |

On loopback, removing the rate-limiter lets attach calls fire faster than queries can finish, so the pool grows aggressively (in increments of 5, with each growth round taking ~27 ms). The cumulative growth overhead dominates wall clock and outweighs the per-query `VALUES 1` saving. Tuning `incrementConnections.size` higher would reduce the number of growth rounds and likely flip this — the test deliberately leaves the default to surface the trade-off. **On remote, the picture flips because Sc 1's serialised RTTs dominate** — Sc 2's growth overhead is significant but bounded, while Sc 1 pays an RTT for every single attach.

`keepalive` (Sc 4 vs Sc 3) has **no measurable cost during sustained load** — the background timer only fires on idle connections, so a continuous burst never triggers it. Enabling `keepalive` for multiplex pools that need health-check coverage is therefore a free improvement.

**`onAttach` is forced off in multiplex mode**: every multiplex connection is shared and may have many in-flight queries, so running `VALUES 1` on every `pool.query()` call would defeat the point. For multiplex pools that need health-check coverage, configure `keepalive`.

### 5g. Preferred options — summary

The right defaults depend on **where you're running** and **what your workload looks like**:

| Deployment | Workload | Recommended config |
|---|---|---|
| **On IBM i, using idb (recommended)** | All workloads | **`backend: 'idb'`, defaults**. ~2× faster than mapepire on small queries; consistently fastest under burst. The per-attach health check (~0.24 ms) is well worth keeping for the rate-limiter behaviour and stale-connection protection. |
| **On IBM i, using mapepire** (e.g. for portability with off-IBM-i environments) | Sequential | **Defaults** — `onAttach: true`, no multiplex. |
| **On IBM i, using mapepire** | Concurrent | **`multiplex: true`**, optionally with `keepalive: 5` for long-lived pools. ~3× faster than the default on concurrent bursts; `keepalive` is free in this mode. |
| **Off IBM i (workstation → remote IBM i)** | Sequential | **Defaults work.** Each query pays one RTT (~100 ms in our environment). For long-lived pools, set `keepalive: 5` so connections survive intermediary timeouts. |
| **Off IBM i** | Concurrent / burst | **`multiplex: true` is the right default.** 24×–204× faster than the serialised default on remote. Combine with `keepalive: 5` for production. |
| **Off IBM i** | Large result sets | mapepire is the only option remote, and it handles bulk transfer well via JTOpen's JDBC path. No special config needed beyond the above. |

**Two notes on the trade-offs we don't recommend optimising for:**

- *Disabling `onAttach` to chase per-query speed*: the per-query saving is real but small (~0.24 ms idb, ~0.77 ms mapepire local), and removing the rate-limiter changes Pool Promise.all behaviour in non-obvious ways (see [5f](#5f-health-check-onattach-and-keepalive)). The default rate-limiter behaviour is designed to make the pool work safely under load. Stick with `onAttach: true` unless you have a specific reason and are testing the resulting Pool Promise.all behaviour.
- *Tuning `incrementConnections.size` higher to make Sc 2 faster*: would help that scenario but isn't relevant unless you've already opted out of `onAttach`. The default of 5 is conservative and rarely fires under normal traffic.

**The dual-environment story holds.** Develop locally on a workstation using mapepire (with `multiplex: true` for concurrent code paths), deploy on IBM i using idb. Both paths share the same `RmPool` API, so application code doesn't need to know which backend it's running against.

[↑ Back to contents](#contents)

## 6. References

- [IBM/nodejs-idb-connector (GitHub)](https://github.com/IBM/nodejs-idb-connector)
- [IBM/nodejs-idb-pconnector (GitHub)](https://github.com/IBM/nodejs-idb-pconnector)
- [Mapepire-IBMi/mapepire-server (GitHub)](https://github.com/Mapepire-IBMi/mapepire-server)
- [Mapepire documentation](https://mapepire-ibmi.github.io/)
- [Mapepire: A new IBM i database client (Liam)](https://github.com/worksofliam/blog/issues/68)
- [Mapepire: Node.js performance testing against ODBC (Liam)](https://github.com/worksofliam/blog/issues/69)
- [IBM Introduces Mapepire (IT Jungle)](https://www.itjungle.com/2024/09/09/ibm-introduces-mapepire-the-new-db2-for-i-client/)

[↑ Back to contents](#contents)

## Appendix A: Pool growth under burst — `DBPool` vs `RmPool`

This appendix expands on why the pool configurations differ across scenarios (noted in [Section 3f](#3f-benchmark-scenarios)): specifically, why the native idb baseline (Scenario 5) had to use 50 pre-allocated raw `Connection` objects for Pool — Promise.all instead of `DBPool` directly. The reason is a structural difference in how the two pools grow under burst — and the difference is interesting enough to be worth understanding rather than just paving over.

This isn't a knock on `DBPool`. It just doesn't survive a 1000-query Promise.all the way `RmPool` does.

### The setup

A connection pool serves database queries. When all connections are busy and a new query arrives, the pool either makes the caller wait, creates more connections, or fails. Both `DBPool` and `RmPool` choose to *create more connections on demand*. The difference is *how* they create them.

Imagine 1000 queries arrive at the pool at the same instant, and the pool starts with 5 connections.

### `DBPool`'s approach: synchronous growth, no upper bound

When a query reaches `DBPool.attach()` and no connection is free, `DBPool` **synchronously** creates 5 new connections (the `incrementSize`) before returning. While it's doing that, **JavaScript can't do anything else** — the event loop is blocked. The next query waiting in the queue can't run, queries that already have connections can't deliver their results, nothing finishes.

Source-level facts (from [`dbPool.js`](https://github.com/IBM/nodejs-idb-pconnector/blob/master/lib/dbPool.js) and [`dbPoolConnection.js`](https://github.com/IBM/nodejs-idb-pconnector/blob/master/lib/dbPoolConnection.js)):

- `DBPool.attach()` is declared as `attach()` (not `async attach()`) and returns the connection directly — no `await`, no Promise wrapping.
- The growth loop calls `this.createConnection(...)` `incrementSize` times sequentially.
- `createConnection()` instantiates `new DBPoolConnection(...)`, which synchronously calls `this.connection.connect(url)` in its constructor.
- There's no `maxSize` option — growth is unbounded.

For 1000 concurrent queries:

- The first 5 grab existing connections.
- Every ~5 subsequent attaches triggers a growth round of 5 new connections.
- Each connection takes ~4 ms via the synchronous `connect()` call (Scenario 5 measured median: 3.80 ms).
- 199 growth rounds × 5 connections × ~4 ms ≈ **~4 seconds of synchronous event-loop blocking**.
- During that 4 seconds, no queries can deliver results — earlier queries are stuck waiting for the JavaScript engine to come back to them.

After those 4 seconds of blocking, the pool now has ~1000 active connections, all marked busy. Each is a separate `QSQSRVR` job on the IBM i. On a 4 GB LPAR, 1000 active jobs is a large load, and the actual database phase takes many tens of seconds more.

Observed wall clock at q=1000 with `DBPool`: **~56 seconds** (per-query median ~25 seconds).

### `RmPool`'s approach: asynchronous growth, with a cap

When a query reaches `RmPool._attach()` and no connection is free, `RmPool` says "I'll create 5 new connections — **asynchronously**, and while I'm doing it, **other things keep happening**". Specifically the growth path does:

```typescript
await Promise.all([5 createConnection promises]);
```

While that `await` is in flight, the event loop yields. Other callbacks run. Earlier queries can complete, call `detach()`, and free their connections back to the pool. By the time the growth round finishes and the pool re-scans for an available connection, several earlier queries are likely already done — so the pool finds existing freed-up connections without needing to grow again.

Two extra protections:

- An **attach mutex** ensures only one growth round is ever in flight at any moment.
- A **`maxSize` cap** (1000 in our tests) bounds the worst case absolutely.

**How the mutex makes growth incremental.** `RmPool.attach()` queues each `_attach()` invocation onto a promise chain so they run end-to-end rather than in parallel:

```typescript
attach(): Promise<RmPoolConnection> {
  const result = this.attachQueue.then(() => this._attach());
  this.attachQueue = result.catch(() => {});
  return result;
}
```

So 1000 concurrent `attach()` calls build a 1000-step chain — only one `_attach()` runs at a time. When the running `_attach()` hits its `await Promise.all([...])` for a growth round, the chain is blocked at that step until the round completes. Crucially, **during those ~20 ms of growth the JavaScript event loop is free**: queries that already have connections can finish their `exec()` and call `detach()`, returning those connections to the pool. By the time `_attach()` re-scans, it often finds existing freed-up connections alongside the newly-created ones.

Tracing a 1000-query burst:

- Calls 1–5: scan, grab existing connections, return. No growth.
- Call 6: triggers growth. `await Promise.all` for ~20 ms. **During those 20 ms, some of calls 1–5's queries finish their `exec()` and detach.** Call 6 sees several connections available after the await — picks one, returns.
- Call 7: only *starts* after call 6 resolves. By now even more queries have finished and detached. Call 7 likely finds an existing connection without triggering another growth round.
- Calls 8–N: same pattern. Each one waits its turn; by the time it runs, more queries have detached.

The mutex doesn't prevent growth — it **serializes** growth, giving each growth round a cooling-off period during which existing queries can release connections. Without it, every concurrent attach that found nothing would simultaneously trigger its own growth round, and you'd get many parallel `connect()` attempts in flight at once (the same shape of problem we saw with native mapepire's `Pool` overwhelming the server on a small LPAR).

For the same 1000-query workload, our Scenario 2 (`onAttach: false`, growable pool) saw the pool grow to roughly 100 active connections (a rough observation during the test, not a precise count) before queries started detaching at roughly the rate new attaches arrived — at which point further growth wasn't needed.

Observed wall clock at q=1000 with `RmPool` and `onAttach: false` (Scenario 2 idb local): **~4.2 seconds** — nearly all of which is the cumulative growth-round overhead, not raw query work.

### Why this matters for the benchmark

| At q=1000 idb local Pool — Promise.all | `DBPool` (sync growth) | `RmPool` (async growth) |
|---|---|---|
| Wall clock | ~56 seconds | ~4.2 seconds |
| Pool grows to | ~1000 connections | ~100 connections |
| Per-query median | ~25 seconds | ~4 ms |

The **~13× wall clock difference** isn't wrapper magic — it's the structural difference between synchronous and asynchronous growth. Both pools run on the same machine, the same database, with a similar per connection setup cost. How they choose to grow makes the difference.

For Scenario 5's native baseline, we work around `DBPool`'s burst behaviour by pre-allocating 50 raw `Connection` objects ahead of time. That measures the underlying driver's actual throughput floor at fixed concurrency 50, without `DBPool.attach()` obscuring the picture.

For typical production workloads — moderate concurrency, sequential queries through a long-lived pool — `DBPool` works fine. The different behaviour only kicks in when a large burst arrives faster than the pool can serve it. `RmPool`'s async growth is what makes it robust under exactly that scenario.

[↑ Back to contents](#contents)
