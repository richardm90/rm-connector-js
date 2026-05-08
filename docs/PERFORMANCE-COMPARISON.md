# Performance Comparison: idb-pconnector vs Mapepire

## Introduction

One of the primary motivations behind rm-connector-js is enabling a dual-environment development workflow: develop locally using Mapepire (which can connect to IBM i from various platforms) and deploy to production on IBM i using idb-pconnector for superior performance. This document examines the architectural and performance differences between the two database connectors to validate that assumption with technical evidence.

## Architecture Overview

### idb-pconnector

`idb-pconnector` is a Promise-based wrapper around `idb-connector`, the native C++ Node.js addon (N-API) that actually calls the DB2 SQL CLI API directly on IBM i. `idb-connector` is the piece doing the real work — the N-API bridge, the C buffers, the CLI calls into the QSQSRVR job. `idb-pconnector` re-exports its classes and methods, adding `Promise` semantics on top of the original callback-based API so modern `async`/`await` code can use it directly. rm-connector-js imports `idb-pconnector`, so everywhere this document says "idb-pconnector" the behaviour, performance characteristics, and data path are really those of the underlying `idb-connector` addon.

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

## Performance Differences

### Connection Establishment (~2.5-7x faster with idb on loopback)

Empirical measurements (POWER10 PowerVS, IBM i 7.5):

| Connector | Loopback (on IBM i) | Remote (workstation → IBM i) |
|-----------|---------------------|------------------------------|
| idb-pconnector | ~4-15 ms (warm) | N/A — local-only driver |
| Mapepire | ~25-35 ms | ~100-250 ms (TCP + TLS + WebSocket upgrade + JDBC init) |

The first idb connection in a session takes ~145-165 ms while the `QSQSRVR` prestart job activates; subsequent connections drop to the warm range above. Mapepire shows no such cold-start step — its connection time is dominated by handshake / upgrade overhead, which is consistent regardless of session age.

The difference on loopback is due to Mapepire still going through a (loopback-local) WebSocket upgrade and JDBC connection setup on the Java server side, whereas idb-pconnector performs a single in-process CLI connect call. Off-box (remote) the gap widens substantially because Mapepire has to traverse a real TCP + TLS handshake over the network — but in that scenario idb-pconnector isn't an option at all.

This matters for connection pool creation, recovery after idle connection expiry, and burst scenarios where new connections must be established quickly.

### Per-Query Overhead

Every Mapepire query involves the following steps that idb-pconnector avoids entirely:

1. JSON serialization of the request on the client
2. WebSocket framing and TLS encryption
3. TCP transmission (even on loopback, this involves kernel context switches)
4. Java-side JSON parsing
5. JDBC execution and result set processing
6. JSON serialization of results on the server
7. The reverse path back to the Node.js client

With idb-pconnector, the C++ addon reads DB2 CLI result buffers directly and copies them across the N-API boundary once. For high-frequency, low-latency queries this eliminates a substantial amount of overhead.

**Note:**
- JSON serialization means converting a JavaScript object into a JSON string so it can be sent over the wire. Then on the server side, the Java process has to parse that string back into an object. The same thing happens in reverse for the results — the Java server serializes the result set into a JSON string, and the Node.js client parses it back with `JSON.parse`. idb-pconnector doesn't need any of this because it's not sending data over a network protocol. The C++ addon calls the SQL CLI functions directly with the SQL string as a C-style parameter — it's a function call within the same process, not a message sent to a remote server. There's no need to package the request into a transmittable format and unpackage it on the other end.

### Memory and Data Copying

The number of times result data is copied differs significantly:

- **Mapepire**: JDBC ResultSet -> Java objects -> JSON string -> WebSocket frame -> JS `JSON.parse` -> JS objects (4-5 copies)
- **idb-pconnector**: CLI result buffer -> N-API copy -> JS objects (1 copy)

Fewer copies means less CPU and memory pressure, particularly for large result sets.

### Server Process Overhead

Mapepire requires a Java server process that consumes its own CPU and memory. idb-pconnector has no server component. This means one fewer process competing for system resources, one fewer garbage collector running, and one fewer layer of memory allocation for result sets.

### Concurrent Request Handling

Mapepire's WebSocket protocol supports **multiplexing** — an async, id-correlated model where multiple queries can be in-flight on a single connection without serializing. Each request is assigned a unique ID, sent immediately over the WebSocket, and responses are routed back to the correct caller by ID. Liam's benchmarks ([blog #69](https://github.com/worksofliam/blog/issues/69)) demonstrated that this gives Mapepire a significant advantage over ODBC-based connectors when connecting remotely.

idb-pconnector, by contrast, can only process one query at a time per connection. Concurrency requires multiple connections via a pool. There is one nuance worth flagging: at very high concurrency on a *single* connection (e.g. 1000 concurrent `Promise.all` queries on one `RmConnection`), mapepire's intra-`SQLJob` correlation multiplexing eventually overtakes idb's serialized CLI path — at 50 queries idb is ~1.6× faster, by 1000 queries mapepire edges ahead by ~1.1× (see [Wall Clock Times](#wall-clock-times-promiseall-scenarios) above). With a *pool* of connections idb still wins overall on Pool Promise.all throughput.

rm-connector-js's RmPool treats both backends as one-query-at-a-time by default (the lowest common denominator). For mapepire-backed pools, rm-connector-js exposes an opt-in `multiplex: true` flag that lets each pool connection serve unlimited concurrent in-flight queries via Mapepire's native ID-correlated WebSocket protocol, round-robin dispatched across pool members. See [Opt-in multiplex mode](#opt-in-multiplex-mode-mapepire-only) below.

Benchmark results (see below) show where each mode wins:

- **On IBM i (local loopback)**: idb-pconnector is still the fastest option for typical concurrent workloads because it has no protocol overhead at all. However, within the mapepire backend, `multiplex: true` is **2.8x-3.7x faster than the serialized default** and **2.0x-6.4x faster than the native mapepire pool** across 50/100/200/400/1000/2000-query scales (POWER10 PowerVS measurements).
- **Off IBM i (remote)**: Multiplexing provides a **24x-222x** speedup over the serialized default for concurrent workloads — the gain compounds with concurrency because serialized RmPool is RTT-bound (every query pays a full round-trip in series) while multiplex is dispatch-bound. Multiplex also consistently beats the native mapepire pool by 1.4-2.0x across all scales, thanks to round-robin dispatch. Sequential workloads still pay rm-connector-js's attach/detach overhead (~2x slower than native) regardless of the `multiplex` setting, because there is no concurrency for multiplexing to overlap.

## Summary

| Factor | idb-pconnector (on IBM i) | Mapepire (on IBM i, loopback) |
|--------|--------------------------|-------------------------------|
| Connection creation | ~4-15 ms warm (~145-165 ms first-in-session cold start) | ~25-35 ms loopback (~100-250 ms remote) |
| Per-query overhead | N-API boundary crossing only | JSON + WebSocket + TLS + Java + JDBC |
| Memory copies per result | 1 (CLI -> JS) | 4-5 (JDBC -> Java -> JSON -> WS -> JS) |
| Server process required | No | Yes (Java) |
| Concurrent query handling | Good with connection pool | Natively async per connection |
| Platform requirement | IBM i only | Various platforms |

## Benchmark Results

The following benchmarks were run on IBM i with both backends operating on the same machine. They fill a gap in the public record — prior benchmarks (such as Liam's [blog #69](https://github.com/worksofliam/blog/issues/69)) tested remote ODBC vs Mapepire from a Mac, not both connectors running locally on IBM i.

**Both backends are accessed through rm-connector-js (`RmConnection` for single-connection scenarios, `RmPool` for pool scenarios) — this is an apples-to-apples comparison through the same wrapper, isolating the underlying driver as the only variable.** For a baseline using the native drivers directly (without rm-connector-js), see [Native Driver Baseline](#native-driver-baseline-power10-powervs-ibm-i-75) below. For the existing cross-comparison against the native `@ibm/mapepire-js` `Pool` (with built-in multiplexing) on the same hardware as the rm-connector-js measurements, see [Native Mapepire Pool vs idb](#native-mapepire-pool-vs-idb-multiplexing-test).

### Test Environment

- **Platform**: IBM i 7.5 on IBM Cloud PowerVS LPAR (POWER10, processor feature `EDP2`)
- **Resources**: 2 CPUs, 4 GB RAM
- **Node.js**: Running directly on IBM i
- **Harness**: rm-connector-js `RmConnection` / `RmPool` for both backends (see note above)
  - **Mapepire backend**: `@ibm/mapepire-js` `SQLJob`, connecting via loopback (localhost)
  - **idb-pconnector backend**: `idb-pconnector` native local connection (`*LOCAL`)
- **Queries per scenario**: 50, 200, and 1000 (with 3 warm-up queries excluded from measurement)
- **Runs per scenario**: 3 (values below are median-of-medians across 3 runs)
- **Pool size**: 5 connections
- **Standard query**: `SELECT * FROM SAMPLE.DEPARTMENT`
- **Large result set query**: `SELECT * FROM SAMPLE.EMPLOYEE CROSS JOIN (VALUES 1,2,3,4,5,6,7,8,9,10) AS T(N)`

### What each scenario measures

The seven rows in the [Results](#results) table come from distinct test scenarios in [`tests/performance/backend-performance.test.ts`](../tests/performance/backend-performance.test.ts). Knowing what each one actually does makes the numbers easier to interpret.

- **Connection creation** — Constructs a fresh `RmConnection`, calls `init()` (which is what gets timed), then closes. Repeats 10 times in a loop. The cell value is the median per-iteration init time. For idb the *first* iteration in any session pays a `QSQSRVR` prestart-job cold start (~145–165 ms — visible in the max but not the median); subsequent iterations drop to single-digit ms. ([`backend-performance.test.ts:265`](../tests/performance/backend-performance.test.ts#L265))
- **Single sequential** — Opens one `RmConnection`, then runs N queries serially with `await conn.execute(SQL_STANDARD)` in a `for` loop. Each query is timed individually; the cell is the median single-query execution time on a warm connection. Closest representation of "the cost of one query in a long-running script". ([`:300`](../tests/performance/backend-performance.test.ts#L300))
- **Single sequential (large)** — Same shape as Single sequential but with the larger SQL (`SELECT * FROM SAMPLE.EMPLOYEE CROSS JOIN ... × 10`, ~420 rows). Shows how per-query cost shifts when DB execution and result-set transfer dominate over protocol overhead. ([`:321`](../tests/performance/backend-performance.test.ts#L321))
- **Single Promise.all** — Opens one `RmConnection`, fires N queries concurrently via `Promise.all(... conn.execute(...))`. Tests what happens when many in-flight queries share a single connection. The mapepire side correlates them on its `SQLJob`; the idb side serializes them through the CLI handle. ⚠️ **The per-query median for this row is misleading** — it includes time each query spent queued behind earlier ones on the same connection. Use the [Wall Clock Times](#wall-clock-times-promiseall-scenarios) table below for throughput. ([`:345`](../tests/performance/backend-performance.test.ts#L345))
- **Pool sequential** — Creates an `RmPool` of 5 connections, then runs N queries serially via `pool.query()` (each call grabs a connection, runs the query, releases it). Closest match to a typical production workload — every query incurs an attach + per-attach health check + detach on top of the SQL work. ([`:370`](../tests/performance/backend-performance.test.ts#L370))
- **Pool Promise.all** — Same `RmPool`, but fires N queries concurrently via `Promise.all`. With 5 connections and N concurrent calls, queries 6+ have to wait for connections to free up. RmPool has no explicit FIFO queue; it relies on the per-attach `VALUES 1` health check to slow each attach call enough for earlier queries to release their connections — this timing dependency is proven by the `pool-contention-proof` test suite. ⚠️ **Per-query median for this row is also misleading** for the same reason as Single Promise.all — use Wall Clock for throughput. ([`:395`](../tests/performance/backend-performance.test.ts#L395))
- **Parameterized sequential** — Opens one `RmConnection`, runs N parameterised queries serially (`SELECT * FROM QIWS.QCUSTCDT WHERE STATE = ?` with `['TX']`). Same shape as Single sequential but with a bound parameter; confirms whether the per-query overhead is sensitive to query shape (it isn't — protocol overhead dominates either way). ([`:420`](../tests/performance/backend-performance.test.ts#L420))

#### What the figures mean

Each cell in the [Results](#results) table is a **median per-query time in milliseconds**, computed in two stages:

1. Within one run of a scenario, the test executes N queries (where N is 50, 200, or 1000) and records each query's elapsed time. The median of those N times is the run's value.
2. Each (scenario, query-count) combination is run 3 independent times. The cell shows the **median of those 3 medians**.

Each cell in the [Wall Clock Times](#wall-clock-times-promiseall-scenarios) table is the **total elapsed time** in milliseconds to process the entire batch of N queries end-to-end — from before the first query is fired to after the last one resolves — again median of 3 runs. Wall clock is the right metric whenever you want to answer "how long does this batch take", and particularly for the Promise.all rows where per-query medians count queue-wait time on top of execution time.

### Results

All values are median query times in milliseconds, median across 3 independent runs per query count.

| Scenario | 50q idb | 50q mapepire | 200q idb | 200q mapepire | 1000q idb | 1000q mapepire | Stable Ratio |
|---|---|---|---|---|---|---|---|
| Connection creation | 9.58ms | 27.90ms | 9.27ms | 26.74ms | 9.81ms | 25.65ms | **idb ~2.7x faster** |
| Single sequential | 0.66ms | 1.50ms | 0.64ms | 1.29ms | 0.59ms | 1.15ms | **idb ~2x faster** |
| Single sequential (large) | 20.15ms | 7.61ms | 20.48ms | 6.90ms | 20.50ms | 7.09ms | **mapepire ~3x faster** (cache-warmed — see analysis) |
| Single Promise.all | 23.51ms | 30.06ms | 62.48ms | 85.91ms | 330.48ms | 481.47ms | **idb ~1.5x faster** |
| Pool sequential | 0.87ms | 2.46ms | 0.79ms | 1.96ms | 0.82ms | 1.87ms | **idb ~2.5x faster** |
| Pool Promise.all | 18.41ms | 55.05ms | 57.30ms | 158.83ms | 238.01ms | 692.73ms | **idb ~3x faster** |
| Parameterized sequential | 0.40ms | 1.32ms | 0.40ms | 1.22ms | 0.39ms | 1.26ms | **idb ~3x faster** |

### Wall Clock Times (Promise.all scenarios)

The wall clock measures how long it takes to process the entire batch of queries end-to-end. All values are in milliseconds, median across 3 runs.

| Scenario | 50q idb | 50q mapepire | 200q idb | 200q mapepire | 1000q idb | 1000q mapepire | Stable Ratio |
|---|---|---|---|---|---|---|---|
| Single Promise.all | 35.59ms | 57.59ms | 115.37ms | 136.65ms | 746.54ms | 666.38ms | varies — idb wins at low concurrency, mapepire's correlation multiplexing edges ahead at high concurrency |
| Pool Promise.all | 29.57ms | 99.56ms | 102.32ms | 295.73ms | 465.29ms | 1389.10ms | **idb ~3x faster** |

The pool Promise.all wall clock is the best throughput metric: it shows how quickly each backend can push N queries through 5 connections under maximum contention. At 1000 queries, idb completes the batch in under half a second while mapepire takes nearly 1.4 seconds.

The single Promise.all wall clock at 1000 queries is a finding worth flagging at this scale: a single mapepire `SQLJob` correlates concurrent in-flight queries via its native ID-tagged WebSocket protocol, whereas a single idb `Connection` can only execute one query at a time, so 1000 concurrent calls serialize through the CLI handle. At low concurrency idb still wins on raw per-query speed, but as concurrency grows mapepire's multiplexing closes the gap and at 1000q ends up slightly ahead.

### Analysis

- **idb-pconnector is consistently 2–3x faster for typical sequential workloads.** Pool sequential remains the best representation of a typical production workload (grab a connection, run a query, release): stable ~2.5x advantage from idb-pconnector's zero-network-overhead architecture.
- **Results are highly reproducible.** Across 3 independent runs at each query count, idb medians barely moved (e.g., pool sequential: 0.79–0.87ms across all runs). Mapepire was equally stable for sequential workloads, with one notable exception (next bullet).
- **Parameterized queries show a ~3x advantage**, confirming the overhead is in the protocol layer, not the query type. Per-query medians stay flat across 50/200/1000 (idb 0.39–0.40ms, mapepire 1.22–1.32ms) — the overhead is fixed per query.
- **Large result sets favour mapepire — but the ~3x magnitude in this run is cache-influenced.** The rm phase of the benchmark session ran *after* a complete native-driver phase against the same SAMPLE schema, and the buffer pool / mapepire prepared-statement cache had warmed substantially by that point. The native baseline at [Native Driver Baseline](#native-driver-baseline-power10-powervs-ibm-i-75) below shows the un-warmed behaviour (mapepire ~13ms, idb ~20ms — closer to a ~1.5x ratio). The architectural finding stands — JTOpen/JDBC handles bulk data transfer more efficiently than CLI buffering — but the "~3x" number here should be read as an upper bound under warm-cache conditions, not the steady-state ratio.
- **Single Promise.all wall clock crosses over with concurrency.** At 50q idb is 1.6x faster, at 200q 1.2x faster, at 1000q mapepire is 1.1x faster (746ms idb vs 666ms mapepire). A single mapepire `SQLJob` correlates concurrent in-flight queries via its ID-tagged WebSocket protocol, while a single idb `Connection` serializes them through the CLI handle. At low concurrency idb's faster per-query path wins; at high concurrency mapepire's multiplexing closes the gap.
- **Connection creation shows ~2.7x advantage on median** (idb ~9–10ms vs mapepire ~26–28ms). The "first idb connection ~145–165ms cold start" observation from the original data is still visible — the first iteration in any session activates a `QSQSRVR` prestart job before subsequent connections drop to single-digit milliseconds.
- **Mapepire shows larger outliers under sustained load** — same observation as before. Occasional 1000q outliers (mapepire single-sequential max at 50–60ms vs median ~1ms) are consistent with brief GC pauses or WebSocket congestion in the Java server.

### Wrapper Overhead vs Native Drivers

Comparing the rm-connector-js results in this section against the [Native Driver Baseline](#native-driver-baseline-power10-powervs-ibm-i-75) below (same hardware, same SAMPLE schema, same QUERY_COUNTs, same run methodology) gives a per-scenario wrapper-overhead measurement. All values below are at q=1000 — the most stable scale across 3 runs.

| Scenario | Metric | Native | rm-connector-js | Δ wrapper | Per-query overhead |
|---|---|---|---|---|---|
| Connection creation | median (10 iter) | 4.33ms | 9.81ms | +5.48ms | one-time cost per connection |
| Single sequential idb | median | 0.56ms | 0.59ms | +0.03ms | ~0 (within noise) |
| Single sequential mapepire | median | 1.09ms | 1.15ms | +0.06ms | ~0 (within noise) |
| Pool sequential idb | median | 0.56ms | 0.82ms | **+0.26ms / query** | attach/detach + per-attach health check |
| Pool sequential mapepire | median | 0.99ms | 1.87ms | **+0.88ms / query** | attach/detach + per-attach health check |
| Pool Promise.all idb (1000q) | wall | 292.89ms | 465.29ms | **+172.40ms / 1000q ≈ +0.17ms / query** | attach/detach + health checks under burst |
| Pool Promise.all mapepire (1000q) | wall | 1140.66ms | 1389.10ms | +248.44ms (+22%) | (mapepire side already dominated by first-ready dispatch quirk; attach overhead is a smaller fraction of the total) |
| Parameterized sequential idb | median | 0.45ms | 0.39ms | -0.06ms | ~0 (within noise) |
| Parameterized sequential mapepire | median | 1.48ms | 1.26ms | -0.22ms | ~0 (within noise) |

**Headline reading**: rm-connector-js's pool-management layer adds **~0.17–0.26 ms per query of overhead in typical idb workloads** (Pool sequential and Pool Promise.all rows). For most application-level latencies that's negligible — and in exchange the application gets pooled lifecycle, automatic connection retirement, health-checked attach, EventEmitter hooks, structured logging, and a unified API across both backends. Connection creation has a larger one-time wrapper cost (~5 ms) that amortises immediately over any reasonable workload.

The "negative wrapper overhead" rows (parameterised, single sequential mapepire/large) are within session-to-session noise, not wrapper magic — the wrapper cannot make queries faster than the underlying driver. The single sequential (large) mapepire row in particular skews strongly negative because the rm phase of the benchmark session ran *after* the native phase and the system caches had warmed; this is discussed in the "Large result sets" analysis bullet above.

### Native Driver Baseline (POWER10 PowerVS, IBM i 7.5)

The benchmarks above measure rm-connector-js with both backends going through the same wrapper. This section measures the **native drivers directly** — `idb-pconnector` and `@ibm/mapepire-js` — without any rm-connector-js code on the data path. The intent is to characterise raw driver throughput at the same row-by-row scenarios as the rm baseline so wrapper overhead can be measured by direct comparison. The test file is [`tests/performance/native-backend-performance.test.ts`](../tests/performance/native-backend-performance.test.ts), which mirrors the structure of `backend-performance.test.ts` row-for-row.

#### Test Environment

- **Platform**: IBM i 7.5 on IBM Cloud PowerVS LPAR (POWER10, processor feature `EDP2`). The rm-connector-js measurements in the section above were also re-run on this same hardware as part of the same benchmark session, so the two tables are directly comparable — the [Wrapper Overhead vs Native Drivers](#wrapper-overhead-vs-native-drivers) subsection above quantifies the per-scenario difference.
- **Resources**: 2 active CPUs (4 cap), 8 GB RAM cap (4 GB active)
- **Mapepire harness**: `@ibm/mapepire-js` `SQLJob` for single-connection scenarios, native `Pool` (with built-in multiplexing) for pool scenarios. Loopback (`localhost`).
- **idb-pconnector harness**: `idb-pconnector` `Connection({ url: '*LOCAL' })` for single-connection scenarios. For Pool scenarios, the test bypasses `DBPool` and constructs `POOL_SIZE` raw `Connection` objects directly, dispatching queries round-robin across them. This is necessary because `DBPool` has no `maxSize` option — under concurrent attach pressure it grows on demand, which produces wildly inflated wall clocks vs RmPool's bounded behaviour and makes the comparison meaningless. Five raw connections, held throughout the burst, gives a clean fixed-concurrency-5 baseline.
- **Queries per scenario**: 50, 200, and 1000 (3 warm-up queries excluded from measurement)
- **Runs per scenario**: 3 (values below are median-of-medians across the 3 runs at each scale)
- **Pool size**: 5 connections
- Same SAMPLE schema queries as the rm-connector-js benchmark above (`SELECT * FROM SAMPLE.DEPARTMENT` for the standard query, `SELECT * FROM SAMPLE.EMPLOYEE CROSS JOIN ...` for the large result set).

#### Results

All values are median query times in milliseconds, median across 3 independent runs per query count.

| Scenario | 50q idb | 50q mapepire | 200q idb | 200q mapepire | 1000q idb | 1000q mapepire | Stable Ratio |
|---|---|---|---|---|---|---|---|
| Connection creation | 4.24ms | 31.14ms | 4.30ms | 28.57ms | 4.33ms | 28.51ms | **idb ~6.5x faster** |
| Single sequential | 0.60ms | 1.50ms | 0.60ms | 1.17ms | 0.56ms | 1.09ms | **idb ~2x faster** |
| Single sequential (large) | 19.88ms | 13.22ms | 19.70ms | 12.86ms | 19.89ms | 12.60ms | **mapepire ~1.5x faster** |
| Single Promise.all | 17.58ms | 24.08ms | 50.36ms | 74.85ms | 267.13ms | 455.75ms | **idb ~1.5x faster** |
| Pool sequential | 0.58ms | 1.47ms | 0.55ms | 1.05ms | 0.56ms | 0.99ms | **idb ~2x faster** |
| Pool Promise.all | 1.39ms | 242.93ms | 1.45ms | 285.37ms | 1.45ms | 816.21ms | (per-query medians dominated by mapepire dispatch quirk — see analysis; use wall clock below) |
| Parameterized sequential | 0.40ms | 1.50ms | 0.38ms | 1.30ms | 0.45ms | 1.48ms | **idb ~3.5x faster** |

#### Wall Clock Times (Promise.all scenarios)

The wall clock measures how long it takes to process the entire batch of queries end-to-end. All values are in milliseconds, median across 3 runs.

| Scenario | 50q idb | 50q mapepire | 200q idb | 200q mapepire | 1000q idb | 1000q mapepire | Stable Ratio |
|---|---|---|---|---|---|---|---|
| Single Promise.all | 31.29ms | 29.50ms | 101.08ms | 109.02ms | 551.77ms | 638.05ms | ~equal |
| Pool Promise.all | 14.50ms | 250.10ms | 58.47ms | 323.18ms | 292.89ms | 1140.66ms | **idb 5–17x faster** |

#### Analysis

- **idb-pconnector is 2–3x faster than mapepire for typical small-result sequential workloads.** Pool sequential and parameterised query medians are stable across 50/200/1000 (e.g. Pool sequential idb 0.55–0.58ms, mapepire 0.99–1.47ms), confirming throughput is bound by protocol overhead rather than per-query setup costs.
- **Mapepire wins for large result sets by ~1.5x** (idb ~20ms median, mapepire ~13ms median, stable across all scales). When DB2 transfer time dominates, JTOpen's JDBC path moves a substantial result set more efficiently than CLI buffering. Same finding as the rm-connector-js benchmark above.
- **Pool Promise.all idb wall clock scales linearly with N** — 14.5 / 58.5 / 292.9ms → **~0.29ms per query effective throughput across 5 connections**. This is the throughput floor for native idb at fixed concurrency 5: five raw `Connection` objects, each running ~N/5 queries sequentially, all five chains in parallel.
- **Pool Promise.all mapepire wall clock scales worse than linearly** (250 / 323 / 1141ms — note the 3.5x jump between 200q and 1000q). Native `@ibm/mapepire-js` `Pool.getJob()` returns the first job whose status is `ready`, which on fast loopback funnels work to job 0 while jobs 1–4 stay underused. The per-query medians (243 / 285 / 816ms) make the queueing visible: most queries effectively serialize through the same job. This is the same dispatch quirk documented in [Three-way loopback](#three-way-loopback-rm-connector-js-serialized-vs-multiplex-vs-native-mapepire-pool); it is the reason rm-connector-js's opt-in [`multiplex: true`](#opt-in-multiplex-mode-mapepire-only) path uses round-robin dispatch instead.
- **Single Promise.all wall clock is essentially equal between backends** (~31ms idb vs ~30ms mapepire at 50q). With only one connection on each side and a single-digit-millisecond per-query cost, the wall clock is dominated by query execution time itself, not by protocol overhead. Per-query medians grow proportionally with N (17→50→267ms for idb; 24→74→456ms for mapepire) — that's the queue-wait artifact noted in the rm benchmark above and is why wall clock is the meaningful metric for Promise.all rows.
- **Connection creation idb shows a small first-session cold start.** Wall clock for 10 idb iterations was 63ms in the q=50 run versus ~46ms in the q=200/1000 runs, because the first idb connection in a session activates a `QSQSRVR` prestart job (the doc captures this elsewhere as a ~145–165ms penalty for the first connection). After that the prestart pool is warm and median per-iteration drops to ~4ms. Mapepire shows no such cold start; its connection times are dominated by WebSocket / TLS / JDBC handshake overhead, which is consistent regardless of session age.
- **Note on the Pool Promise.all "idb 175x faster" per-query median row above.** That ratio is misleading because mapepire's per-query median includes substantial queue-wait time on job 0 (the dispatch quirk). The wall clock comparison (5–17x faster) is the meaningful one — and that ratio is real and consistent across scales.

### Native Mapepire Pool vs idb (Multiplexing Test)

Mapepire's WebSocket protocol supports **multiplexing** — sending multiple queries concurrently on a single connection, with responses routed back by ID. This is fundamentally different from idb-pconnector, where each DB2 CLI connection can only process one query at a time. rm-connector-js's RmPool treats both backends as one-query-at-a-time by default (the lowest common denominator), so the standard benchmarks above do not take advantage of Mapepire's multiplexing.

To determine whether Mapepire's multiplexing could compensate for its higher per-query latency, a separate test was run comparing the **native @ibm/mapepire-js Pool** (with full multiplexing) against **idb-pconnector through RmPool** (one-at-a-time per connection). Both used 5 connections. All values below are wall-clock milliseconds averaged across 3 independent runs.

**Sequential (pool of 5, one query at a time):**

| Queries | idb Wall Clock | Mapepire (native) Wall Clock | Ratio |
|---|---|---|---|
| 50 | 77.34ms | 143.96ms | **idb 1.9x faster** |
| 200 | 315.47ms | 925.25ms | **idb 2.9x faster** |
| 1000 | 1582.37ms | 3739.61ms | **idb 2.4x faster** |

**Promise.all (all queries fired concurrently):**

| Queries | idb Wall Clock | Mapepire (native) Wall Clock | Ratio |
|---|---|---|---|
| 50 | 46.42ms | 294.30ms | **idb 6.3x faster** |
| 200 | 146.02ms | 320.69ms | **idb 2.2x faster** |
| 1000 | 787.63ms | 1505.35ms | **idb 1.9x faster** |

**High concurrency burst (QUERY_COUNT × 2 queries fired concurrently):**

| Queries | idb Wall Clock | Mapepire (native) Wall Clock | Ratio |
|---|---|---|---|
| 100 | 81.73ms | 282.57ms | **idb 3.5x faster** |
| 400 | 275.06ms | 690.23ms | **idb 2.5x faster** |
| 2000 | 1361.03ms | 4515.99ms | **idb 3.3x faster** |

**Native Mapepire multiplexing is slower than idb on loopback at every scale.** Even with 50 queries in-flight simultaneously across 5 WebSocket connections, native Mapepire took ~294ms vs idb's ~46ms — idb wins because it has no protocol overhead to begin with, and its 5 QSQSRVR jobs each process queries directly through shared memory. Notice the idb advantage is largest in the Promise.all 50-query scenario (6.3x) and narrows as concurrency grows — by the time both are processing 2000 queries through 5 pool connections, idb is "only" 3.3x faster because the mapepire-side protocol overhead is amortised across more work per round-trip.

The more interesting question is **why** native Mapepire performs so poorly here, and whether multiplexing itself is the problem or whether it's specific to native's implementation. The three-way benchmark below answers that.

### Three-way loopback: rm-connector-js serialized vs multiplex vs Native Mapepire Pool

To separate the effect of **multiplexing** from the effect of **dispatch strategy**, a three-way test runs serialized rm-connector-js, rm-connector-js with `multiplex: true`, and the native mapepire pool against the same server. All values below are wall-clock milliseconds, median across 3 independent runs on POWER10 PowerVS (IBM i 7.5), pool size 5.

**Promise.all (concurrent burst):**

| Queries | rm serialized | rm `multiplex: true` | native mapepire | mux vs serialized | mux vs native |
|---|---|---|---|---|---|
| 50 | 95.75ms | **31.51ms** | 200.57ms | **3.0x faster** | **6.4x faster** |
| 200 | 354.58ms | **101.82ms** | 298.46ms | **3.5x faster** | **2.9x faster** |
| 1000 | 1589.65ms | **503.38ms** | 985.97ms | **3.2x faster** | **2.0x faster** |

**High concurrency burst (QUERY_COUNT × 2 queries):**

| Queries | rm serialized | rm `multiplex: true` | native mapepire | mux vs serialized | mux vs native |
|---|---|---|---|---|---|
| 100 | 178.19ms | **47.85ms** | 241.45ms | **3.7x faster** | **5.0x faster** |
| 400 | 609.73ms | **163.53ms** | 396.40ms | **3.7x faster** | **2.4x faster** |
| 2000 | 2860.30ms | **1015.93ms** | 2765.95ms | **2.8x faster** | **2.7x faster** |

Two things fall out of this:

- **Multiplexing itself isn't the problem on loopback — dispatch strategy is.** Native mapepire's `Pool.getJob()` returns the first job whose status is `ready`, so when queries complete quickly (as they do on loopback), the dispatcher keeps handing work to the earliest job in the array while later jobs stay idle. rm-connector-js's multiplex path does blind round-robin (`i++ % N`), which guarantees an even fan-out across all five WebSocket connections. Round-robin multiplexing is **2.8x-3.7x faster than serialized access** and **2.0x-6.4x faster than native mapepire's multiplexing** across every scale tested.
- **idb still wins for typical (moderate) concurrency on loopback** — narrowly. At 50 concurrent queries, idb-RmPool completes in 29.6ms wall vs rm multiplex's 31.5ms (see Pool Promise.all in the rm baseline above) — within noise, but slightly favouring idb. At 1000 queries, idb-RmPool 465ms beats rm multiplex 503ms; at very high concurrency (2000q burst), rm multiplex 1016ms is comparable to idb-RmPool's ~930ms (linear projection from 1000q). idb remains the best loopback choice for moderate concurrency, but the gap closes as concurrency climbs.

This means the recommendation "use idb-pconnector on IBM i in production" is still correct for typical workloads, but the underlying reason is not "multiplexing is bad on loopback." It's "idb has no protocol overhead, so for moderate concurrency it wins regardless of what the mapepire side is doing." If you are running the mapepire backend on IBM i for some reason (e.g. platform-independent dev, or a deployment topology where idb is not available), **enable `multiplex: true`** — it is faster than the serialized default at every scale tested.

### Remote: rm-connector-js serialized vs multiplex vs Native Mapepire Pool

The local IBM i results above show that multiplexing on loopback yields a measurable but modest gain. The opposite scenario was tested **remotely from a development PC** connecting to a POWER10 PowerVS LPAR over a real network. Three modes were compared: rm-connector-js with its default serialized pool, rm-connector-js with the opt-in `multiplex: true` flag, and the native mapepire Pool (which multiplexes unconditionally). Six query scales, median across 3 independent runs at each scale, pool size 5.

**Promise.all (concurrent burst), wall clock:**

| Queries | rm serialized | rm `multiplex: true` | native mapepire | mux vs serialized | mux vs native |
|---|---|---|---|---|---|
| 50 | 5,363.19 ms | **221.81 ms** | 335.93 ms | **24.2x faster** | **1.5x faster** |
| 200 | 20,657.17 ms | **256.22 ms** | 375.79 ms | **80.6x faster** | **1.5x faster** |
| 1000 | 100,681.66 ms | **563.87 ms** | 818.39 ms | **178.6x faster** | **1.5x faster** |

**High concurrency burst (QUERY_COUNT × 2 queries), wall clock:**

| Queries | rm serialized | rm `multiplex: true` | native mapepire | mux vs serialized | mux vs native |
|---|---|---|---|---|---|
| 100 | 11,037.96 ms | **256.83 ms** | 352.98 ms | **43.0x faster** | **1.4x faster** |
| 400 | 40,090.40 ms | **329.45 ms** | 524.20 ms | **121.7x faster** | **1.6x faster** |
| 2000 | 204,852.22 ms | **922.30 ms** | 1,745.62 ms | **222.1x faster** | **1.9x faster** |

The `mux vs serialized` ratio scales aggressively with N because serialized is RTT-bound — every query pays one full network round-trip in series. Multiplex is dispatch-bound rather than RTT-bound, so its wall clock grows much more slowly. The `mux vs native` ratio stays in the 1.4–2.0× band across every scale; native mapepire's `Pool.getJob()` first-ready bias still costs measurably even when network latency dominates the wall clock.

**Sequential (50 queries, pool of 5), wall clock:**

| Mode | Wall Clock | Ratio |
|---|---|---|
| rm-connector-js (default / no multiplex) | 9796.23ms | — |
| Native mapepire pool | 4794.07ms | **native 2.0x faster** |

Sequential is not a multiplexing workload — queries run one after another with no concurrency to hide latency behind — so `multiplex: true` makes no difference here and the benchmark omits it. Native mapepire remains ~2x faster because rm-connector-js's attach/health-check/detach sequence is paid on every query, and each round-trip over the network amplifies that overhead.

**Why multiplex rm-connector-js consistently beats native for concurrent bursts:** native mapepire's `Pool.getJob()` returns the first job whose status is `ready`, so the dispatcher is biased toward the earliest job in the array. rm-connector-js's multiplex path does blind round-robin (`i++ % N`), which guarantees an even fan-out across all five WebSocket connections immediately. The advantage stays at 1.4–2.0× across every remote scale measured. The evenness becomes most valuable at the very high end (2.0× at 2000 concurrent queries) because the first-ready dispatcher's idle-job problem compounds with the larger work set.

This reveals a clear trade-off in rm-connector-js's design:

- **On IBM i (production)**: The serialized pool model is optimal — idb-pconnector wins regardless, and serialized access is actually faster than multiplexing on local loopback.
- **Off IBM i (development / remote)**: Enable `multiplex: true` for concurrent workloads. Sequential workloads still pay the ~2x attach/detach overhead against native, but concurrent bursts match or slightly beat native without users having to drop down to the native API. See the [Opt-in multiplex mode](#opt-in-multiplex-mode-mapepire-only) section below.

### Opt-in multiplex mode (mapepire only)

By default RmPool serializes one query at a time per connection, which is the right call on local IBM i (see above). For remote workstation-to-IBM-i workloads where multiplexing pays off, you can opt in by setting `multiplex: true` on the pool config (mapepire backend only — idb is rejected at construction).

```ts
const pool = new RmPool({
  id: 'remote',
  config: {
    id: 'remote',
    PoolOptions: {
      backend: 'mapepire',
      creds: { host, user, password },
      maxSize: 5,
      initialConnections: { size: 5 },
      multiplex: true,
    },
  },
});
```

When `multiplex: true`:

- Each `RmPoolConnection` is **shared**: multiple callers can hold it at the same time, and concurrent `pool.query()` calls map directly to mapepire-js's parallel `job.execute()` calls on the same `SQLJob`.
- `attach()` round-robins across pool members rather than claiming exclusive ownership; `detach()` is a no-op. The promise-chain mutex stays in place (its cost is negligible in this path) but it no longer gates concurrency.
- Per-attach health checks are skipped (they would defeat the point). Use `healthCheck.keepalive` for periodic background checks instead.
- `connection.expiry` still applies, interpreted as max age from creation. If the timer fires while `inFlight > 0`, retirement is deferred until in-flight queries finish; the pool then auto-creates a replacement so subsequent attaches still find a connection.
- `getInfo()` exposes a new `inFlight` counter (and `multiplex: true`) for visibility, since `available`/`busy` no longer carry their usual meaning.

**When to use it:** any mapepire workload with concurrent queries (Promise.all, burst patterns).

- **Remote (workstation-to-IBM-i over a network):** 24x-222x faster than serialized across 50–2000 query scales — the speedup compounds with concurrency because serialized is RTT-bound. Consistently beats the native mapepire Pool by 1.4–2.0x at every scale (POWER10 PowerVS measurements).
- **Local (on IBM i loopback):** 2.8x-3.7x faster than serialized mapepire, and 2.0x-6.4x faster than the native mapepire Pool across 50/100/200/400/1000/2000-query scales (POWER10 PowerVS). Round-robin dispatch wins over native's first-ready-job bias even without network latency. Note this is still within the *mapepire backend* — if you are on IBM i, idb-pconnector remains the fastest option for moderate concurrency regardless.

**When not to use it:** purely sequential workloads (there is no concurrency to hide latency behind — multiplex provides no benefit for sequential traffic, and the serialized default is slightly simpler to reason about operationally).

A three-way benchmark (`tests/performance/remote-mapepire-multiplex.test.ts`) compares serialized RmPool, multiplex RmPool, and the native mapepire pool on the same host so you can verify the gain on your network before adopting it in production.

### Reproducing These Benchmarks

The benchmark suite is included in the rm-connector-js test suite. All tests require environment variables: `IBMI_HOST`, `IBMI_USER`, `IBMI_PASSWORD`.

1. Ensure the SAMPLE schema exists on your IBM i:
   ```sql
   CALL QSYS.CREATE_SQL_SAMPLE('SAMPLE');
   ```

2. Run all performance tests (idb vs mapepire through rm-connector-js):
   ```bash
   IBMI_HOST=myibmi.com IBMI_USER=MYUSER IBMI_PASSWORD=MYPASS npm run test:performance

   # or ...
   export IBMI_HOST=myibmi.com
   export IBMI_USER=MYUSER
   export IBMI_PASSWORD=MYPASS
   npm run test:performance
   ```

3. Optionally configure the number of queries per scenario (default: 50) and the SAMPLE schema name (default: SAMPLE):
   ```bash
   QUERY_COUNT=200 SAMPLE_SCHEMA=MYLIB IBMI_HOST=myibmi.com IBMI_USER=MYUSER IBMI_PASSWORD=MYPASS npm run test:performance
   ```

4. Run individual test suites separately:
   ```bash
   # idb vs mapepire through rm-connector-js (on IBM i)
   npx jest --config jest.perf.config.js backend-performance

   # Native mapepire pool vs idb through RmPool (on IBM i)
   npx jest --config jest.perf.config.js native-mapepire-pool

   # Native mapepire pool vs rm-connector-js mapepire pool (from remote dev PC)
   npx jest --config jest.perf.config.js remote-mapepire-pool

   # Three-way: rm-connector-js serialized vs multiplex vs native (from remote dev PC)
   npx jest --config jest.perf.config.js remote-mapepire-multiplex

   # Pool contention proof (on IBM i)
   npx jest --config jest.perf.config.js pool-contention-proof
   ```

## Conclusion

The performance benefits of idb-pconnector over Mapepire when running on IBM i are real and stem from fundamental architectural differences: no network layer, no serialization overhead, no intermediary server process, and fewer data copies. These are not micro-optimizations that could disappear with a library update; they are inherent to the design of each connector.

Key findings:

- **On IBM i, idb-pconnector is 2-3x faster** for typical sequential workloads and up to ~6x faster under concurrent load, even when compared against Mapepire's native multiplexing capabilities.
- **Native Mapepire's multiplexing underperforms on loopback** — not because multiplexing is a bad idea there, but because native's `Pool.getJob()` biases dispatch toward the first ready job and leaves the rest underused when queries complete quickly. rm-connector-js's `multiplex: true` uses round-robin dispatch instead, which is 2.0x-6.4x faster than native mapepire and 2.8x-3.7x faster than rm-connector-js's own serialized default on loopback across 50/100/200/400/1000/2000-query scales (POWER10 PowerVS).
- **Over a real network, multiplexing is transformative** — rm-connector-js's default serialized pool is 24x slower than multiplexing at 50 concurrent queries and **over 200x slower at 2000 concurrent queries** because each query pays the full round-trip latency sequentially while multiplex stays dispatch-bound.
- **rm-connector-js offers opt-in multiplexing as a universal win for concurrent mapepire workloads.** Set `multiplex: true` and the mapepire backend matches or slightly beats the native mapepire Pool in every concurrent scenario we measured, both local and remote. Sequential workloads are unchanged — native remains ~2x faster there due to rm-connector-js's per-query attach/detach overhead.
- **The right defaults depend on which backend, not where you run.** On IBM i, idb-pconnector is still the best choice for typical concurrent workloads — it has no protocol overhead at all, and wins regardless of what happens on the mapepire side. Off IBM i, use the mapepire backend with `multiplex: true` for concurrent workloads, or leave `multiplex` off for purely sequential ones.

The rm-connector-js approach of using Mapepire for off-IBM i development and idb-pconnector for on-IBM i production combines the convenience of cross-platform development with the performance benefits of native database access where it matters most — and the opt-in multiplex mode closes the remaining gap against native mapepire for concurrent workloads over a network.

## References

- [IBM/nodejs-idb-connector (GitHub)](https://github.com/IBM/nodejs-idb-connector)
- [IBM/nodejs-idb-pconnector (GitHub)](https://github.com/IBM/nodejs-idb-pconnector)
- [Mapepire-IBMi/mapepire-server (GitHub)](https://github.com/Mapepire-IBMi/mapepire-server)
- [Mapepire documentation](https://mapepire-ibmi.github.io/)
- [Mapepire: A new IBM i database client (Liam)](https://github.com/worksofliam/blog/issues/68)
- [Mapepire: Node.js performance testing against ODBC (Liam)](https://github.com/worksofliam/blog/issues/69)
- [IBM Introduces Mapepire (IT Jungle)](https://www.itjungle.com/2024/09/09/ibm-introduces-mapepire-the-new-db2-for-i-client/)
