# Backend Differences: idb-pconnector vs mapepire

This document details the differences between the two backends based on testing all examples on IBM i with both `backend: 'idb'` and `backend: 'mapepire'`.

## Connection & Infrastructure

| Item | idb | mapepire |
|---|---|---|
| **Job subsystem** | `QSQSRVR` | `QZDASOINIT` |
| **Connection protocol** | Native DB2 CLI (in-process, links against IBM i Db2 Call Level Interface) | WebSocket (remote) |
| **Connection creation speed** | ~4-15 ms warm; ~145-165 ms for the first connection in a session (`QSQSRVR` prestart activation) | ~25-35 ms on loopback; ~100-250 ms remote (TCP + TLS + WebSocket upgrade + JDBC init) |
| **Connected log message** | `Connected (idb-pconnector)` | `Connected (mapepire-js)` |

## Result Envelope

See [RESULT-ENVELOPES.md](RESULT-ENVELOPES.md) for full JSON examples of
the envelope returned by each backend for a query covering every major
SQL data type.

| Property | idb | mapepire |
|---|---|---|
| **`id`** | Sequential number (`"3"`, `"5"`, etc.) | Named identifier (`"query7"`, `"query26"`, etc.) |
| **`metadata`** | `null` | Object with `column_count`, `job`, `columns` array |
| **`update_count`** | `0` | `-1` (except stored procedures with output params which return `0` on both) |
| **`sql_rc`** | `0` (present) | absent |
| **`sql_state`** | `'00000'` (present) | absent |
| **`execution_time`** | High-res float in ms (`3.93`, `218.32`) | Integer milliseconds (`7`, `244`) |
| **`parameter_count`** | absent | Present on parameterized queries |
| **Property order** | `success` first | `id` first |

## Output Parameters (stored procedures)

| Property | idb | mapepire |
|---|---|---|
| **`output_parms` presence** | Only when `execute()` returns non-null | Always present for parameterized CALL |
| **`output_parms` entries** | All parameters (IN + OUT + INOUT) | All parameters (IN + OUT + INOUT) |
| **`index`** | Present | Present |
| **`value`** | Present on all parameters (input and output) | Present only on OUT/INOUT parameters (absent on IN) |
| **`type`** | absent | Present (`'VARCHAR'`, etc.) |
| **`name`** | absent | Present (`'P_NAME'`, etc.) |
| **`precision`/`scale`/`ccsid`** | absent | Present |
| **NULL OUT parameter** | Returns zero value (`""` for string/date/CLOB, `0` for numeric) — idb-pconnector limitation | Returns `null` |

## Error Messages

| Part | idb | mapepire |
|---|---|---|
| **Format** | `SQLSTATE=42601 SQLCODE=-104 <message>` | `[SQL0104] <message>, 42601, -104` |
| **SQLSTATE/SQLCODE position** | Prefixed before message | Suffixed after message |

## Data

| Item | idb | mapepire |
|---|---|---|
| **Query data** | Identical | Identical |
| **String trimming** | `trimEnd()` (trailing only) | Trimmed by mapepire server |
| **Numeric conversion** | Via `enableNumericTypeConversion(true)` | Native from mapepire |
| **Column names** | Identical | Identical |
| **Numeric values** | Identical (except BIGINT, DOUBLE) | Identical (except BIGINT, DOUBLE) |
| **BIGINT type** | Returned as `string` | Returned as `number` |
| **DOUBLE precision** | Truncated to ~6 significant digits | Full double precision (~15 digits) |
| **NULL CLOB** | Returned as empty string `""` | Returned as `null` |
| **BINARY/VARBINARY/BLOB reads** | Returned as Node `Buffer` (native from idb-pconnector) | Returned as Node `Buffer` (wrapper decodes the hex string mapepire-js returns) |
| **Binary parameters (writes)** | Node `Buffer` passed straight to `Statement.bindParameters` | Node `Buffer` converted to lowercase hex string by the wrapper before being sent to mapepire-js |
| **BOOLEAN** | Returned as strings (`"TRUE"`, `"FALSE"`) — pending [idb-connector#191](https://github.com/IBM/nodejs-idb-connector/pull/191) for buffer fix | Returned as native `true`/`false` |
| **DECFLOAT** | Returned as `string` (preserves full precision) | Returned as `number` (truncated to JS double precision) |
| **DATE format** | `YYYY-MM-DD` (ISO default forced) | `YYYY-MM-DD` (ISO default forced) |
| **TIME format** | `HH.MM.SS` (DB2 ISO default forced — dots, not colons) | `HH.MM.SS` (DB2 ISO default forced — dots, not colons) |
| **TIMESTAMP format** | `YYYY-MM-DD-HH.MM.SS.NNNNNN` (DB2 native) | `YYYY-MM-DD-HH.MM.SS.NNNNNN` (normalised from JT400 format to DB2 native) |

## Features

| Feature | idb | mapepire |
|---|---|---|
| **Keepalive** | Automatically disabled (no WebSocket) | Supported (ping via `VALUES 1`) |
| **JDBCOptions `libraries`** | SQL naming: `SET SCHEMA` (first lib only); system naming: `setLibraryList()` (all libs) | Native JDBC support (same behaviour) |
| **JDBCOptions `naming`** | Mapped to `setConnAttr(SQL_ATTR_DBC_SYS_NAMING)` | Native JDBC support |
| **JDBCOptions `transaction isolation`** | Mapped to `setConnAttr(SQL_ATTR_COMMIT)` | Native JDBC support |
| **JDBCOptions `auto commit`** | Mapped to `setConnAttr(SQL_ATTR_AUTOCOMMIT)` | Native JDBC support |
| **JDBCOptions `date format` / `date separator`** | Mapped to `setConnAttr(SQL_ATTR_DATE_FMT / SQL_ATTR_DATE_SEP)`; defaults to `iso` / `/` when unset | Passed through to JT400; rm-connector-js injects `iso` / `/` defaults when unset |
| **JDBCOptions `time format` / `time separator`** | Mapped to `setConnAttr(SQL_ATTR_TIME_FMT / SQL_ATTR_TIME_SEP)`; defaults to `iso` / `:` when unset | Passed through to JT400; rm-connector-js injects `iso` / `:` defaults when unset |
| **JDBCOptions `decimal separator`** | Not handled — numeric values are returned as JS numbers regardless of this setting | Passed through to JT400 if set explicitly; no default injected |
| **Default commitment control** | `SQL_TXN_NO_COMMIT` set explicitly (required for LOB column operations) | No commitment control by default |
| **`SET OPTION` statements** | Not allowed (use `setConnAttr` instead) | Supported via JDBC |
| **Credentials** | Optional (`creds: { user?, password?, database? }`). Defaults to `*LOCAL` with current user profile. `database` is an RDB directory entry name (see `WRKRDBDIRE`). | Required (`host`, `user`, `password`) |

## Libraries Behaviour by Naming Mode

The `libraries` JDBCOption behaves differently depending on the `naming` mode:

| Naming | Behaviour | Unqualified table resolution |
|--------|-----------|------------------------------|
| `'sql'` (default) | First library becomes the **default schema** | Only searches the first library |
| `'system'` | All libraries added to the **job library list** (`*LIBL`) | Searches all libraries in order |

Under SQL naming, additional libraries beyond the first are not searchable via unqualified references — use fully qualified names (e.g. `QIWS.QCUSTCDT`) or switch to system naming. Both backends now implement this consistently.

## Binary size limits

Both backends surface binary columns (`BINARY`, `VARBINARY`, `BLOB`) as Node
`Buffer`, and both accept `Buffer` as a bound parameter. How large a value can
flow through each path differs significantly.

### Measured ceilings

These round-trip figures (insert + select) were measured on a single-partition
Power system against IBM i 7.5 with the gated sizing parity test. Re-run
against your own deployment to measure the ceilings on your network, server,
and client combination:

```sh
RM_RUN_SIZING=1 npm run test:parity -- --testNamePattern="BLOB-sizing"
```

| Size    | idb insert / select † | mapepire insert / select |
|---------|-----------------------|--------------------------|
| 1 KiB   | 9 ms / 6 ms           | 8 ms / 18 ms             |
| 64 KiB  | 7 ms / 2 ms           | 18 ms / 36 ms            |
| 1 MiB   | 36 ms / 2 ms          | 387 ms / 574 ms          |
| 8 MiB   | 378 ms / 28 ms ⚠      | 3.0 s / 3.7 s            |
| 16 MiB  | 760 ms / 86 ms ⚠      | 4.4 s / 5.1 s            |
| 24 MiB  | 1.1 s / 0.6 s ⚠       | 7.0 s / 13.2 s           |
| 32 MiB  | 1.5 s / 1.0 s ⚠       | **FAIL** (WS cap)        |
| 128 MiB | 11.7 s / 4.8 s ⚠      | —                        |

† The idb timings above are from a single clean run. Repeated runs on the same
PASE host exhibited intermittent failures at every size marked ⚠ — silent byte
corruption on readback at 16 MiB in one run, and native `SIGILL` / `SIGSEGV`
crashes at 8 MiB on two further runs. Only the sub-8 MiB sizes round-tripped
reliably across every run. Treat the reliable idb ceiling as roughly **1 MiB**
on this host; the larger figures show what is *possible* on a good run, not
what to expect every time.

- **idb**: reliably round-trips up to at least 1 MiB. At ≥ 8 MiB the
  round-trip is flaky — see [IBM/nodejs-idb-connector#202](https://github.com/IBM/nodejs-idb-connector/issues/202)
  for the underlying native-memory bug and the details further down this
  section. The hard upper bound (on a good run) is driven by Node heap and
  `Buffer.constants.MAX_LENGTH` rather than the driver.
- **mapepire**: round-trips cleanly up to 24 MiB. 32 MiB fails with WebSocket
  close code 1009 "Resulting message size [52436992] is too large for
  configured max of [52428800]" — the server-side 50 MiB message cap. Since
  mapepire wraps the binary value in a lowercase hex string (2× size) inside a
  JSON envelope, the practical per-row ceiling is roughly 24 MiB of binary.

### Why the two backends behave differently

- **idb backend**: `Buffer` is passed through `Statement.bindParameters` to the
  native DB2 CLI driver, and returned directly on reads. The wrapper
  defensively copies each `Buffer` parameter before binding to work around
  memory-safety bugs in idb-connector's native parameter binding (tracked
  upstream as [IBM/nodejs-idb-connector#202](https://github.com/IBM/nodejs-idb-connector/issues/202);
  fix in progress). Without the copy:
  - the first 16 bytes of the caller's `Buffer` are overwritten with CLI
    length-indicator bytes at every size tested (8 B upward) — the server-side
    row is correct, but the caller's `Buffer` is silently clobbered after the
    call;
  - a payload of ~1 MiB causes a deterministic `SIGSEGV` during INSERT; and
  - larger payloads occasionally come back corrupted in the middle of the
    buffer (and on the server-side row), at a 16-byte window roughly 1 MiB
    before the end of the payload.
  The defensive copy fully neutralises the first two; the third is an
  additional upstream symptom the copy doesn't cover.
- **mapepire backend**: every binary value goes through a lowercase hex-string
  intermediate in both directions. This introduces three limits:
  - **V8 `String::kMaxLength`** — roughly ~512 MB on 32-bit Node and ~1 GB on
    recent 64-bit Node. Because hex is 2× the byte size, the hard per-value
    ceiling is ~256–512 MB even if the WebSocket cap were lifted.
  - **Memory amplification** — during conversion the `Buffer`, the hex string,
    and the encoded JSON payload all live in memory simultaneously, so peak
    RSS is roughly 3–4× the payload size.
  - **WebSocket message size** — mapepire's server imposes a 50 MiB maximum on
    inbound messages (observed as close code 1009 on 32 MiB BLOB inserts).

Neither backend exposes a streaming LOB API — result sets are read fully into
memory. For callers that need BLOBs larger than the mapepire ceiling, either
use the idb backend (when running on IBM i) or plan for a future streaming
initiative.

## Logging & Behaviour

| Item | idb | mapepire |
|---|---|---|
| **Log messages** | Identical structure | Identical structure |
| **Per-pool logger** | Works correctly | Works correctly |
| **Health checks** | Works correctly | Works correctly |
| **Connection expiry** | Works correctly | Works correctly |
| **Error recovery** | Connection reusable after errors | Connection reusable after errors |
| **Pool init order** | Connections may initialize out of order | Connections may initialize out of order |
