# Backend Differences: idb-pconnector vs mapepire

This document details the differences between the two backends based on testing all examples on IBM i with both `backend: 'idb'` and `backend: 'mapepire'`.

## Connection & Infrastructure

| Item | idb | mapepire |
|---|---|---|
| **Job subsystem** | `QSQSRVR` | `QZDASOINIT` |
| **Connection protocol** | Native ODBC (in-process) | WebSocket (remote) |
| **Connection creation speed** | ~5-25ms per connection | ~100-250ms per connection |
| **Connected log message** | `Connected (idb-pconnector)` | `Connected (mapepire-js)` |

## Result Envelope

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
| **`index`** | Present | Present |
| **`value`** | Present on all parameters (input and output) | Present only on output parameters |
| **`type`** | absent | Present (`'VARCHAR'`, etc.) |
| **`name`** | absent | Present (`'DATABASE_OBJECT_NAME'`, etc.) |
| **`precision`/`scale`/`ccsid`** | absent | Present |

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
| **Numeric values** | Identical | Identical |

## Features

| Feature | idb | mapepire |
|---|---|---|
| **Keepalive** | Automatically disabled (no WebSocket) | Supported (ping via `VALUES 1`) |
| **JDBCOptions `libraries`** | SQL naming: `SET SCHEMA` (first lib only); system naming: `setLibraryList()` (all libs) | Native JDBC support (same behaviour) |
| **JDBCOptions `naming`** | Mapped to `setConnAttr(SQL_ATTR_DBC_SYS_NAMING)` | Native JDBC support |
| **JDBCOptions `transaction isolation`** | Mapped to `setConnAttr(SQL_ATTR_COMMIT)` | Native JDBC support |
| **JDBCOptions `auto commit`** | Mapped to `setConnAttr(SQL_ATTR_AUTOCOMMIT)` | Native JDBC support |
| **`SET OPTION` statements** | Not allowed (use `setConnAttr` instead) | Supported via JDBC |
| **Credentials** | Not needed (connects to `*LOCAL`) | Required (`host`, `user`, `password`) |

## Libraries Behaviour by Naming Mode

The `libraries` JDBCOption behaves differently depending on the `naming` mode:

| Naming | Behaviour | Unqualified table resolution |
|--------|-----------|------------------------------|
| `'sql'` (default) | First library becomes the **default schema** | Only searches the first library |
| `'system'` | All libraries added to the **job library list** (`*LIBL`) | Searches all libraries in order |

Under SQL naming, additional libraries beyond the first are not searchable via unqualified references — use fully qualified names (e.g. `QIWS.QCUSTCDT`) or switch to system naming. Both backends now implement this consistently.

## Logging & Behaviour

| Item | idb | mapepire |
|---|---|---|
| **Log messages** | Identical structure | Identical structure |
| **Per-pool logger** | Works correctly | Works correctly |
| **Health checks** | Works correctly | Works correctly |
| **Connection expiry** | Works correctly | Works correctly |
| **Error recovery** | Connection reusable after errors | Connection reusable after errors |
| **Pool init order** | Connections may initialize out of order | Connections may initialize out of order |
