import { BackendConnection } from './types';
import { InitCommand, QueryOptions, RmQueryResult } from '../types';
import { RmLogger } from '../logger';

// idb-pconnector types (loaded dynamically)
interface IdbConnection {
  setConnAttr(attr: number, value: number): void;
  setLibraryList(libraryList: string[]): void;
  connect(url?: string, username?: string, password?: string): any;
  getStatement(): IdbStatement;
  disconn(): void;
  close(): void;
}

interface IdbStatement {
  exec(sql: string): Promise<any[]>;
  prepare(sql: string): Promise<void>;
  bindParameters(params: any[]): Promise<void>;
  execute(): Promise<void>;
  fetchAll(): Promise<any[]>;
  enableNumericTypeConversion(enable: boolean): void;
  numRows(): number;
  numFields(): number;
  fieldName(index: number): string;
  fieldType(index: number): number;
  close(): void;
}

// ODBC constants (SQL_ATTR_COMMIT, SQL_TXN_*, SQL_ATTR_AUTOCOMMIT, etc.) are
// loaded dynamically from idb-pconnector (re-exported from idb-connector).

export class IdbBackend implements BackendConnection {
  private conn!: IdbConnection;
  private jobName?: string;
  private initCommands: InitCommand[];
  private JDBCOptions: Record<string, any>;
  private rmLogger: RmLogger;
  private status: string = 'notStarted';
  private queryCounter: number = 0;
  private IdbModule: any;

  constructor(JDBCOptions: Record<string, any>, initCommands: InitCommand[], rmLogger: RmLogger) {
    this.JDBCOptions = JDBCOptions;
    this.initCommands = initCommands;
    this.rmLogger = rmLogger;
  }

  async init(suppressConnectionMessage: boolean = false): Promise<void> {
    // Dynamically load idb-pconnector
    try {
      this.IdbModule = require('idb-pconnector');
    } catch (e) {
      throw new Error(
        'idb-pconnector is not available. This backend only works on IBM i. ' +
        'Use backend: "mapepire" for remote connections.'
      );
    }

    const { Connection } = this.IdbModule;
    this.conn = new Connection({ url: '*LOCAL' });
    this.status = 'connecting';

    // Apply JDBCOptions (connection attributes + SQL-based options)
    await this.applyJDBCOptions();

    this.status = 'ready';

    if (!suppressConnectionMessage)
      this.rmLogger.info(`Connected (idb-pconnector)`);

    // Execute init commands
    for (let i = 0; i < this.initCommands.length; i += 1) {
      const { command, type = 'cl' } = this.initCommands[i];
      if (command) {
        if (type === 'sql') {
          await this.execSimple(command);
        } else {
          await this.execParameterized(`CALL QSYS2.QCMDEXC(?)`, [command]);
        }
        this.rmLogger.debug(`Executed init command (${type}): ${command}`);
      }
    }

    // Get job name
    const result = await this.execSimple('VALUES QSYS2.JOB_NAME');
    if (result.length > 0) {
      const row = result[0];
      this.jobName = row[Object.keys(row)[0]];
    }
  }

  async execute(sql: string, opts: QueryOptions = {}): Promise<RmQueryResult<any>> {
    const startTime = performance.now();
    let data: any[];
    let outputParms: any[] | null = null;
    let updateCount = 0;
    let hasResultSet = true;

    try {
      if (opts.parameters && opts.parameters.length > 0) {
        const result = await this.execParameterized(sql, opts.parameters);
        data = result.data;
        outputParms = result.outputParms;
        hasResultSet = result.hasResultSet;
      } else {
        // execSimple (stmt.exec) always returns a result set
        data = await this.execSimple(sql);
      }

      // Trim string values to match mapepire behaviour
      data = data.map(row => {
        const trimmed: Record<string, any> = {};
        for (const key of Object.keys(row)) {
          trimmed[key] = typeof row[key] === 'string' ? row[key].trimEnd() : row[key];
        }
        return trimmed;
      });

      const executionTime = performance.now() - startTime;
      this.queryCounter++;

      const result: RmQueryResult<any> = {
        success: true,
        data,
        has_results: hasResultSet,
        is_done: true,
        update_count: updateCount,
        sql_rc: 0,
        sql_state: '00000',
        execution_time: executionTime,
        id: String(this.queryCounter),
        job: this.jobName!,
        metadata: null as any,
      };

      // Include output parameters if returned (e.g. from stored procedures)
      if (outputParms) {
        (result as any).output_parms = outputParms.map((value, i) => ({
          index: i + 1,
          value: typeof value === 'string' ? value.trimEnd() : value,
        }));
      }

      return result;
    } catch (error) {
      this.queryCounter++;
      throw error;
    }
  }

  async close(): Promise<void> {
    try {
      this.conn.disconn();
      this.conn.close();
    } catch (e) {
      // Ignore close errors
    }
    this.status = 'ended';
  }

  getJobName(): string | undefined {
    return this.jobName;
  }

  getStatus(): string {
    return this.status;
  }

  private async execSimple(sql: string): Promise<any[]> {
    const stmt = this.conn.getStatement();
    try {
      stmt.enableNumericTypeConversion(true);
      const result = await stmt.exec(sql);
      return result || [];
    } finally {
      stmt.close();
    }
  }

  private async execParameterized(sql: string, params: any[]): Promise<{ data: any[]; outputParms: any[] | null; hasResultSet: boolean }> {
    const stmt = this.conn.getStatement();
    try {
      stmt.enableNumericTypeConversion(true);
      await stmt.prepare(sql);
      await stmt.bindParameters(params);
      // execute() returns output parameters as an array at runtime, but is typed as void
      const outputParms = (await stmt.execute() as any) || null;
      let data: any[];
      let hasResultSet = true;
      try {
        data = await stmt.fetchAll();
      } catch (e: any) {
        // Statements like CALL QSYS2.QCMDEXC(?) don't produce a result set
        if (e?.message?.includes('no result set') || e?.sqlcode === 8014) {
          data = [];
          hasResultSet = false;
        } else {
          throw e;
        }
      }
      return { data, outputParms, hasResultSet };
    } finally {
      stmt.close();
    }
  }

  private async applyJDBCOptions(): Promise<void> {
    const opts = this.JDBCOptions as any;
    const {
      SQL_ATTR_COMMIT,
      SQL_TXN_NO_COMMIT,
      SQL_TXN_READ_UNCOMMITTED,
      SQL_TXN_READ_COMMITTED,
      SQL_TXN_REPEATABLE_READ,
      SQL_TXN_SERIALIZABLE,
      SQL_ATTR_AUTOCOMMIT,
      SQL_TRUE,
      SQL_FALSE,
    } = this.IdbModule;

    // --- Connection attributes (must be set before any SQL) ---

    const isolationMap: Record<string, number> = {
      'none': SQL_TXN_NO_COMMIT,
      'read uncommitted': SQL_TXN_READ_UNCOMMITTED,
      'read committed': SQL_TXN_READ_COMMITTED,
      'repeatable read': SQL_TXN_REPEATABLE_READ,
      'serializable': SQL_TXN_SERIALIZABLE,
    };

    // Transaction isolation (commitment control level)
    if (opts['transaction isolation']) {
      const level = isolationMap[opts['transaction isolation']];
      if (level !== undefined) {
        this.conn.setConnAttr(SQL_ATTR_COMMIT, level);
        this.rmLogger.debug(`Set transaction isolation: ${opts['transaction isolation']}`);
      } else {
        this.rmLogger.debug(`Unknown transaction isolation: ${opts['transaction isolation']}, defaulting to none`);
        this.conn.setConnAttr(SQL_ATTR_COMMIT, SQL_TXN_NO_COMMIT);
      }
    } else {
      // Default: no commitment control (matches previous behaviour)
      this.conn.setConnAttr(SQL_ATTR_COMMIT, SQL_TXN_NO_COMMIT);
    }

    // Auto commit
    if (opts['auto commit'] !== undefined) {
      this.conn.setConnAttr(SQL_ATTR_AUTOCOMMIT, opts['auto commit'] ? SQL_TRUE : SQL_FALSE);
      this.rmLogger.debug(`Set auto commit: ${opts['auto commit']}`);
    }

    // Naming
    if (opts.naming) {
      const SQL_ATTR_DBC_SYS_NAMING = this.IdbModule.SQL_ATTR_DBC_SYS_NAMING ?? 0x10012;
      const value = opts.naming === 'system' ? 1 : 0;
      this.conn.setConnAttr(SQL_ATTR_DBC_SYS_NAMING, value);
      this.rmLogger.debug(`Set naming: ${opts.naming === 'system' ? '*SYS' : '*SQL'}`);
    }

    // Libraries
    // Mapepire/JDBC behaviour:
    //   SQL naming:    first library becomes the default schema; additional libraries are
    //                  not searchable via unqualified references.
    //   System naming: all libraries are added to the job library list (*LIBL).
    // We replicate this so both backends behave identically.
    if (opts.libraries) {
      const libs = Array.isArray(opts.libraries) ? opts.libraries : [opts.libraries];
      if (libs.length > 0) {
        const isSystemNaming = opts.naming === 'system';
        if (isSystemNaming) {
          this.conn.setLibraryList(libs);
          this.rmLogger.debug(`Set library list: ${libs.join(', ')}`);
        } else {
          // SQL naming: set first library as default schema (matches mapepire)
          await this.execSimple(`SET SCHEMA ${libs[0]}`);
          this.rmLogger.debug(`Set default schema: ${libs[0]}`);
          if (libs.length > 1) {
            this.rmLogger.debug(`Libraries beyond first (${libs.slice(1).join(', ')}) are not searchable under SQL naming — use qualified references or switch to system naming`);
          }
        }
      }
    }

    // Log warnings for other JDBC options that don't map to idb
    const mappedKeys = ['libraries', 'naming', 'transaction isolation', 'auto commit'];
    for (const key of Object.keys(opts)) {
      if (!mappedKeys.includes(key)) {
        this.rmLogger.debug(`JDBCOption '${key}' is not supported by idb backend, ignoring`);
      }
    }
  }
}
