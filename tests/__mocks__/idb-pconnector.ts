// Manual mock for idb-pconnector

// Track all created MockStatement instances for test assertions
const statementInstances: MockStatement[] = [];

class MockStatement {
  private numericConversion = false;

  exec = jest.fn(async (sql: string): Promise<any[]> => {
    if (sql === 'VALUES QSYS2.JOB_NAME') {
      return [{ '00001': '123456/QUSER/QZDASOINIT' }];
    }
    return [];
  });

  async prepare(_sql: string): Promise<void> {}
  async bindParameters(_params: any[]): Promise<void> {}
  async execute(): Promise<void> {}
  async fetchAll(): Promise<any[]> { return []; }
  enableNumericTypeConversion(enable: boolean): void { this.numericConversion = enable; }
  numRows(): number { return 0; }
  numFields(): number { return 0; }
  fieldName(_index: number): string { return ''; }
  fieldType(_index: number): number { return 0; }
  close(): void {}

  constructor() {
    statementInstances.push(this);
  }
}

(MockStatement as any).__instances = statementInstances;

// Track all created Connection instances for test assertions
const instances: Connection[] = [];

export class Connection {
  setConnAttr = jest.fn();
  setLibraryList = jest.fn();
  connect = jest.fn().mockReturnThis();

  constructor(_opts?: any) {
    instances.push(this);
  }

  getStatement(): any { return new MockStatement(); }
  disconn(): void {}
  close(): void {}
}

// Expose instances for tests
(Connection as any).__instances = instances;

// ODBC constants (matching idb-connector values)
export const SQL_ATTR_COMMIT = 0;
export const SQL_TXN_NO_COMMIT = 0;
export const SQL_TXN_READ_UNCOMMITTED = 1;
export const SQL_TXN_READ_COMMITTED = 2;
export const SQL_TXN_REPEATABLE_READ = 4;
export const SQL_TXN_SERIALIZABLE = 8;
export const SQL_ATTR_AUTOCOMMIT = 10003;
export const SQL_TRUE = 1;
export const SQL_FALSE = 0;
export const SQL_ATTR_DBC_SYS_NAMING = 0x10012;

// Expose MockStatement for tests
export { MockStatement };
