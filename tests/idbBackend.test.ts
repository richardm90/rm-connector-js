import './setup';
import { IdbBackend } from '../src/backends/idb';
import { RmLogger } from '../src/logger';

jest.mock('idb-pconnector');

// eslint-disable-next-line @typescript-eslint/no-var-requires
const idbMock = require('idb-pconnector');

// Helper to get the mock Connection instance created during init()
function getLastConnection(): any {
  const instances = (idbMock.Connection as any).__instances;
  return instances[instances.length - 1];
}

describe('IdbBackend', () => {
  const mockLogger = { log: jest.fn() };
  const rmLogger = new RmLogger(mockLogger, 'debug', 'IdbBackendTest');

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('applyJDBCOptions', () => {
    it('should default to SQL_TXN_NO_COMMIT when no transaction isolation set', async () => {
      const backend = new IdbBackend({}, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_COMMIT, idbMock.SQL_TXN_NO_COMMIT);
    });

    it('should set transaction isolation to read uncommitted', async () => {
      const backend = new IdbBackend({ 'transaction isolation': 'read uncommitted' }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_COMMIT, idbMock.SQL_TXN_READ_UNCOMMITTED);
    });

    it('should set transaction isolation to read committed', async () => {
      const backend = new IdbBackend({ 'transaction isolation': 'read committed' }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_COMMIT, idbMock.SQL_TXN_READ_COMMITTED);
    });

    it('should set transaction isolation to repeatable read', async () => {
      const backend = new IdbBackend({ 'transaction isolation': 'repeatable read' }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_COMMIT, idbMock.SQL_TXN_REPEATABLE_READ);
    });

    it('should set transaction isolation to serializable', async () => {
      const backend = new IdbBackend({ 'transaction isolation': 'serializable' }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_COMMIT, idbMock.SQL_TXN_SERIALIZABLE);
    });

    it('should set transaction isolation to none', async () => {
      const backend = new IdbBackend({ 'transaction isolation': 'none' }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_COMMIT, idbMock.SQL_TXN_NO_COMMIT);
    });

    it('should default to no commit for unknown isolation level', async () => {
      const backend = new IdbBackend({ 'transaction isolation': 'invalid' }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_COMMIT, idbMock.SQL_TXN_NO_COMMIT);
    });

    it('should enable auto commit when set to true', async () => {
      const backend = new IdbBackend({ 'auto commit': true }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_AUTOCOMMIT, idbMock.SQL_TRUE);
    });

    it('should disable auto commit when set to false', async () => {
      const backend = new IdbBackend({ 'auto commit': false }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_AUTOCOMMIT, idbMock.SQL_FALSE);
    });

    it('should not set auto commit when not specified', async () => {
      const backend = new IdbBackend({}, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      const autocommitCalls = conn.setConnAttr.mock.calls.filter(
        (call: any[]) => call[0] === idbMock.SQL_ATTR_AUTOCOMMIT
      );
      expect(autocommitCalls).toHaveLength(0);
    });

    it('should set system naming', async () => {
      const backend = new IdbBackend({ naming: 'system' }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_DBC_SYS_NAMING, 1);
    });

    it('should set SQL naming', async () => {
      const backend = new IdbBackend({ naming: 'sql' }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_DBC_SYS_NAMING, 0);
    });

    it('should set default schema for libraries under SQL naming', async () => {
      const backend = new IdbBackend({ libraries: ['MYLIB', 'TESTLIB'] }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      // SQL naming (default): first library set via SET SCHEMA, not setLibraryList
      expect(conn.setLibraryList).not.toHaveBeenCalled();
      const stmtInstances = (idbMock.MockStatement as any).__instances as any[];
      const setSchemaStmt = stmtInstances.find((s: any) =>
        s.exec.mock.calls.some((c: any[]) => c[0] === 'SET SCHEMA MYLIB')
      );
      expect(setSchemaStmt).toBeDefined();
    });

    it('should use setLibraryList for libraries under system naming', async () => {
      const backend = new IdbBackend({ libraries: ['MYLIB', 'TESTLIB'], naming: 'system' }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setLibraryList).toHaveBeenCalledWith(['MYLIB', 'TESTLIB']);
    });

    it('should handle single library as string', async () => {
      const backend = new IdbBackend({ libraries: 'MYLIB' }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      // SQL naming (default): SET SCHEMA used instead of setLibraryList
      expect(conn.setLibraryList).not.toHaveBeenCalled();
      const stmtInstances = (idbMock.MockStatement as any).__instances as any[];
      const setSchemaStmt = stmtInstances.find((s: any) =>
        s.exec.mock.calls.some((c: any[]) => c[0] === 'SET SCHEMA MYLIB')
      );
      expect(setSchemaStmt).toBeDefined();
    });

    it('should not call setLibraryList for empty libraries array', async () => {
      const backend = new IdbBackend({ libraries: [] }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setLibraryList).not.toHaveBeenCalled();
    });

    it('should apply multiple options together', async () => {
      const backend = new IdbBackend({
        'transaction isolation': 'read committed',
        'auto commit': true,
        naming: 'system',
        libraries: ['MYLIB'],
      }, [], rmLogger);
      await backend.init(true);

      const conn = getLastConnection();
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_COMMIT, idbMock.SQL_TXN_READ_COMMITTED);
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_AUTOCOMMIT, idbMock.SQL_TRUE);
      expect(conn.setConnAttr).toHaveBeenCalledWith(idbMock.SQL_ATTR_DBC_SYS_NAMING, 1);
      expect(conn.setLibraryList).toHaveBeenCalledWith(['MYLIB']);
    });
  });
});
