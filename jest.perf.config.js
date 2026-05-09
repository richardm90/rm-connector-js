/**
 * Jest config for backend performance tests.
 *
 * These tests connect to a real IBM i system and compare the performance
 * of the mapepire and idb backends running on the same machine.
 *
 * IMPORTANT: roots points to tests/performance/ (not tests/) to avoid
 * Jest auto-discovering the manual mocks in tests/__mocks__/ which
 * would intercept the real idb-pconnector and @ibm/mapepire-js modules.
 *
 * Usage:
 *   IBMI_HOST=myibmi.com IBMI_USER=MYUSER IBMI_PASSWORD=MYPASS npm run test:performance
 */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/tests/performance'],
  testMatch: ['**/rm-backend-performance.test.ts', '**/native-backend-performance.test.ts', '**/pool-contention-proof.test.ts'],
  moduleFileExtensions: ['ts', 'js', 'json'],
  verbose: true,
};
