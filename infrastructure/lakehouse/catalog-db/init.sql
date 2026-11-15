-- A second, disposable catalog database for tests/lakehouse. Tests drop and
-- recreate tables; they refuse to run against any catalog whose name does not
-- end in _test, so they can never purge the real catalog (iceberg_catalog).
CREATE DATABASE iceberg_catalog_test OWNER iceberg;
