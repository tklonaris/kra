\set ON_ERROR_STOP on

\if :{?kra_migrator_password}
\else
\echo 'Missing required psql variable: kra_migrator_password'
\quit 3
\endif

\if :{?kra_app_password}
\else
\echo 'Missing required psql variable: kra_app_password'
\quit 3
\endif

\if :{?kra_cdc_password}
\else
\echo 'Missing required psql variable: kra_cdc_password'
\quit 3
\endif

\if :{?kra_cdc_state_password}
\else
\echo 'Missing required psql variable: kra_cdc_state_password'
\quit 3
\endif

\if :{?kra_temporal_password}
\else
\echo 'Missing required psql variable: kra_temporal_password'
\quit 3
\endif

-- Passwords are only used while creating a missing role. Attribute statements
-- below make every later run converge without changing existing passwords.
SELECT format(
    'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L',
    'kra_migrator',
    :'kra_migrator_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kra_migrator')
\gexec

SELECT format(
    'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L',
    'kra_app',
    :'kra_app_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kra_app')
\gexec

SELECT format(
    'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT REPLICATION NOBYPASSRLS PASSWORD %L',
    'kra_cdc',
    :'kra_cdc_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kra_cdc')
\gexec

SELECT format(
    'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L',
    'kra_cdc_state',
    :'kra_cdc_state_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kra_cdc_state')
\gexec

ALTER ROLE kra_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
ALTER ROLE kra_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
ALTER ROLE kra_cdc LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT REPLICATION NOBYPASSRLS;
ALTER ROLE kra_cdc_state LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;

SELECT format(
    'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD %L',
    'temporal',
    :'kra_temporal_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'temporal')
\gexec

ALTER ROLE temporal LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;

-- CREATE DATABASE cannot run in a transaction block, so psql executes this
-- generated statement only when the application database is absent.
SELECT format('CREATE DATABASE %I OWNER %I', 'kra', 'kra_migrator')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'kra')
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', 'temporal', 'temporal')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'temporal')
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', 'temporal_visibility', 'temporal')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'temporal_visibility')
\gexec

REVOKE CONNECT ON DATABASE kra FROM PUBLIC, temporal;
REVOKE CONNECT ON DATABASE temporal FROM PUBLIC, kra_app, kra_migrator, kra_cdc, kra_cdc_state;
REVOKE CONNECT ON DATABASE temporal_visibility FROM PUBLIC, kra_app, kra_migrator, kra_cdc, kra_cdc_state;
GRANT CONNECT ON DATABASE temporal TO temporal;
GRANT CONNECT ON DATABASE temporal_visibility TO temporal;

\connect kra

CREATE SCHEMA IF NOT EXISTS kra AUTHORIZATION kra_migrator;
ALTER SCHEMA kra OWNER TO kra_migrator;

GRANT CONNECT ON DATABASE kra TO kra_migrator, kra_app, kra_cdc, kra_cdc_state;
GRANT CREATE ON DATABASE kra TO kra_migrator;
GRANT USAGE ON SCHEMA kra TO kra_app, kra_cdc, kra_cdc_state;

-- Bring already-migrated objects into the same access model.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA kra TO kra_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA kra TO kra_app;
GRANT SELECT ON ALL TABLES IN SCHEMA kra TO kra_cdc;

-- A bootstrap rerun repairs broad business-table grants for existing tables, then reasserts the
-- append-only lifecycle ledger and infrastructure-table boundaries that those grants exclude.
DO $$
BEGIN
    IF to_regclass('kra.payment_lifecycle_event') IS NOT NULL THEN
        REVOKE UPDATE, DELETE ON TABLE kra.payment_lifecycle_event FROM kra_app;
    END IF;
    IF to_regclass('kra.databasechangelog') IS NOT NULL THEN
        REVOKE ALL PRIVILEGES ON TABLE kra.databasechangelog FROM kra_app;
    END IF;
    IF to_regclass('kra.databasechangeloglock') IS NOT NULL THEN
        REVOKE ALL PRIVILEGES ON TABLE kra.databasechangeloglock FROM kra_app;
    END IF;
    IF to_regclass('kra.debezium_offset_storage') IS NOT NULL THEN
        REVOKE ALL PRIVILEGES ON TABLE kra.debezium_offset_storage FROM kra_app;
    END IF;
END
$$;

-- Liquibase creates objects as kra_migrator. These grants keep new objects
-- usable by the runtime and readable by Debezium initial snapshots.
ALTER DEFAULT PRIVILEGES FOR ROLE kra_migrator IN SCHEMA kra
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO kra_app;
ALTER DEFAULT PRIVILEGES FOR ROLE kra_migrator IN SCHEMA kra
    GRANT USAGE, SELECT ON SEQUENCES TO kra_app;
ALTER DEFAULT PRIVILEGES FOR ROLE kra_migrator IN SCHEMA kra
    GRANT USAGE ON TYPES TO kra_app;
ALTER DEFAULT PRIVILEGES FOR ROLE kra_migrator IN SCHEMA kra
    GRANT SELECT ON TABLES TO kra_cdc;
ALTER DEFAULT PRIVILEGES FOR ROLE kra_migrator IN SCHEMA kra
    GRANT USAGE ON TYPES TO kra_cdc;
