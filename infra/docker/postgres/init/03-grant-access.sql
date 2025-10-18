\set ON_ERROR_STOP on
\set VERBOSITY verbose
\set ECHO all
-- Revoke
REVOKE CONNECT ON DATABASE demo FROM public;
GRANT CONNECT ON DATABASE demo TO staging_ro;
GRANT USAGE ON SCHEMA staging TO staging_ro;
-- Grant privileges
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO staging_ro;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO staging_rw;
-- Set default schemas
ALTER ROLE staging_ro SET search_path = staging;
ALTER ROLE staging_rw SET search_path = staging;
ALTER ROLE staging_airflow SET search_path = staging;
-- Future objects
ALTER DEFAULT PRIVILEGES FOR ROLE staging_owner IN SCHEMA staging
    GRANT SELECT ON TABLES to staging_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE staging_owner IN SCHEMA staging
    GRANT INSERT, UPDATE, DELETE ON TABLES to staging_rw;
