\set ON_ERROR_STOP on
\set VERBOSITY verbose
\set ECHO all
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='staging_ro')
    THEN
        CREATE ROLE staging_ro
        NOSUPERUSER
        NOCREATEDB
        NOCREATEROLE
        LOGIN
        PASSWORD 'dsf78%$fd';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='staging_rw')
    THEN
        CREATE ROLE staging_rw
        NOLOGIN
        INHERIT;
        GRANT staging_ro TO staging_rw;
    END IF;
        IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='staging_owner')
    THEN
        CREATE ROLE staging_owner
        NOLOGIN
        INHERIT;
        GRANT staging_rw to staging_owner;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='staging_airflow')
    THEN
        CREATE ROLE staging_airflow
        NOSUPERUSER
        NOCREATEDB
        NOCREATEROLE
        LOGIN
        PASSWORD 'jf532vVT'
        INHERIT;
        GRANT staging_rw to staging_airflow;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='owner_ods')
    THEN
        CREATE ROLE owner_ods NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='owner_analytics')
    THEN
        CREATE ROLE owner_analytics NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='user_ods')
    THEN
        CREATE ROLE user_ods
            NOSUPERUSER
            NOCREATEDB
            NOCREATEROLE
            INHERIT
            LOGIN
            PASSWORD 'user_ods';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='user_analytics')
    THEN
        CREATE ROLE user_analytics
            NOSUPERUSER
            NOCREATEDB
            NOCREATEROLE
            INHERIT
            LOGIN
            PASSWORD 'user_analytics';
    END IF;
END $$;