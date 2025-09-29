\set ON_ERROR_STOP on
\set VERBOSITY verbose
\set ECHO all
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='owner_staging')
    THEN
        CREATE ROLE owner_staging NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='user_staging')
    THEN
        CREATE ROLE user_staging
            NOSUPERUSER
            NOCREATEDB
            NOCREATEROLE
            INHERIT
            LOGIN
            PASSWORD 'user_staging';
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