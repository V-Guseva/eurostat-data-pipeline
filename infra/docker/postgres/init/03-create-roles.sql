DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='owner_staging')
    THEN
        CREATE ROLE owner_staging nologin;
    ELSE
        RAISE NOTICE 'owner_staging is already existed';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='owner_ods')
    THEN
        CREATE ROLE owner_ods nologin;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='owner_analitics')
    THEN
        CREATE ROLE owner_analitics nologin;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='user_staging')
    THEN
        CREATE ROLE user_staging LOGIN PASSWORD 'user_staging';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='user_ods')
    THEN
        CREATE ROLE user_ods LOGIN PASSWORD 'user_ods';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM PG_ROLES WHERE rolname='user_analitics')
    THEN
        CREATE ROLE user_analitics LOGIN PASSWORD 'user_analitics';
    END IF;
END $$;