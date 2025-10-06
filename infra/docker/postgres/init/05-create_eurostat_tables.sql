SET ROLE staging_owner;
CREATE TABLE IF NOT EXISTS
staging.eurostat_ds (
code text primary key,
title text,
last_update_timestamp TIMESTAMPTZ NOT NULL,
last_structure_change_timestamp TIMESTAMPTZ NOT NULL,
start_year NUMERIC(4),
end_year NUMERIC(4),
period text,
load_date DATE default CURRENT_DATE);
RESET ROLE;