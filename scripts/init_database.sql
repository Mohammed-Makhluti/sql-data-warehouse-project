/*
=============================================================
1) Create Database
=============================================================
*/

CREATE DATABASE data_warehouse;

/*
=============================================================
2) Create Schemas
=============================================================
*/

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA bronze IS 'Raw data layer';
COMMENT ON SCHEMA silver IS 'Cleaned and transformed data layer';
COMMENT ON SCHEMA gold   IS 'Final analytical and reporting layer';
