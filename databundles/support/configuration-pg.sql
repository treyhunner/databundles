/* ---------------------------------------------------------------------- */
/* Script generated with: DeZign for Databases v6.3.4                     */
/* Target DBMS:           PostgreSQL 9                                    */
/* Project file:          configuration-pg.dez                            */
/* Project name:                                                          */
/* Author:                                                                */
/* Script type:           Database creation script                        */
/* Created on:            2012-08-30 20:23                                */
/* ---------------------------------------------------------------------- */


/* ---------------------------------------------------------------------- */
/* Tables                                                                 */
/* ---------------------------------------------------------------------- */

/* ---------------------------------------------------------------------- */
/* Add table "datasets"                                                   */
/* ---------------------------------------------------------------------- */

CREATE TABLE datasets (
    d_id TEXT  NOT NULL,
    d_name TEXT,
    d_source TEXT,
    d_dataset TEXT,
    d_subset TEXT,
    d_variation TEXT,
    d_creator TEXT,
    d_revision TEXT,
    d_data TEXT,
    d_repository TEXT,
    CONSTRAINT PK_datasets PRIMARY KEY (d_id)
);

/* ---------------------------------------------------------------------- */
/* Add table "config"                                                     */
/* ---------------------------------------------------------------------- */

CREATE TABLE config (
    co_d_id TEXT  NOT NULL,
    co_group TEXT  NOT NULL,
    co_key TEXT  NOT NULL,
    co_value TEXT,
    co_source TEXT,
    CONSTRAINT PK_config PRIMARY KEY (co_d_id, co_group, co_key)
);

/* ---------------------------------------------------------------------- */
/* Add table "files"                                                      */
/* ---------------------------------------------------------------------- */

CREATE TABLE files (
    f_id INTEGER  NOT NULL,
    f_path TEXT  NOT NULL,
    f_source_url TEXT,
    f_process TEXT,
    f_ref TEXT,
    f_group TEXT,
    f_state TEXT,
    f_hash TEXT,
    f_modified TIMESTAMP,
    f_size INTEGER,
    CONSTRAINT PK_files PRIMARY KEY (f_id)
);

/* ---------------------------------------------------------------------- */
/* Add table "tables"                                                     */
/* ---------------------------------------------------------------------- */

CREATE TABLE tables (
    t_id TEXT  NOT NULL,
    t_sequence_id INTEGER  NOT NULL,
    t_d_id TEXT  NOT NULL,
    t_name TEXT  NOT NULL,
    t_altname TEXT,
    t_description TEXT,
    t_keywords TEXT,
    t_data TEXT,
    CONSTRAINT PK_tables PRIMARY KEY (t_id),
    CONSTRAINT TUC_tables_1 UNIQUE (t_name, t_d_id),
    CONSTRAINT TUC_tables_2 UNIQUE (t_d_id, t_sequence_id)
);

/* ---------------------------------------------------------------------- */
/* Add table "columns"                                                    */
/* ---------------------------------------------------------------------- */

CREATE TABLE columns (
    c_id TEXT  NOT NULL,
    c_sequence_id INTEGER  NOT NULL,
    c_t_id TEXT,
    c_name TEXT,
    c_altname TEXT,
    c_is_primary_key BOOLEAN,
    c_is_foreign_key BOOLEAN,
    c_unique_constraints TEXT,
    c_indexes TEXT,
    c_uindexes TEXT,
    c_datatype TEXT,
    c_default TEXT,
    c_size INTEGER,
    c_width INTEGER,
    c_illegal_value TEXT,
    c_precision INTEGER,
    c_flags TEXT,
    c_description TEXT,
    c_keywords TEXT,
    c_measure TEXT,
    c_units TEXT,
    c_universe TEXT,
    c_scale REAL,
    c_data TEXT,
    CONSTRAINT PK_columns PRIMARY KEY (c_id),
    CONSTRAINT TUC_columns_1 UNIQUE (c_sequence_id, c_t_id),
    CONSTRAINT TUC_columns_2 UNIQUE (c_sequence_id, c_t_id)
);

/* ---------------------------------------------------------------------- */
/* Add table "partitions"                                                 */
/* ---------------------------------------------------------------------- */

CREATE TABLE partitions (
    p_id TEXT  NOT NULL,
    p_name TEXT  NOT NULL,
    p_d_id TEXT  NOT NULL,
    p_sequence_id INTEGER  NOT NULL,
    p_space TEXT,
    p_time TEXT,
    p_grain TEXT,
    p_t_id TEXT,
    p_data TEXT,
    p_state TEXT,
    CONSTRAINT PK_partitions PRIMARY KEY (p_id),
    CONSTRAINT TUC_partitions_1 UNIQUE (p_name)
);

/* ---------------------------------------------------------------------- */
/* Foreign key constraints                                                */
/* ---------------------------------------------------------------------- */

ALTER TABLE tables ADD CONSTRAINT datasets_tables 
    FOREIGN KEY (t_d_id) REFERENCES datasets (d_id) ON DELETE CASCADE;

ALTER TABLE columns ADD CONSTRAINT tables_columns 
    FOREIGN KEY (c_t_id) REFERENCES tables (t_id) ON DELETE CASCADE;

ALTER TABLE config ADD CONSTRAINT datasets_config 
    FOREIGN KEY (co_d_id) REFERENCES datasets (d_id);

ALTER TABLE partitions ADD CONSTRAINT datasets_partitions 
    FOREIGN KEY (p_d_id) REFERENCES datasets (d_id) ON DELETE CASCADE;

ALTER TABLE partitions ADD CONSTRAINT tables_partitions 
    FOREIGN KEY (p_t_id) REFERENCES tables (t_id) ON DELETE CASCADE;
