/* ---------------------------------------------------------------------- */
/* Script generated with: DeZign for Databases v6.3.4                     */
/* Target DBMS:           SQLite3                                         */
/* Project file:          configuration.dez                               */
/* Project name:                                                          */
/* Author:                                                                */
/* Script type:           Database creation script                        */
/* Created on:            2012-06-23 11:13                                */
/* ---------------------------------------------------------------------- */


/* ---------------------------------------------------------------------- */
/* Tables                                                                 */
/* ---------------------------------------------------------------------- */

/* ---------------------------------------------------------------------- */
/* Add table "datasets"                                                   */
/* ---------------------------------------------------------------------- */

CREATE TABLE "datasets" (
    "d_id" TEXT NOT NULL,
    "d_name" INTEGER,
    "d_source" TEXT,
    "d_dataset" TEXT,
    "d_subset" TEXT,
    "d_variation" TEXT,
    "d_creator" TEXT,
    "d_revision" TEXT,
    CONSTRAINT "PK_datasets" PRIMARY KEY ("d_id")
);

/* ---------------------------------------------------------------------- */
/* Add table "files"                                                      */
/* ---------------------------------------------------------------------- */

CREATE TABLE "files" (
    "f_id" INTEGER NOT NULL,
    "f_path" TEXT NOT NULL,
    "f_process" TEXT NOT NULL,
    "f_hash" TEXT,
    "f_modified" INTEGER,
    CONSTRAINT "PK_files" PRIMARY KEY ("f_id")
);

/* ---------------------------------------------------------------------- */
/* Add table "tables"                                                     */
/* ---------------------------------------------------------------------- */

CREATE TABLE "tables" (
    "t_id" INTEGER NOT NULL,
    "t_d_id" TEXT NOT NULL,
    "t_name" TEXT,
    "t_altname" TEXT,
    "t_description" TEXT,
    "t_keywords" TEXT,
    CONSTRAINT "PK_tables" PRIMARY KEY ("t_id"),
    CONSTRAINT "TUC_tables_1" UNIQUE ("t_name"),
    FOREIGN KEY ("t_d_id") REFERENCES "datasets" ("d_id")
);

/* ---------------------------------------------------------------------- */
/* Add table "columns"                                                    */
/* ---------------------------------------------------------------------- */

CREATE TABLE "columns" (
    "c_id" INTEGER NOT NULL,
    "c_t_id" TEXT NOT NULL,
    "c_d_id" TEXT,
    "c_name" TEXT,
    "c_altname" TEXT,
    "c_datatype" TEXT,
    "c_size" INTEGER,
    "c_precision" INTEGER,
    "c_flags" TEXT,
    "c_description" TEXT,
    "c_keywords" TEXT,
    "c_measure" TEXT,
    "c_units" TEXT,
    "c_universe" TEXT,
    "c_scale" REAL,
    CONSTRAINT "PK_columns" PRIMARY KEY ("c_id"),
    CONSTRAINT "TUC_columns_1" UNIQUE ("c_d_id", "c_t_id", "c_id"),
    FOREIGN KEY ("c_t_id") REFERENCES "tables" ("t_id"),
    FOREIGN KEY ("c_d_id") REFERENCES "datasets" ("d_id")
);

/* ---------------------------------------------------------------------- */
/* Add table "config"                                                     */
/* ---------------------------------------------------------------------- */

CREATE TABLE "config" (
    "co_d_id" TEXT NOT NULL,
    "co_group" TEXT,
    "co_key" TEXT,
    "co_source" TEXT,
    "co_value" TEXT,
    CONSTRAINT "PK_config" PRIMARY KEY ("co_d_id", "co_group", "co_key"),
    FOREIGN KEY ("co_d_id") REFERENCES "datasets" ("d_id"),
    FOREIGN KEY ("co_source") REFERENCES "files" ("f_id")
);

/* ---------------------------------------------------------------------- */
/* Add table "partitions"                                                 */
/* ---------------------------------------------------------------------- */

CREATE TABLE "partitions" (
    "p_id" TEXT NOT NULL,
    "p_t_id" INTEGER,
    "p_d_id" TEXT,
    "p_space" TEXT,
    "p_time" TEXT,
    "p_name" TEXT,
    CONSTRAINT "PK_partitions" PRIMARY KEY ("p_id"),
    FOREIGN KEY ("p_t_id") REFERENCES "tables" ("t_id"),
    FOREIGN KEY ("p_d_id") REFERENCES "datasets" ("d_id")
);

/* ---------------------------------------------------------------------- */
/* Foreign key constraints                                                */
/* ---------------------------------------------------------------------- */
