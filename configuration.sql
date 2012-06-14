/* ---------------------------------------------------------------------- */
/* Script generated with: DeZign for Databases v6.3.4                     */
/* Target DBMS:           SQLite3                                         */
/* Project file:          configuration.dez                               */
/* Project name:                                                          */
/* Author:                                                                */
/* Script type:           Database creation script                        */
/* Created on:            2012-06-13 16:35                                */
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
/* Add table "config"                                                     */
/* ---------------------------------------------------------------------- */

CREATE TABLE "config" (
    "co_d_id" TEXT NOT NULL,
    "co_group" TEXT,
    "co_key" TEXT,
    "co_value" TEXT,
    CONSTRAINT "PK_config" PRIMARY KEY ("co_d_id"),
    FOREIGN KEY ("co_d_id") REFERENCES "datasets" ("d_id")
);

/* ---------------------------------------------------------------------- */
/* Add table "tables"                                                     */
/* ---------------------------------------------------------------------- */

CREATE TABLE "tables" (
    "t_d_id" TEXT NOT NULL,
    "t_id" INTEGER NOT NULL,
    "t_name" TEXT,
    "t_altname" TEXT,
    "t_description" TEXT,
    "t_keywords" TEXT,
    CONSTRAINT "PK_tables" PRIMARY KEY ("t_d_id", "t_id"),
    FOREIGN KEY ("t_d_id") REFERENCES "datasets" ("d_id")
);

/* ---------------------------------------------------------------------- */
/* Add table "columns"                                                    */
/* ---------------------------------------------------------------------- */

CREATE TABLE "columns" (
    "c_d_id" TEXT NOT NULL,
    "c_t_id" INTEGER NOT NULL,
    "c_id" INTEGER NOT NULL,
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
    CONSTRAINT "PK_columns" PRIMARY KEY ("c_d_id", "c_t_id", "c_id"),
    FOREIGN KEY ("c_d_id", "c_t_id") REFERENCES "tables" ("t_d_id","t_id")
);

/* ---------------------------------------------------------------------- */
/* Foreign key constraints                                                */
/* ---------------------------------------------------------------------- */
