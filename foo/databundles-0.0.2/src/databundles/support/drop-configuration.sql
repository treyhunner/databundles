/* ---------------------------------------------------------------------- */
/* Script generated with: DeZign for Databases v6.3.4                     */
/* Target DBMS:           SQLite3                                         */
/* Project file:          configuration.dez                               */
/* Project name:                                                          */
/* Author:                                                                */
/* Script type:           Database drop script                            */
/* Created on:            2012-07-01 15:44                                */
/* ---------------------------------------------------------------------- */


/* ---------------------------------------------------------------------- */
/* Drop foreign key constraints                                           */
/* ---------------------------------------------------------------------- */

/* ---------------------------------------------------------------------- */
/* Drop table "partitions"                                                */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE "partitions" DROP CONSTRAINT "PK_partitions";

/* Drop table */

DROP TABLE "partitions";

/* ---------------------------------------------------------------------- */
/* Drop table "config"                                                    */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE "config" DROP CONSTRAINT "NN_config_co_d_id";

ALTER TABLE "config" DROP CONSTRAINT "PK_config";

/* Drop table */

DROP TABLE "config";

/* ---------------------------------------------------------------------- */
/* Drop table "columns"                                                   */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE "columns" DROP CONSTRAINT "NN_columns_c_id";

ALTER TABLE "columns" DROP CONSTRAINT "NN_columns_c_sequence_id";

ALTER TABLE "columns" DROP CONSTRAINT "PK_columns";

ALTER TABLE "columns" DROP CONSTRAINT "TUC_columns_1";

ALTER TABLE "columns" DROP CONSTRAINT "TUC_columns_2";

/* Drop table */

DROP TABLE "columns";

/* ---------------------------------------------------------------------- */
/* Drop table "tables"                                                    */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE "tables" DROP CONSTRAINT "NN_tables_t_sequence_id";

ALTER TABLE "tables" DROP CONSTRAINT "NN_tables_t_d_id";

ALTER TABLE "tables" DROP CONSTRAINT "PK_tables";

ALTER TABLE "tables" DROP CONSTRAINT "TUC_tables_1";

ALTER TABLE "tables" DROP CONSTRAINT "TUC_tables_2";

/* Drop table */

DROP TABLE "tables";

/* ---------------------------------------------------------------------- */
/* Drop table "files"                                                     */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE "files" DROP CONSTRAINT "NN_files_f_id";

ALTER TABLE "files" DROP CONSTRAINT "NN_files_f_path";

ALTER TABLE "files" DROP CONSTRAINT "PK_files";

/* Drop table */

DROP TABLE "files";

/* ---------------------------------------------------------------------- */
/* Drop table "datasets"                                                  */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE "datasets" DROP CONSTRAINT "NN_datasets_d_id";

ALTER TABLE "datasets" DROP CONSTRAINT "PK_datasets";

/* Drop table */

DROP TABLE "datasets";
