/* ---------------------------------------------------------------------- */
/* Script generated with: DeZign for Databases v6.3.4                     */
/* Target DBMS:           SQLite3                                         */
/* Project file:          configuration.dez                               */
/* Project name:                                                          */
/* Author:                                                                */
/* Script type:           Database drop script                            */
/* Created on:            2012-06-13 16:35                                */
/* ---------------------------------------------------------------------- */


/* ---------------------------------------------------------------------- */
/* Drop foreign key constraints                                           */
/* ---------------------------------------------------------------------- */

/* ---------------------------------------------------------------------- */
/* Drop table "columns"                                                   */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE "columns" DROP CONSTRAINT "NN_columns_c_d_id";

ALTER TABLE "columns" DROP CONSTRAINT "NN_columns_c_t_id";

ALTER TABLE "columns" DROP CONSTRAINT "NN_columns_c_id";

ALTER TABLE "columns" DROP CONSTRAINT "PK_columns";

/* Drop table */

DROP TABLE "columns";

/* ---------------------------------------------------------------------- */
/* Drop table "tables"                                                    */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE "tables" DROP CONSTRAINT "NN_tables_t_d_id";

ALTER TABLE "tables" DROP CONSTRAINT "NN_tables_t_id";

ALTER TABLE "tables" DROP CONSTRAINT "PK_tables";

/* Drop table */

DROP TABLE "tables";

/* ---------------------------------------------------------------------- */
/* Drop table "config"                                                    */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE "config" DROP CONSTRAINT "NN_config_co_d_id";

ALTER TABLE "config" DROP CONSTRAINT "PK_config";

/* Drop table */

DROP TABLE "config";

/* ---------------------------------------------------------------------- */
/* Drop table "datasets"                                                  */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE "datasets" DROP CONSTRAINT "NN_datasets_d_id";

ALTER TABLE "datasets" DROP CONSTRAINT "PK_datasets";

/* Drop table */

DROP TABLE "datasets";
