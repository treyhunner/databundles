CREATE TABLE "Tables" (
"t_id" INT NOT NULL,
"t_bundle" VARCHAR(255) NOT NULL,
"t_name" VARCHAR(255) NOT NULL,
"t_altname" VARCHAR(255) NOT NULL,
PRIMARY KEY ("t_id") 
);

CREATE TABLE "Columns" (
"c_id" INT NULL,
"c_bundle" VARCHAR(255) NOT NULL,
"c_name" VARCHAR(255) NOT NULL,
"c_altname" VARCHAR(255) NOT NULL,
"c_table" VARCHAR(255) NULL,
PRIMARY KEY ("c_id") 
);

CREATE TABLE "Configuration" (
"cf_id" SERIAL NULL,
"cf_table" INTEGER NULL,
PRIMARY KEY ("cf_id") 
);

