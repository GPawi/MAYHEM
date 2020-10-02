CREATE DATABASE "MAYHEM-Example"
OWNER = postgres
ENCODING = 'UTF8'
TABLESPACE = pg_default;

\connect "MAYHEM-Example";

-- DROP SCHEMA analysis_system;

CREATE SCHEMA analysis_system AUTHORIZATION postgres;

ALTER DATABASE "MAYHEM-Example" SET search_path TO analysis_system;
SET search_path TO analysis_system;

-- analysis_system.climateclassification definition

-- Drop table

-- DROP TABLE climateclassification;

CREATE TABLE climateclassification (
	climatezone varchar(3) NOT NULL,
	czdefinition varchar NULL,
	CONSTRAINT climateclassification_pk PRIMARY KEY (climatezone)
);

INSERT INTO climateclassification (climatezone,czdefinition) VALUES 
('Af','Tropical - Rainforest')
,('Am','Tropical - Monsoon')
,('Aw','Tropical - Savannah')
,('BWh','Arid - Desert  - Hot')
,('BWk','Arid - Desert - Cold')
,('BSh','Arid - Steppe - Hot')
,('BSk','Arid - Steppe - Cold')
,('Csa','Temperate - Dry Summer - Hot Summer')
,('Csb','Temperate - Dry Summer - Warm Summer')
,('Csc','Temperate - Dry Summer - Cold Summer')
;
INSERT INTO climateclassification (climatezone,czdefinition) VALUES 
('Cwa','Temperate - Dry Winter - Hot Summer')
,('Cwb','Temperate - Dry Winter - Warm Summer')
,('Cwc','Temperate - Dry Winter - Cold Summer')
,('Cfa','Temperate - Without Dry Season - Hot Summer')
,('Cfb','Temperate - Without Dry Season - Warm Summer')
,('Cfc','Temperate - Without Dry Season - Cold Summer')
,('Dsa','Cold - Dry Summer - Hot Summer')
,('Dsb','Cold - Dry Summer - Warm Summer')
,('Dsc','Cold - Dry Summer - Cold Summer')
,('Dsd','Cold - Dry Summer - Very cold winter')
;
INSERT INTO climateclassification (climatezone,czdefinition) VALUES 
('Dwa','Cold - Dry Winter - Hot Summer')
,('Dwb','Cold - Dry Winter - Warm Summer')
,('Dwc','Cold - Dry Winter - Cold Summer')
,('Dwd','Cold - Dry Winter - Very cold winter')
,('Dfa','Cold - Without Dry Season - Hot Summer')
,('Dfb','Cold - Without Dry Season - Warm Summer')
,('Dfc','Cold - Without Dry Season - Cold Summer')
,('Dfd','Cold - Without Dry Season - Very cold winter')
,('ET','Polar - Tundra')
,('EF','Polar - Frost')
;


-- analysis_system.scientist definition

-- Drop table

-- DROP TABLE scientist;

CREATE TABLE scientist (
	scientistid serial NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	email varchar NULL,
	orcid varchar(37) NULL,
	CONSTRAINT scientist_pk PRIMARY KEY (scientistid)
);


-- analysis_system.expedition definition

-- Drop table

-- DROP TABLE expedition;

CREATE TABLE expedition (
	expeditionname varchar NOT NULL,
	expeditionyear int4 NOT NULL,
	scientistid int4 NULL,
	CONSTRAINT expedition_pk PRIMARY KEY (expeditionname, expeditionyear),
	CONSTRAINT expedition_fk FOREIGN KEY (scientistid) REFERENCES scientist(scientistid)
);


-- analysis_system.lake definition

-- Drop table

-- DROP TABLE lake;

CREATE TABLE lake (
	sitename varchar NOT NULL,
	country varchar NULL,
	lakedepth numeric(6,2) NULL,
	lakeextent float8 NULL,
	catchmentarea float8 NULL,
	climatezone varchar(3) NULL,
	vegetationzone varchar NULL,
	laketype varchar NULL,
	CONSTRAINT lake_pk PRIMARY KEY (sitename),
	CONSTRAINT lake_climateclassification_fk FOREIGN KEY (climatezone) REFERENCES climateclassification(climatezone)
);


-- analysis_system.drilling definition

-- Drop table

-- DROP TABLE drilling;

CREATE TABLE drilling (
	coreid varchar NOT NULL,
	latitude numeric(9,6) NOT NULL,
	longitude numeric(9,6) NOT NULL,
	waterdepth numeric(5,2) NOT NULL,
	corelength numeric(5,2) NOT NULL,
	drillingdevice varchar NULL,
	sitename varchar NOT NULL,
	expeditionname varchar NOT NULL,
	expeditionyear int4 NOT NULL,
	CONSTRAINT drilling_pk PRIMARY KEY (coreid),
	CONSTRAINT drilling_expedition_fk FOREIGN KEY (expeditionname, expeditionyear) REFERENCES expedition(expeditionname, expeditionyear),
	CONSTRAINT drilling_lake_fk FOREIGN KEY (sitename) REFERENCES lake(sitename)
);


-- analysis_system.measurement definition

-- Drop table

-- DROP TABLE measurement;

CREATE TABLE measurement (
	measurementid varchar NOT NULL,
	coreid varchar NULL,
	compositedepth float8 NULL,
	CONSTRAINT measurement_pk PRIMARY KEY (measurementid),
	CONSTRAINT measurement_drilling_fk FOREIGN KEY (coreid) REFERENCES drilling(coreid)
);


-- analysis_system.mineral definition

-- Drop table

-- DROP TABLE mineral;

CREATE TABLE mineral (
	measurementid varchar NOT NULL,
	mineral_name varchar NOT NULL,
	mineral_wavelength numeric NULL,
	mineral_intensity int4 NULL,
	CONSTRAINT minerals_un UNIQUE (measurementid, mineral_name, mineral_wavelength),
	CONSTRAINT minerals_fk FOREIGN KEY (measurementid) REFERENCES measurement(measurementid)
);


-- analysis_system.modeloutput definition

-- Drop table

-- DROP TABLE modeloutput;

CREATE TABLE modeloutput (
	measurementid varchar NOT NULL,
	modeloutput_median numeric(8,2) NULL,
	modeloutput_mean numeric(8,2) NULL,
	lower_2_sigma numeric(8,2) NULL,
	lower_1_sigma numeric(8,2) NULL,
	upper_1_sigma numeric(8,2) NULL,
	upper_2_sigma numeric(8,2) NULL,
	model_name varchar NOT NULL,
	CONSTRAINT modeloutput_un UNIQUE (measurementid, model_name),
	CONSTRAINT modeloutput_fk FOREIGN KEY (measurementid) REFERENCES measurement(measurementid)
);


-- analysis_system.organic definition

-- Drop table

-- DROP TABLE organic;

CREATE TABLE organic (
	measurementid varchar NOT NULL,
	tn numeric(4,2) NULL,
	tc numeric(4,2) NULL,
	toc numeric(4,2) NULL,
	d13c numeric(4,2) NULL,
	water_content numeric(5,2) NULL,
	CONSTRAINT organics_un UNIQUE (measurementid),
	CONSTRAINT organics_fk FOREIGN KEY (measurementid) REFERENCES measurement(measurementid)
);


-- analysis_system.participant definition

-- Drop table

-- DROP TABLE participant;

CREATE TABLE participant (
	coreid varchar NOT NULL,
	scientistid int4 NOT NULL,
	CONSTRAINT participant_un UNIQUE (coreid, scientistid),
	CONSTRAINT participant_fk FOREIGN KEY (scientistid) REFERENCES scientist(scientistid),
	CONSTRAINT participant_fk_1 FOREIGN KEY (coreid) REFERENCES drilling(coreid)
);


-- analysis_system.pollen definition

-- Drop table

-- DROP TABLE pollen;

CREATE TABLE pollen (
	measurementid varchar NOT NULL,
	pollen_taxa varchar NOT NULL,
	pollen_count numeric NULL,
	CONSTRAINT pollen_un UNIQUE (measurementid, pollen_taxa),
	CONSTRAINT pollen_fk FOREIGN KEY (measurementid) REFERENCES measurement(measurementid)
);


-- analysis_system.publication definition

-- Drop table

-- DROP TABLE publication;

CREATE TABLE publication (
	pubshort varchar NOT NULL,
	citation varchar NOT NULL,
	type varchar(12) NOT NULL,
	coreid varchar NOT NULL,
	CONSTRAINT publication_fk FOREIGN KEY (coreid) REFERENCES drilling(coreid)
);


-- analysis_system.source definition

-- Drop table

-- DROP TABLE source;

CREATE TABLE source (
	coreid varchar NOT NULL,
	entity varchar NOT NULL,
	repository varchar NOT NULL,
	filename varchar NOT NULL,
	accessible varchar NULL,
	CONSTRAINT source_fk FOREIGN KEY (coreid) REFERENCES drilling(coreid)
);


-- analysis_system.agedetermination definition

-- Drop table

-- DROP TABLE agedetermination;

CREATE TABLE agedetermination (
	measurementid varchar NOT NULL,
	thickness numeric NULL,
	labid varchar NULL,
	lab_location varchar NULL,
	material_category varchar(22) NULL,
	material_description varchar NULL,
	material_weight int4 NULL,
	age numeric(8,2) NOT NULL,
	age_error numeric(7,2) NOT NULL,
	pretreatment_dating varchar NULL,
	reservoir_age numeric(7,2) NULL,
	reservoir_error numeric(7,2) NULL,
	CONSTRAINT agedetermination_un UNIQUE (measurementid, labid),
	CONSTRAINT agedetermination_fk FOREIGN KEY (measurementid) REFERENCES measurement(measurementid)
);


-- analysis_system.chironomid definition

-- Drop table

-- DROP TABLE chironomid;

CREATE TABLE chironomid (
	measurementid varchar NOT NULL,
	chironomid_taxa varchar NOT NULL,
	chironomid_count numeric NULL,
	CONSTRAINT chironomids_un UNIQUE (measurementid, chironomid_taxa),
	CONSTRAINT chironomids_fk FOREIGN KEY (measurementid) REFERENCES measurement(measurementid)
);


-- analysis_system.diatom definition

-- Drop table

-- DROP TABLE diatom;

CREATE TABLE diatom (
	measurementid varchar NOT NULL,
	diatom_taxa varchar NOT NULL,
	diatom_count numeric NULL,
	CONSTRAINT diatoms_un UNIQUE (measurementid, diatom_taxa),
	CONSTRAINT diatoms_fk FOREIGN KEY (measurementid) REFERENCES measurement(measurementid)
);


-- analysis_system.element definition

-- Drop table

-- DROP TABLE element;

CREATE TABLE element (
	measurementid varchar NOT NULL,
	element_name varchar NULL,
	element_value numeric NULL,
	CONSTRAINT elements_un UNIQUE (measurementid, element_name),
	CONSTRAINT elements_fk FOREIGN KEY (measurementid) REFERENCES measurement(measurementid)
);


-- analysis_system.grainsize definition

-- Drop table

-- DROP TABLE grainsize;

CREATE TABLE grainsize (
	measurementid varchar NOT NULL,
	total_clay numeric(4,2) NULL,
	total_silt numeric(4,2) NULL,
	fine_silt numeric(4,2) NULL,
	medium_silt numeric(4,2) NULL,
	coarse_silt numeric(4,2) NULL,
	total_sand numeric(4,2) NULL,
	fine_sand numeric(4,2) NULL,
	medium_sand numeric(4,2) NULL,
	coarse_sand numeric(4,2) NULL,
	total_gravel numeric(4,2) NULL,
	CONSTRAINT grainsize_un UNIQUE (measurementid),
	CONSTRAINT grainsize_fk FOREIGN KEY (measurementid) REFERENCES measurement(measurementid)
);
