-- Drop existing tables and views if they exist
DROP TABLE IF EXISTS data_dictionary;
DROP VIEW IF EXISTS clusterdata;
DROP TABLE IF EXISTS FACTS;

-- Create the data_dictionary table
CREATE TABLE data_dictionary(
       indexs TEXT,
       statstypes TEXT,
       names TEXT,
       types TEXT,
       methods TEXT,
       units TEXT,
       precision INTEGER);

-- Create the clusterdata view
CREATE VIEW clusterdata AS
SELECT
     DATETIME(t, 'localtime') AS time,
     FACTS.indexs,
     FACTS.devices,
     FACTS.datum,
     data_dictionary.types,
     data_dictionary.methods,
     data_dictionary.units
FROM
     FACTS
LEFT JOIN
     data_dictionary
ON
     FACTS.indexs = data_dictionary.indexs
/* clusterdata(time,indexs,devices,datum,types,methods,units) */;


-- Create the FACTS table
CREATE TABLE FACTS(
       t DATETIME DEFAULT CURRENT_TIMESTAMP,
       indexs TEXT,
       devices TEXT,
       datum TEXT);
