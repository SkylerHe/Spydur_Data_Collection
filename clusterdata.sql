-- Drop existing tables and views if they exist
DROP TABLE IF EXISTS data_dictionary;
DROP VIEW IF EXISTS clusterdata;
DROP VIEW IF EXISTS latest_reading;
DROP TABLE IF EXISTS FACTS;

-- Create the data_dictionary table
CREATE TABLE data_dictionary(
       indices TEXT,
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
     FACTS.indices,
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
     FACTS.indices = data_dictionary.indices
WHERE 
     FACTS.datum IS NOT NULL;

CREATE VIEW latest_reading AS
SELECT * FROM clusterdata 
    WHERE time = (SELECT max(time) FROM clusterdata);


CREATE TABLE FACTS(
       t DATETIME DEFAULT CURRENT_TIMESTAMP,
       indices TEXT,
       devices TEXT,
       datum TEXT default NULL);
