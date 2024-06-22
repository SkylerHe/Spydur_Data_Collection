CREATE TABLE data_dictionary(
titles TEXT PRIMARY KEY,
statstypes TEXT NOT NULL,
names TEXT NOT NULL,
types TEXT NOT NULL,
methods TEXT,
units TEXT,
precision INTEGER);
CREATE TABLE devices(
titles TEXT PRIMARY KEY,
devices TEXT);
CREATE TABLE FACTS(
t DATETIME DEFAULT CURRENT_TIMESTAMP,
indexs TEXT,
devices TEXT,
datum TEXT);
