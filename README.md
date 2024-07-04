# Spydur Data Collection

This program consists of three main files: `collector.py`, `data_dict.py`, and `clusterdata.sql`. It is designed to facilitate the collection and organization of data from the Spydur cluster, developed by ACT company.

## Components

### 1. collector.py
This script is responsible for collecting data from the Spydur cluster. It gathers various datum that are crucial for the operations and analytics performed within the cluster environment.

### 2. data_dict.py
This script builds a data dictionary that provides users with a general understanding of the data collected by the collector. The data dictionary includes information such as units and methods but does not contain any actual data. It serves as a comprehensive reference for users to understand the context and structure of the data collected by `collector.py` as well as additional metadata.

### 3. clusterdata.sql
This SQL file includes the schemas of facts and a data dictionary table. It also contains a view table which merges the data from the first two tables (`collector.py` and `data_dict.py`) and selects specific columns for streamlined future viewing and analysis.


