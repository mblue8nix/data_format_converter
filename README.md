# Data Format Converter

A Personal Exercise for myself to take a text file with key points and a description of 
any like a mini CV, or anything for fun. It reads a text file, converting and writing in
different formats dynamicly creating a schema for diferent data stores...


## text_to_schema_converter.py

### Example:

A sample text example:

```
Product Super Thingy Test
Cost $123,129.99
Connectors MySQL, MongoDB, Redis, BigQuery, Snowflake
Programs Python, Bash, SQL
DataPlatforms GCP, Hortonworks(Now Cloudera)
```
#### Usage:
```shell
sh-3.2$ python3 text_to_schema_converter.py --upload test_data.txt 
```

Converting test_data from Text to JSON File: test_data.json 
```json
  {
    "Product": "Super Thingy Test",
    "Cost": "$123,129.99",
    "Connectors": "MySQL, MongoDB, Redis, BigQuery, Snowflake",
    "Programs": "Python, Bash, SQL",
    "DataPlatforms": "GCP, Hortonworks(Now Cloudera)"
  }
```

Convert JSON to Parquet:

Created File: ' test_data.parquet ' ...

Reading Parquet ' test_data.parquet ' as a Pandas DataFrame Summmary:

```
             Product         Cost  ...           Programs                   DataPlatforms
0  Super Thingy Test  $123,129.99  ...  Python, Bash, SQL  GCP, Hortonworks(Now Cloudera)

[1 rows x 5 columns]
```

Convert Json to SQL:

Created File: test_data.sql 

```SQL
CREATE TABLE IF NOT EXISTS `test_data` (
   `Id` MEDIUMINT NOT NULL AUTO_INCREMENT,Product varchar(250) DEFAULT NULL,
   Cost varchar(250) DEFAULT NULL,
   Connectors varchar(250) DEFAULT NULL,
   Programs varchar(250) DEFAULT NULL,
   DataPlatforms varchar(250) DEFAULT NULL,   
   PRIMARY KEY (id)
);


INSERT INTO test_data
  (Product, 
    Cost, 
    Connectors, 
    Programs, 
    DataPlatforms)
 VALUES ('Super Thingy Test', 
    '$123,129.99', 
    'MySQL, MongoDB, Redis, BigQuery, Snowflake', 
    'Python, Bash, SQL', 
    'GCP, Hortonworks(Now Cloudera)');
```
