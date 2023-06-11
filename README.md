# Dynamic Data Ingestion and Storage in HDFS using a Configurable Script

## Technologies :
**Python, API, HDFS, Hive**

## Objective :
The goal of this task is to fetch data from https://www2.census.gov/programs-surveys/popest/datasets/ , store it in HDFS, and create a Hive table to view the data. The first step is to verify that you have access to the link and are able to download the data. Next, you will need to determine the format of the data and, if it is in a structured format, determine the schema. Then, you can use a tool like wget or curl to download the data directly from the link and use the HDFS command line interface to store it in HDFS. Finally, you will create a Hive table and load the data into the table. You can then view the data in the table and verify that it has been correctly loaded. As an optional step, you can create a script to automate this process so that you can easily refresh the data in the Hive table as new data becomes available.

## Approach:
1. Verify that you have access to the link https://www2.census.gov/programs-surveys/popest/datasets/ and that you are able to download the data from this link.
2. Determine the format of the data that is available at the link. Is it available in a single file or is it split into multiple files? Is the data in a structured format (e.g. CSV, JSON) or is it in an unstructured format (e.g. PDF)?
3. If the data is available in a structured format, determine the schema of the data. This will help you create a Hive table to store the data in HDFS.
4. Use a tool like wget or curl to download the data directly from the link and pipe the output to the HDFS command line interface. Use the hadoop fs -put command to upload the data to a new HDFS directory. For example:
5. Use the Hive command line interface to create a new database and a new table in that database.
6. Use the LOAD DATA INPATH HiveQL command to load the data from the HDFS directory into the newly created Hive table.
7. Use the SELECT HiveQL command to view the data in the Hive table and verify that it has been correctly loaded.
8 .Optionally, you can create a script to automate the process of fetching the data from the link, storing it in HDFS, and loading it into a Hive table. This will allow you to easily refresh the data in the Hive table as new data becomes available at the link.

## Results:
The result of following the above approach should be that you have successfully fetched data from the link https://www2.census.gov/programs-surveys/popest/datasets/, stored it in HDFS, and created a Hive table to view the data. You should be able to view the data in the Hive table and verify that it has been correctly loaded. If you have created a script to automate the process, you should be able to easily refresh the data in the Hive table as new data becomes available at the link.
