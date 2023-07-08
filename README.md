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
## Source code(hadoop_hive.py)
```python 
import csv
import subprocess
import os

# Configuration
csv_link = "link for the file to be inserted to the hdfs directory"
local_file_path = "local path for your the file to be saved"
hdfs_directory = "hadoop directory path where you want to put the csv file"
hdfs_file_path = f"{hdfs_directory}/file.csv"
hive_commands_file = "path to the hive commands file in which hive commands are to saved"
hive_table = "your hive table name"
field_separator = ","

# Step 1: Download CSV file using wget
wget_command = f"wget {csv_link} -O {local_file_path}"
subprocess.run(wget_command, shell=True)

# Step 2: Create HDFS directory
hdfs_mkdir_command = f"hdfs dfs -mkdir -p {hdfs_directory}"
subprocess.run(hdfs_mkdir_command, shell=True)

# Step 3: Copy local file to HDFS
hdfs_delete_command = f"hdfs dfs -rm {hdfs_file_path}"
subprocess.run(hdfs_delete_command, shell=True)
hdfs_put_command = f"hdfs dfs -put {local_file_path} {hdfs_file_path}"
subprocess.run(hdfs_put_command, shell=True)

# Step 4: Create Hive commands file
with open(hive_commands_file, "w") as file:
    file.write(f"CREATE EXTERNAL TABLE IF NOT EXISTS {hive_table} (\n")
    with open(local_file_path, "r") as csv_file:
        reader = csv.reader(csv_file)
        header = next(reader)
        column_definitions = ', '.join(f'{column} STRING' for column in header)
        file.write(f"{column_definitions}\n")
    file.write(f")\n")
    file.write(f"ROW FORMAT DELIMITED\n")
    file.write(f"FIELDS TERMINATED BY '{field_separator}'\n")
    file.write(f"STORED AS TEXTFILE\n")
    file.write(f"LOCATION '{hdfs_directory}';\n")
    file.write(f"LOAD DATA INPATH '{hdfs_file_path}' INTO TABLE {hive_table};\n")

# Step 5: Launch Hive shell and execute commands from file
hive_shell_command = f"hive -f {hive_commands_file}"
subprocess.run(hive_shell_command, shell=True)
```
## Screenshots
### hive_commands.hql and hadoop_hive.py in the project_3 folder
![project3folder](https://github.com/bnaikwad88/Dynamic-Data-Ingestion-Storage-HDFS-/assets/116859424/fb8cdc9e-e29d-45dd-9196-f616039b493a)

### hadoop up and running
![hdfs_up](https://github.com/bnaikwad88/Dynamic-Data-Ingestion-Storage-HDFS-/assets/116859424/4780b3a1-1387-4200-a837-fb2059b34c7f)

### hadoop web UI(file not inserted yet)
![hdfs_web](https://github.com/bnaikwad88/Dynamic-Data-Ingestion-Storage-HDFS-/assets/116859424/85b45049-2fe8-40be-8158-be913be1f0d1)

### hadoop_hive.py results
![hadoop_hive_results](https://github.com/bnaikwad88/Dynamic-Data-Ingestion-Storage-HDFS-/assets/116859424/953ba91d-5e57-45ad-93eb-43b80888a3f8)

### file downloaded to local
![allfiles](https://github.com/bnaikwad88/Dynamic-Data-Ingestion-Storage-HDFS-/assets/116859424/c6292f82-95b5-49e6-bac2-25ee769bbee8)

### file uploaded to hadoop
![hdfs_file_inserted](https://github.com/bnaikwad88/Dynamic-Data-Ingestion-Storage-HDFS-/assets/116859424/ac4ce7cb-fa2d-4e06-9b70-accd02aa7234)

### my_table in hive database queried
![hive_data_inserted](https://github.com/bnaikwad88/Dynamic-Data-Ingestion-Storage-HDFS-/assets/116859424/aa0c1bc6-3213-49c0-b0cb-72ba42ce9202)



