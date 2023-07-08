import csv
import subprocess
import os

# Configuration
csv_link = "https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/national/totals/nst-est2019-alldata.csv"
local_file_path = "/home/bhushan/project3/nst-est2019-alldata.csv"
hdfs_directory = "/census/national/totals/2010-2019"
hdfs_file_path = f"{hdfs_directory}/file.csv"
hive_commands_file = "/home/bhushan/project3/hive_commands.hql"
hive_table = "my_table"
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
