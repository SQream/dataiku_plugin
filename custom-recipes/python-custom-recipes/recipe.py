import os
import sys
import json
import dataiku
from urllib.parse import urlparse
from dataiku.customrecipe import *
import pysqream.pysqream as pysqream

# Settings
DATASET_IN = get_input_names_for_role("input_dataset")[0]
DATASET_OUT = get_output_names_for_role("output_dataset")[0]

AWS_ACCESS_KEY = get_recipe_config().get("aws_access_key")
AWS_SECRET_KEY = get_recipe_config().get("aws_secret_key")

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    print("[+] AWS Access Key or Secret Key not entered in the Plugin interface. Looking into Project Variables...")
    dss = dataiku.api_client()
    project = dss.get_project(dataiku.default_project_key())
    variables = project.get_variables()["standard"]
    if "sqreamd" in variables:
        if "aws_access_key" in variables["sqreamd"] and "aws_secret_key" in variables["sqreamd"]:
            print("[+] Found AWS credentials in Project Variables")
            AWS_ACCESS_KEY = variables["sqreamd"]["aws_access_key"]
            AWS_SECRET_KEY = variables["sqreamd"]["aws_secret_key"]
        else:
            print(
                "[-] sqreamd key found in Project Variables but can not retrieve aws_access_key and/or aws_secret_key.")
            print("[-] Please check and correct your Project Variables.")
            sys.exit("Project Variables error")
    else:
        # Looking into Global Variables
        variables = dss.get_variables()
        if "sqreamd" in variables:
            if "aws_access_key" in variables["sqreamd"] and "aws_secret_key" in variables["sqreamd"]:
                print("[+] Found AWS credentials in Global Variables")
                AWS_ACCESS_KEY = variables["sqreamd"]["aws_access_key"]
                AWS_SECRET_KEY = variables["sqreamd"]["aws_secret_key"]
        else:
            print(
                "[-] sqreamd key found in Global Variables but can not retrieve aws_access_key and/or aws_secret_key.")
            print("[-] Please check and correct your Global Variables.")
            sys.exit("Global Variables error")

# Dataiku Datasets
ds = dataiku.Dataset(DATASET_IN)
out = dataiku.Dataset(DATASET_OUT)

# ------------------------------------------------------------------------------
# INPUT DATASET SETTINGS
# ------------------------------------------------------------------------------

config = ds.get_config()

# check what files need to be supported

if config["formatType"] != 'csv' and config["formatType"] != 'json' and config["formatType"] != 'avro' and config["formatType"] != 'json':
    print("[-] Only a CSV, json, avro format for the input DSS Dataset is supported (you used {}).".format(
        config["formatType"]))
    print("[-] Please adjust the format. Aborting")
    sys.exit("Format error (CSV or json or avro needed)")
file_formate = config["formatType"]
project_key = config["projectKey"]

print(f"file formate {file_formate}")
# Actual path of the input file on S3
bucket = config["params"]["bucket"]
path = config["params"]["path"].replace("${projectKey}", config["projectKey"])
full_path = "s3://{}{}".format(bucket, path)

# Input file definition for CSV
if file_formate == "csv":
    separator = config["formatParams"]["separator"]
    skip_rows = config["formatParams"]["skipRowsBeforeHeader"]

# ------------------------------------------------------------------------------
# OUTPUT DATASET SETTINGS
# ------------------------------------------------------------------------------

# Output configuration
config = out.get_location_info(sensitive_info=True)

# sqreamd credentials & output table
jdbc_url = config["info"]["connectionParams"]["jdbcurl"]


parsed_url = urlparse(jdbc_url)
# Get the path component
path_components = parsed_url.path
# Split the path component by '/' and ';' to extract parameters
parameters = [component.split('=', 1) for component in path_components.split(';') if '=' in component]
# Convert the parameters into a dictionary
parameters_dict = {param[0]: param[1] if len(param) > 1 else None for param in parameters}
# Extract the value between '/' and ';' as 'database' key

url_parts = jdbc_url.split('//')
host_port_parts = url_parts[1].split("/")
host = host_port_parts[0].split(":")[0]
port = int(host_port_parts[0].split(":")[1])

# Extract the database name
database_name = host_port_parts[1].split(";")[0]
# Extract the database name

# Add 'database' key and its value to the parameters_dict
parameters_dict['database'] = database_name
parameters_dict['host'] = host
parameters_dict['port'] = port

sqream_user = parameters_dict["user"]
sqream_password = parameters_dict["password"]
sqream_database = parameters_dict["database"]
sqream_host = parameters_dict["host"]
sqream_port = parameters_dict["port"]
sqream_clustered = parameters_dict["cluster"]


output_table = config["info"]["table"].replace("${projectKey}", project_key)
print(f"output_table -->>> {output_table}")
# ------------------------------------------------------------------------------
# BULK LOADING TO sqreamd
# ------------------------------------------------------------------------------

cnx = pysqream.connect(
    host=sqream_host,
    port=sqream_port,
    username=sqream_user,
    password=sqream_password,
    database=sqream_database,
    clustered=bool(sqream_clustered)
)
cur = cnx.cursor()

# Building schema
fieldSetterMap = {
    'boolean': 'BOOLEAN',
    'tinyint': 'SMALLINT',
    'smallint': 'SMALLINT',
    'int': 'INTEGER',
    'bigint': 'BIGINT',
    'float': 'FLOAT',
    'double': 'FLOAT',
    'date': 'text',
    'string': 'text',
}
if file_formate == 'json' or 'avro':
    print("[+] Create a foreign table ...")
    q = f"""CREATE OR REPLACE FOREIGN TABLE f_{output_table} wrapper {file_formate}_fdw options 
        (location = '{full_path}', AWS_ID = '{AWS_ACCESS_KEY}', AWS_SECRET = '{AWS_SECRET_KEY}' )"""
    cur.execute(q)
    q = f""" CREATE OR REPLACE TABLE "{output_table}" as select * from f_{output_table}"""
    print(f"create statment -->> {q}")
    cur.execute(q)
    print(f"ds.read_schema()  {ds.read_schema()}")
    out.write_schema(ds.read_schema())
else:
    schema = []
    print(ds.read_schema())
    for column in ds.read_schema():
        print(f"column info -> {column}")
        _name = column["name"]
        _type = fieldSetterMap.get(column["type"], "VARCHAR")
        s = "\"{}\" {}".format(_name, _type)
        schema.append(s)

    schema_out = ", ".join(schema)

    print(f"table ddl {schema_out}")
    # Actual sqreamd bulkload
    print("[+] Create target table ...")
    q = f""" CREATE OR REPLACE TABLE {output_table} ({schema_out})"""
    print(f"create statment -->> {q}")
    cur.execute(q)

    print("[+] Create a foreign table ...")
    q = f"""CREATE OR REPLACE FOREIGN TABLE f_{output_table} ({schema_out}) wrapper {file_formate}_fdw options 
    (location = '{full_path}', AWS_ID = '{AWS_ACCESS_KEY}', AWS_SECRET = '{AWS_SECRET_KEY}' )"""
    q_csv = f"""CREATE OR REPLACE FOREIGN TABLE f_{output_table} ({schema_out}) wrapper {file_formate}_fdw options 
    (location = '{full_path}', AWS_ID = '{AWS_ACCESS_KEY}', AWS_SECRET = '{AWS_SECRET_KEY}', DELIMITER  = '|' )"""
    print(f"foreign table create -->> {q_csv}")
    cur.execute(q_csv)

    print("[+] Loading data...")
    q = f"""INSERT INTO {output_table} SELECT *  FROM f_{output_table} """
    print(q)
    cur.execute(q)

    # Write recipe outputs
    # out.write_schema(ds.read_schema())
    print("[+] Loading Done")
    print(ds.read_schema())
