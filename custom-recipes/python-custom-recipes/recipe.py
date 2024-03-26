import os
import sys
import json
import dataiku
import urllib.parse as urlparse
from dataiku.customrecipe import *
import pysqream.pysqream as pysqream


# Settings
DATASET_IN = get_input_names_for_role("input_dataset")[0]
DATASET_OUT = get_output_names_for_role("output_dataset")[0]

AWS_ACCESS_KEY = get_recipe_config().get("aws_access_key")
AWS_SECRET_KEY = get_recipe_config().get("aws_secret_key")

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    print("$" * 40)
    # Looking up in Project Variables
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

# Input dataset settings


config = ds.get_config()
print(f' config {config}')
# check what files need to be supported
file_formate = 'csv'
if config["formatType"] != 'csv':
    print("[-] Only a CSV format for the input DSS Dataset is supported (you used {}).".format(config["formatType"]))
    print("[-] Please adjust the format. Aborting")
    sys.exit("Format error (CSV needed)")

project_key = config["projectKey"]

# Actual path of the input file on S3
bucket = config["params"]["bucket"]
path = config["params"]["path"].replace("${projectKey}", config["projectKey"])
full_path = "s3://{}{}".format(bucket, path)
print("!" * 40)
print(f"full_path {full_path} ")
# Input file definition
separator = config["formatParams"]["separator"]
skip_rows = config["formatParams"]["skipRowsBeforeHeader"]

# ------------------------------------------------------------------------------
# OUTPUT DATASET SETTINGS
# ------------------------------------------------------------------------------

# Output configuration
config = out.get_location_info(sensitive_info=True)

# sqreamd credentials & output table
print(config["info"])
print("%" * 40)
# jdbc_url = config["info"]["connectionParams"]["jdbcurl"]
# components = urlparse.parse_qs(urlparse.urlparse(jdbc_url).query)

# sqream_user = components["user"][0]
# sqream_password = components["password"][0]
# sqream_database = components["db"][0]
# sqream_schema = components["schema"][0]
# sqream_warehouse = components["warehouse"][0]
# sqream_host = components["host"]
# sqream_port = components["port"]
# sqream_clustered = components["clustered"]
# sqream_account   = urlparse.urlparse(jdbc_url).path.replace("sqreamd://", "").replace(".sqreamdcomputing.com", "")

sqream_user = 'sqream'
sqream_password = 'sqream'
sqream_database = 'master'
sqream_host = '192.168.4.69'
sqream_port = '3108'
sqream_clustered = True

# output_table = config["info"]["table"].replace("${projectKey}", project_key)
output_table = "dataiku_table"
# ------------------------------------------------------------------------------
# BULK LOADING TO sqreamd
# ------------------------------------------------------------------------------


cnx = pysqream.connect(
    host='192.168.4.69',
    port=int('3108'),
    username='sqream',
    password='sqream',
    database='master',
    clustered=True
)
# cnx = pysqream.connect(
#     host=sqream_host,
#     port=sqream_port,
#     username=sqream_user,
#     password=sqream_password,
#     database=sqream_database,
# )
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
    'array': 'text',
}

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
q = """ CREATE OR REPLACE TABLE \"{}\" ({})""".format(output_table, schema_out)
print(f"create statment -->> {q}")
cur.execute(q)

print("[+] Create a foreign table ...")
q = f"""CREATE OR REPLACE FOREIGN TABLE f_{output_table} ({schema_out}) wrapper {file_formate}_fdw options 
(location = '{full_path}', AWS_ID = '{AWS_ACCESS_KEY}', AWS_SECRET = '{AWS_SECRET_KEY}', DELIMITER  = '|' )"""
print(f"foreign table create -->> {q}")
cur.execute(q)

print("[+] Loading data...")
q = f"""INSERT INTO {output_table} SELECT *  FROM f_{output_table} """
print(q)
cur.execute(q)

# Write recipe outputs
out.write_schema(ds.read_schema())
print("[+] Loading Done")