import flask
from flask import jsonify
from flask_cors import CORS
import json
from snowflake.snowpark import Session
import socket
import time
from datetime import datetime


# Initializing flask app
app = flask.Flask(__name__)
# Adding cors to flask
CORS(app)

connection_parameters = {
  "account": "cp48682.central-india.azure",
  "user": "raks801",
  "password": "Raks@801",
  "role": "ACCOUNTADMIN",
  "warehouse": "COMPUTE_WH",
  "database": "SNOWFLAKE_SAMPLE_DATA",
  "schema": "TPCH_SF1"
}

session = Session.builder.configs(connection_parameters).create()

# current dateTime
now = datetime.now()
# convert to string
date_time_str = now.strftime("%Y-%m-%d %H:%M:%S")


# Controller-1
@app.route("/pem", methods=['GET'])
def get_pem_data():
    df=session.sql("select * from CUSTOMER")
    pandas_df = df.to_pandas()  # this requires pandas installed in the Python environment
    json_list = json.loads(json.dumps(list(pandas_df.T.to_dict().values())))
    return json_list

# Controller-2
@app.route("/key", methods=['POST'])
def get_key_data():
    data = flask.request.data
    body = json.loads(data)
    key_to_query = body["key"]
    df=session.sql("select * from CUSTOMER where C_CUSTKEY="+key_to_query)
    pandas_df = df.to_pandas()  # this requires pandas installed in the Python environment
    if len(pandas_df)>0:
        json_list = json.loads(json.dumps(list(pandas_df.T.to_dict().values())))
    else:
        json_list="Key {} not found".format(key_to_query)
    return json_list

# Running the api
if __name__ == '__main__':
    app.run()