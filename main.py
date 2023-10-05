# -*- coding: utf-8 -*-
"""
Created on Mon Oct  2 11:17:18 2023

@author: Akshay.Nimbalkar
"""

from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.cloud import storage
from io import StringIO
import os

import os
print(os.getcwd())
app = Flask(__name__)

# Construct a BigQuery client object.
client = bigquery.Client()


@app.route('/')
def main(client = client):
    
    table_id = "gcp-mlops-sidak.test_schema.us_states"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("post_abbr", "STRING"),
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    uri = "gs://ak-mlops/us-states.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    return {"data": destination_table.num_rows}

if __name__ == '__main__':
    app.run('0.0.0.0')
       