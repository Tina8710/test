from __future__ import absolute_import
import json
import pprint
import subprocess
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from datetime import timedelta, datetime

spark = SparkSession.builder.appName('abc').getOrCreate()
#sc = pyspark.SparkContext()

# Current Date to pick correct folder
get_datetime_today = datetime.now()
get_datetime_yesterday = get_datetime_today + timedelta(days=-2)

# Use the Cloud Storage bucket for temporary BigQuery export data used
bucket = spark._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = spark._jsc.hadoopConfiguration().get('fs.gs.project.id')



## bigquery dataset
output_dataset = 'search_project'

## bigquery tables
output_table_item_impressed = 'item_impressed'
output_table_get_items = 'get_items'

## Output directory and files
folder_item_impressed = "gs://shpock-shubi/events_item_impressed/2/"+str(get_datetime_yesterday.year) + "/" + get_datetime_yesterday.strftime('%m') + "/" + get_datetime_yesterday.strftime('%d')
folder_get_items = "gs://shpock-shubi/events_get_items/2/"+str(get_datetime_yesterday.year) + "/" + get_datetime_yesterday.strftime('%m') + "/" + get_datetime_yesterday.strftime('%d')


# Output directory and files for item impressed
output_directory_item_impressed= folder_item_impressed.format(bucket)
output_files_item_impressed = output_directory_item_impressed + '/5*'

# Output directory and files for get_items
output_directory_get_items= folder_get_items.format(bucket)
output_files_get_items = output_directory_get_items + '/5*'

#schema_file="gs://shpock-shubi/events_item_clicked/schema_item_clicked.json"
#output_directory = 'gs://shpock-shubi/events_item_clicked/2/"2018/11/24'.format(bucket)




# load item_impressed
subprocess.check_call(
    'bq load --source_format CSV '
    '--autodetect '
    '--noreplace '
    '--skip_leading_rows=1 '
    '{dataset}.{table} {files}'.format(
        dataset=output_dataset, table=output_table_item_impressed, files=output_files_item_impressed).split())


# load get_items
subprocess.check_call(
    'bq load --source_format CSV '
    '--autodetect '
    '--noreplace '
    '--skip_leading_rows=1 '
    '{dataset}.{table} {files}'.format(
        dataset=output_dataset, table=output_table_get_items, files=output_files_get_items).split())


## crontba job
# /usr/lib/spark/bin/spark-submit /home/tb/bigquery_autoupdate.py


# new line