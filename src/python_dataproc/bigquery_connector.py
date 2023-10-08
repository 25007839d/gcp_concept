from google.cloud import bigquery
import os
import findspark
findspark.init()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

credentals_path = r'C:\Users\KAJAL\PycharmProjects\GCP-Project1\python_dataproc\prpate_key.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentals_path
client = bigquery.Client()
table_id ='ritu-351906.bwt_session2.data'

# client.create_table(table_id)
from Man_df_dropmal_dropna_val import manual_df

df = manual_df(r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata\data.csv")
df.show()

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
)

with open(file= r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata\data.csv",mode='rb') as source_file:
    bigquery.source_file =r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata\data.csv"
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)


job.result()  # Waits for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)

def implicit():
    from google.cloud import storage

    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    storage_client = storage.Client()

    # Make an authenticated API request
    buckets = list(storage_client.list_buckets())
    print(buckets)

a = implicit()



# from google.cloud import storage
# from google.cloud import bigquery
#
# if __name__ == '_main_':
#     obj_client = bigquery.Client()
#
#
#     obj_client1 = storage.Client.from_service_account_json(r"C:\Big Data\charming-shield-350913-2a1c8524ef6c.json")
#
#     bucket_name= "pcm_dev-ingestion"
#     buck = obj_client1.get_bucket(bucket_name)
#
#     filename = list(buck.list_blobs(prefix=""))
#     for i in filename:
#         print(i.name)
#
#
#     # filename ="sam_tb1"
#     filename =str(input("enter file name:"))
#     ext = ".csv"
#
#     schema1 = [
#         bigquery.SchemaField("id","integer"),
#         bigquery.SchemaField("name","string"),
#         bigquery.SchemaField("branch","string")
#     ]
#
20#
#
#     table = obj_client.create_table(table1)
#
#     print("syccessfully created :{}".format(table.table_id))
#
#     load_conf = bigquery.LoadJobConfig(
#         schema=schema1,
#         skip_leading_rows=1,
#     )
#
#     # gcs_path = "gs://project_samp1/sam_tb1.csv"
#     gcs_path = "gs://pcm_dev-ingestion/{}{}".format(filename,ext)
#
#     load_job = obj_client.load_table_from_uri(
#         project="charming-shield-350913",
#         source_uris=gcs_path,
#         destination="cloudproject1.{}".format(filename),
#         location="US",
#         job_config=load_conf,
#         job_id_prefix="loadcsv"
#     )
#     load_job.result()