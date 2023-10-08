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
#     table1 =bigquery.Table("charming-shield-350913.ritu_dataset.{}".format(filename),schema1)
#
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

    spark = SparkSession.builder \
    .config('spark.jars.packages','com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-1.9.5,com.google.guava:guava:r05') \
    .master('local[*]').appName('spark-bigquery-demo').getOrCreate()

    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", key)
    spark.conf.set('temporaryGcsBucket', temp_buck)