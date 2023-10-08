# Imports the Google Cloud client library
from google.cloud import storage
import os
os.environ["GCLOUD_PROJECT"]="ritu-351906"

# Instantiates a client
storage_client = storage.Client()

# The name for the new bucket
bucket_name = "pcm_dev-ingestion4"

# Creates the new bucket
# bucket = storage_client.create_bucket(bucket_name)
#
# print(f"Bucket {bucket.name} created.")

delbucket = storage_client.list_buckets(project='ritu-351906')
for i in delbucket:
  print(i)

storage_client = storage.Client()
bucket = storage_client.bucket("pcm_dev-ingestion1")
blob = bucket.blob('cloud_assingment.txt')
#
blob.upload_from_filename(r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata\department_2022_05_21.csv")

storage_client = storage.Client()

bucket = storage_client.bucket(bucket_name)

# Construct a client side representation of a blob.
# Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
# any content from Google Cloud Storage. As we don't need additional data,
# using `Bucket.blob` is preferred here.
blob = bucket.blob("pcm_dev-ingestion1")
blob.download_to_filename("gs://pcm_dev-ingestion1/data/my_sql/data.csv")

# print(
#     "Downloaded storage object {} from bucket {} to local file {}.".format(
#         source_blob_name, bucket_name, destination_file_name
#     )
# )



