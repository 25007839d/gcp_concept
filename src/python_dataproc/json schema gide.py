from google.cloud import storage
from google.cloud import bigquery

if __name__ == '_main_':
    obj_client = bigquery.Client()


    obj_client1 = storage.Client.from_service_account_json(r"C:\Users\KAJAL\PycharmProjects\GCP-Project\charming-shield-350913-ecccf1d79c4b.json")

    bucket_name= "project_samp1"
    buck = obj_client1.get_bucket(bucket_name)

    filename = list(buck.list_blobs(prefix=""))
    for i in filename:
        print(i.name)


    # filename ="sam_tb1"
    filename =str(input("enter file name:"))
    ext = ".csv"

    schema1 = [
        bigquery.SchemaField("id","integer"),
        bigquery.SchemaField("name","string"),
        bigquery.SchemaField("branch","string")
    ]

    table1 =bigquery.Table("warm-alliance-352004.cloudproject1.{}".format(filename),schema1)


    table = obj_client.create_table(table1)

    print("syccessfully created :{}".format(table.table_id))

    load_conf = bigquery.LoadJobConfig(
        schema=schema1,
        skip_leading_rows=1,
    )

    # gcs_path = "gs://project_samp1/sam_tb1.csv"
    gcs_path = "gs://project_samp1/{}{}".format(filename,ext)

    load_job = obj_client.load_table_from_uri(
        project="warm-alliance-352004",
        source_uris=gcs_path,
        destination="cloudproject1.{}".format(filename),
        location="US",
        job_config=load_conf,
        job_id_prefix="loadcsv"
    )
    load_job.result()
#---------------------------------------------------------------------------------------
# path = "examples/src/main/resources/people.json"
peopleDF = spark.read.json(path)

# The inferred schema can be visualized using the printSchema() method
peopleDF.printSchema()
# root
#  |-- age: long (nullable = true)
#  |-- name: string (nullable = true)

# Creates a temporary view using the DataFrame
peopleDF.createOrReplaceTempView("people")

# SQL statements can be run by using the sql methods provided by spark
teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
teenagerNamesDF.show()
# +------+
# |  name|
# +------+
# |Justin|
# +------+

# Alternatively, a DataFrame can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string
jsonStrings = ['{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}']
otherPeopleRDD = sc.parallelize(jsonStrings)
otherPeople = spark.read.json(otherPeopleRDD)
otherPeople.show()






JSON representation

{
  "args": [
    string
  ],
  "jarFileUris": [
    string
  ],
  "fileUris": [
    string
  ],
  "archiveUris": [
    string
  ],
  "properties": {
    string: string,
    ...
  },
  "loggingConfig": {
    object (LoggingConfig)
  },

  // Union field driver can be only one of the following:
  "mainJarFileUri": string,
  "mainClass": string
  // End of list of possible types for union field driver.
}