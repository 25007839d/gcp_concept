import findspark
from google.cloud.storage import blob
from google.cloud import bigquery

findspark.init()
from pyspark.sql.types import StructField, IntegerType,StringType,StructType
from pyspark.sql import SparkSession
import logging
from google.cloud import storage,dataproc
import os
os.environ["GCLOUD_PROJECT"]="ritu-351906"  # not required here if we given service account




if __name__ == '__main__':

     spark = SparkSession.builder\
        .master("local[1]") \
        .appName("SparkByExamples.com")\
        .getOrCreate()


# create file record logger program


     pcm_logger = logging.getLogger("pcm.log")
     pcm_logger.setLevel(logging.DEBUG)
     pcm = logging.FileHandler("pcm.txt")
     pcm.setLevel(logging.DEBUG)
     pcm_formater = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
     pcm.setFormatter(pcm_formater)
     pcm_logger.addHandler(pcm)

     logger = logging.getLogger('simple_example')
     logger.setLevel(logging.DEBUG)
     ch = logging.FileHandler(r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\SCD PROJECT\Dushyant\SCD.log")
     ch.setLevel(logging.DEBUG)
     formatter = logging.Formatter('%(message)s')
     ch.setFormatter(formatter)
     logger.addHandler(ch)

     # gcs clint make for onpremises

     obj_clint = storage.Client.from_service_account_json(r"C:\Big Data\ritu-351906-27e10a6678af.json")
     #
     spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",r"C:\Big Data\ritu-351906-27e10a6678af.json")
     #
     path = "gs://pcm_dev-ingestion/data/my_sql/data"
     df= spark.read.csv(r"gs://pcm_dev-ingestion1/data/my_sql/*.csv", header=True)
     df.show()

#create data frame in auto-----------------------------------------------------------------
     # from Auto_schema_col_val import Autodf
     #
     # a = Autodf(r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata\department_2022_05_21.csv").create_df_auto()
     #
     # a.show()
# #
# replacing col name gape with '_'


     # df1=a.rep_any_df(' ','_')
     # df1.show()

# # manula schema df call------------------------------------------------------------------------
#
#      from Man_df_dropmal_dropna_val import manual_df
#
#      manualdf = manual_df(r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata\data.csv")
#
#
# # null value validation with dropna and dropmal---------------------------------------------------------------------
#      from Man_df_dropmal_dropna_val import null_dropmalformed
#
#      df = null_dropmalformed(r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata\data.csv")
#
#      pcm_logger.info("data frame created and null value validation done")
#
#      print("null valdation")
#
#
#
# # only null validation by dropna df-drop the null column-----------------------------------------------------------------
# ##########################################################################################
#      from dropna_null_val import Nullremove
#      a = Nullremove(df)
#      a.null_val()
#
#
#
# # special char validation----------------------------------------------------------------------
#      from special_char import CharValidation
#
# # function call in udf lambda
#
#
#      spchar = udf(lambda z: CharValidation.specialcharvalidation(z), StringType())
#      df2 = df.withColumn("name", spchar(col("name"))).withColumn('email', spchar(col('email')))\
#           .where(col("name") != '').where(col("email") != '')
#      df2.show() # add where col for filter null value
#
#      df3= df2.dropna('any')
#      df3.show()
#
#      print("special valdation")
#
#
# #filter condition if we want -------------------------------------------------------------------
#
#      # Replace empty string with None value
#      # from pyspark.sql.functions import col, when
# #null value filter after specal validation
#      # df4 =df3.where(df3.name != '').where(df3.email != '').show()
#
#
#      # print('none value add')
#
# #all / whatever we want null value replacement in all column ------------------------------------
#      # df4=df3.na.fill({'name': 'abc', 'email': 'er.xxxxx', 'age': '18'}) \
#      #     .replace('null', '188').replace('null', '187').replace('null', 'bad')  # method 1
#
#      # df3.na.fill(value='100', subset=["age"]).fillna(value='bbnbn', subset=["email"]).show() # metod 2 only computer null change
#      # df3.show()
#
# #  bad data collect --------------------------------------------------------------------------
#      ##PERMISSIVE, DROPMALFORMED, FAILFAST
#
#      bdf1= manualdf.join(df3,"name", "leftanti")  # join the filter df and manual df
#      bdf1.show()
#      print("join bad data")














# bad data record write on gcs
#      df.write.mode("append").option('header',True).csv("gs://pcm_dev-ingestion1/bad_data/")

     # # print("write data on gcs")
     # #

# big query setup to load data frame

     '''code for handle defeculty in connection and ensure jar connector should be = to spark version
     # credentals_path =r"C:\Big Data\ritu-351906-27e10a6678af.json"
     # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentals_path
     # client = bigquery.Client()
     # table_id = 'ritu-351906.bwt_session1.try'

     # spark = SparkSession.builder \
     #      .config('spark.jars.packages',
     #              'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-1.9.5,com.google.guava:guava:r05') \
     #      .master('local[*]').appName('spark-bigquery-demo').getOrCreate()
     # spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", r"C:\Big Data\ritu-351906-27e10a6678af.json")
    
     '''

     bucket = "pcm_dev-ingestion1"
     spark.conf.set('temporaryGcsBucket', bucket)


     # df.write.format('bigquery') \
     # .option('table', 'ritu-351906:bwt_session.try5') \
     # .save()

     df.write.format('bigquery') \
          .option('table', 'ritu-351906:bwt_session.try6') \
          .mode('append').save()



