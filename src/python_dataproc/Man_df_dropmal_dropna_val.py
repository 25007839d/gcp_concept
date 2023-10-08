import findspark
findspark.init()
from pyspark.sql.types import StructField, IntegerType,StringType,StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder\
        .master("local[1]")\
        .appName("SparkByExamples.com")\
        .getOrCreate()

schema = StructType([StructField("name",StringType()),
                              StructField("email", StringType(),True),
                              StructField("age",IntegerType()),
                              # StructField("city", StringType()),
                              # StructField("state", StringType()),
                              # StructField("country", StringType()),
                              # StructField("age_s", IntegerType()),
                              StructField("bad_record", StringType())
                              ])



def null_dropmalformed(link):


# PERMISSIVE, DROPMALFORMED, FAILFAST
# null validation of integer col or already string have null value
     df = spark.read \
         .option("mode", "DROPMALFORMED") \
         .option('columnNameOfCorruptRecord', "bad_record") \
         .csv(link, schema=schema,header=True).drop(col("bad_record"))

# again empty value validation with nlldrop(col("bad_record"))
     df1 = df.dropna("any")
     return df1


def null_PERMISSIVE_data(link):

    # PERMISSIVE, DROPMALFORMED, FAILFAST
    # null validation of integer col or already string have null value
    df = spark.read \
             .option("badRecordsPath","bad_data.txt")\
             .option("mode", "PERMISSIVE") \
             .option("delimiter",",")\
             .csv(link,schema=schema,header=True)


    return df

def manual_df(link):
    df = spark.read.csv(link, schema=schema,header=True).drop(col("bad_record"))
    return df


# # # PERMISSIVE, DROPMALFORMED, FAILFAST
#      df = spark.read\
#          .option("badRecordsPath","bad_data.txt")\
#          .option("mode", "PERMISSIVE") \
#          .option('columnNameOfCorruptRecord',"bad_record")\
#          .option("delimiter",",")\
#          .csv(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata",schema=schema,header=True)
#      df.show()
