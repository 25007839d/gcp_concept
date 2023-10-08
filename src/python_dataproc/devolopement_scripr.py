import findspark
findspark.init()
from pyspark.sql.types import StructField, IntegerType,StringType,StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import *





if __name__ == '__main__':

     spark = SparkSession.builder\
        .master("local[1]")\
        .appName("SparkByExamples.com")\
        .getOrCreate()

     schema = StructType([StructField("name",StringType()),
                              StructField("email", StringType()),
                              StructField("age",IntegerType()),
                              # StructField("city", StringType()),
                              # StructField("state", StringType()),
                              # StructField("country", StringType()),
                              # StructField("age_s", IntegerType()),
                              StructField("bad_record", StringType())
                              ])


     adf= spark.read.csv(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata", schema=schema)
     adf.show()

     adf.na.fill({'name':'abc','email':'er.xxxxx','age':'18','bad_record':'bad'})\
    .replace('null','188').replace('null','187').replace('null','bad').show()


     adf.na.fill(value='100', subset=["age"]).fillna(value='bbnbn', subset=["email"]).show()

# PERMISSIVE, DROPMALFORMED, FAILFAST
#      df = spark.read \
#          .option("badRecordsPath", r"C:\Users\KAJAL\PycharmProjects\GCP-Project\config\jsonschema\123.txt") \
#          .option("mode", "DROPMALFORMED") \
#          .option('columnNameOfCorruptRecord', "bad_record") \
#          .option("delimiter", ",") \
#          .csv(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata", schema=schema).drop(col("bad_record"))
#      df.show()
#
# # PERMISSIVE, DROPMALFORMED, FAILFAST
#      df = spark.read\
#          .option("badRecordsPath",r"C:\Users\KAJAL\PycharmProjects\GCP-Project\config\jsonschema\123.txt")\
#          .option("mode", "PERMISSIVE") \
#          .option('columnNameOfCorruptRecord',"bad_record")\
#          .option("delimiter",",")\
#          .csv(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata",schema=schema,header=True).drop(col('bad_record'))
#      df.show()
#      df.printSchema()

     import pyspark.sql.functions as F
     # df_spark1 =df.dropna(str="any")



##data type change od column
     # df1=df.withColumn("age_int",col("age").cast("Integer"))




# column name special char eliminate and any change
     # from pyspark.sql import SparkSession
     # import re
     # #
     # spark = SparkSession.builder.getOrCreate()
     # df = spark.createDataFrame([(1, 2, 3, 4)], [' #1', '%2', ',g3', '(4)'])
     # df.show()
     # df1 = df.toDF(*[re.sub('[^\w]', ' ', c) for c in df.columns])
     # df1.show()
     # df_spark1 = df1.select([F.col(col).alias(col.replace('[^\w]', '_')) for col in df1.columns])
     # df_spark1.show()




#
     import json
#
# # Opening JSON file to distonary
#      f = open(r'C:\Users\KAJAL\PycharmProjects\GCP-Project\config\jsonschema\department.json','r')
#      myjson = f.read()
#      aList = json.loads(myjson)
#      print(aList)
#      schemaFromJson = StructType.fromJson(json.loads(myjson))
#
#      df3 = spark.createDataFrame(spark.sparkContext.csv(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata",schema=aList))
#      df3.printSchema()
#Opening distonary to JSON file
     # import json
     #
     # aDict = {"a": 54, "b": 87}
     # jsonString = json.dumps(aDict)
     # jsonFile = open("data.json", "w")
     # jsonFile.write(jsonString)
     # jsonFile.close()

    # schemaFromJson = StructType.fromJson(json.loads(department.json))
    # df3 = spark.createDataFrame(spark.sparkContext.parallelize(structureData), schemaFromJson)
    # df3.printSchema()


    # # UDF ----------------------------------------------------------------------------------
    #  df = spark.read.csv(r"C:\Users\KAJAL\Desktop\Brainwork\Cloud\PCM PROJECT\rawdata",header=True)
    #  df1 = df.withColumn("age_int", col("Age").cast("Integer"))
    #  df1.show()
    #  # upper case UDF
    #  def upperCase(str):
    #      return str.upper()
    #
    #  upperCaseUDF = udf(lambda z: upperCase(z), StringType())
    #
    #  df.withColumn("Cureated Name", upperCaseUDF(col("Name"))).drop(col('name'))\
    #  .show(truncate=False)
    #  #Age filter
    #
     # def upperCase(x):
     #     if x >=28:
     #         return x
     #     else:
     #         return 0
     # 
     # upperCaseUDF1 = udf(lambda z: upperCase(z))
     # 
     # df1.withColumn("Age_ok", upperCaseUDF1(col("age_int"))).show()

     # to special character
     # def special(x):
     #    a=''
     #
     #    for i in x:
     #
     #       if i in codes:
     #
     #            continue
     #
     #       else:
     #           a=a+i
     #           print(a)
     #    return a
     #
     #
     # codes = ['(', '%','$', '&', '*','~','@','^',')','-','/','#','\\','null',' ']
     #
     #
     # spchar= udf(lambda z: special(z),StringType())
     # df1.withColumn("name", spchar(col("name"))).withColumn('email',spchar(col('email')))   .show()

     # df1.dropna('any').show()




import pyspark.sql.functions as F
#
# df1 =df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])
# df_spark1 = df_spark.select([F.col(col).alias(col.replace('%', '_')) for col in df_spark.columns])
# df_spark = df_spark1.select([F.col(col).alias(col.replace(',', '_')) for col in df_spark1.columns])
# df_spark1 = df_spark.select([F.col(col).alias(col.replace('(', '_')) for col in df_spark.columns])
# df_spark2 = df_spark1.select([F.col(col).alias(col.replace(')', '_')) for col in df_spark1.columns])

# import pyspark.sql.functions as F
# import re
#
# df = df.select([F.col(col).alias(re.sub("[^0-9a-zA-Z$]+","",col)) for col in df.columns])


# from pyspark.sql import SparkSession
# import re
#
# spark = SparkSession.builder.getOrCreate()
# df = spark.createDataFrame([(1, 2, 3, 4)], [' 1', '%2', ',3', '(4)'])
#
# df = df.toDF(*[re.sub('[^\w]', '_', c) for c in df.columns])
# df.show()

