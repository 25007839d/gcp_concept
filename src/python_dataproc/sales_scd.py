import findspark
findspark.init()
from pyspark.sql.types import StructField, IntegerType,StringType,StructType,DateType
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
import logging
from google.cloud import storage
from google.cloud import bigquery




if __name__ == '__main__':

     spark = SparkSession.builder\
        .master("local[1]")\
        .appName("SparkByExamples.com")\
        .getOrCreate()

     from Man_df_dropmal_dropna_val import null_dropmalformed

     dff = spark.read.csv(r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\SCD PROJECT\Ritu\sales_record_2022_05_21.csv",header=True)
     dff.show()

     print("data frame created and null value validation done")

     print("null valdation")

     from Auto_schema_col_val import Autodf


     # replacing col name gape with '_'

     a=Autodf(r"C:\Users\KAJAL\OneDrive\Desktop\Brainwork\Cloud\SCD PROJECT\Ritu\sales_record_2022_05_21.csv")
     df=a.rep_any_df(' ','_')
     df.show()



# only null validation by dropna df-drop the null column-----------------------------------------------------------------
##########################################################################################
     # from dropna_null_val import Nullremove
     # a = Nullremove(df)
     # a.null_val()



# special char validation----------------------------------------------------------------------
     from special_char import CharValidation
#
# # function call in udf lambda
#
#
     spchar = udf(lambda z: CharValidation.charfindreplace(z,"/","-"), StringType())
     df2 = df.withColumn("Order_Date", spchar(col("Order_Date")))

     df2.show() # add where col for filter null value

     spchar = udf(lambda z: CharValidation.date(z), StringType())
     df3 = df2.withColumn("Order_Date", spchar(col("Order_Date")))
     df4 =df3.withColumn("Order_Date",to_date(col("Order_Date"),"MM-dd-yyyy"))

     wind = Window.partitionBy('Item_Type').orderBy(col("Order_Date").desc())
     fdf = df4.withColumn("row_num", row_number().over(wind)).where(col('row_num')==1)
     fdf.show()




