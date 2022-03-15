from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql.functions import explode,year,month,dayofmonth,to_date,split


spark=SparkSession.builder\
    .config("spark.jars","/home/saif/LFS/cohort_c9/jars/mysql-connector-java-8.0.28.jar")\
    .getOrCreate()

df=spark.read.format("csv").option("header","true").option("inferschema","true")\
    .load("file:///home/saif/LFS/cohort_c9/datasets/txns")

df1=df.withColumn("year",year(to_date("txndate",'MM-dd-yyyy'))).withColumn("month",month(to_date("txndate",'MM-dd-yyyy'))).withColumn("day",dayofmonth(to_date("txndate",'MM-dd-yyyy')))

df2=df1.filter(df1.category == 'Exercise & Fitness')
df3=df2.withColumn("New_ex",split(df2.category," & ")[0]).withColumn("New_Fit",split(df2.category," & ")[1])
df3.show()

df3.write.format("jdbc").mode("overwrite") \
    .option("url", "jdbc:mysql://localhost:3306/retail_db?useSSL=False") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("user", "root") \
        .option("password", "Welcome@123") \
        .option("dbtable", "airflow").save()