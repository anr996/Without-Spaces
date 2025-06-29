from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark=SparkSession.builder.appName("test").master("local").getOrCreate()
data="dbfs:/FileStore/tables/aadharpancarddata.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df1=df.where(col('age')>40)
df2=df.where((df.Age>40) & (df.Salary>75000))
#display(df2)
df.createOrReplaceTempView("tab")
ndf=spark.sql("select * from tab where age >26 and salary >75000")
#display(ndf)
res=df.withColumn("Today",current_timestamp()).withColumn("Email",regexp_replace(col("Email"),"gmail","googlemail"))
#display(res)
from pyspark.sql.functions import trim
df3= df.withColumn("AadharCardNumber",regexp_replace("AadharCardNumber", "\s+", "")).withColumn("Name",regexp_replace("Name", "\s+", ""))
display(df3)
