from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

from pyspark.sql import Window
from pyspark.sql.functions import rank, col
import pyspark.sql.functions as F 

#read csv
df = (spark
  .read                                              
  .option("inferSchema","true")                 
  .option("header","true")                           
  .csv("rating.csv"))

#fuctions
def nb_previous_ratings(df):
  df=df.withColumn("nb_previous_ratings",rank().over(Window.partitionBy("userId").orderBy("timestamp")) -1 )
  return df

def avg_ratings_previous(df):
  WindowSpec = Window.partitionBy("userId").rowsBetween(Window.unboundedPreceding,-1)

  df = df.withColumn("avg_ratings_previous", F.avg(F.col("rating")).over(WindowSpec))
  df = df.orderBy("userId", "timestamp")
  return df

#feature enginering
df = nb_previous_ratings(df)
df = avg_ratings_previous(df)

#write csv
df.repartition(1).write.format('com.databricks.spark.csv').save("./process/",header = 'true')
