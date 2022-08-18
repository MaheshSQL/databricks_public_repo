# Databricks notebook source
# MAGIC %md
# MAGIC **Create a Widget to Enter S3 Bucket Name**

# COMMAND ----------

dbutils.widgets.text("BucketName",defaultValue = "",label="Enter Bucket Name")

# COMMAND ----------

# MAGIC %md
# MAGIC **Fetch Wiget Value and Use as Paramter in rest of the notebook code**

# COMMAND ----------

s3bucketname = dbutils.widgets.get("BucketName")

# COMMAND ----------

# MAGIC %md
# MAGIC **Image File Source Location**

# COMMAND ----------

dbutils.fs.ls(f"s3://{s3bucketname}/images")

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Image Files With Format "Image"**
# MAGIC https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/source/image/ImageDataSource.html

# COMMAND ----------

sample_img_dir = f"s3://{s3bucketname}/images"

image_df = spark.read.format("image").load(sample_img_dir)

display(image_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC **"Image" Format has some limiations https://docs.databricks.com/data/data-sources/image.html#limitations-of-image-data-source**
# MAGIC 
# MAGIC **These limitations can be avoided by reading source data as "BINARY FILES" and provide "mimeType" as option so that image can be decoded based on the format**
# MAGIC 
# MAGIC **Binary Files :** https://docs.databricks.com/data/data-sources/binary-file.html
# MAGIC 
# MAGIC **Supported Image Format :** https://github.com/AdoptOpenJDK/openjdk-jdk11/blob/19fb8f93c59dfd791f62d41f332db9e306bc1422/src/java.base/windows/classes/sun/net/www/content-types.properties#L182-L251

# COMMAND ----------

df = spark.read.format("binaryFile").option("mimeType","image/*").load(f"s3://{s3bucketname}/images")
display(df)    # image thumbnails are rendered in the "content" column

# COMMAND ----------

# MAGIC %md
# MAGIC **Select CATALOG and SCHEMA/DATABASE**

# COMMAND ----------

spark.sql("USE CATALOG main")
spark.sql("USE main.external_loc")

# COMMAND ----------

# MAGIC %md
# MAGIC **User Defined Function (UDF) to resive image and convert to standard JPEG format**
# MAGIC 
# MAGIC **This is to make sure lesser data gets moved when rendering this data in Power BI and we don't get any Memory Errors**

# COMMAND ----------

# UDF to resize the image (change value for line 11 to apprropirate size based on your requirements)
import io
from pyspark.sql.functions import concat, base64, lit, col
from PIL import Image

@udf("binary")
def resized_image_binary(content):
  
  buffer = io.BytesIO()
  img = Image.open(io.BytesIO(content))
  new_img = img.resize((64, 64))  # Choose your image size
  new_img.save(buffer, format="JPEG")
  img_binary = buffer.getvalue()
  return img_binary

# COMMAND ----------

# MAGIC %md
# MAGIC **Prepare some Sample Data Sets which we will be saving as table later in this notebook**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(),True), 
    StructField("Flower_Name", StringType(), True), 
    StructField("Location", StringType(), True)
])

data = [(1,"Faux Pink Tulip",f"s3://{s3bucketname}/images/Faux_Pink_Tulips.jpeg"),
      (2,"Pink Rose",f"s3://{s3bucketname}/images/Pink_Rose.jpeg"),
      (3,"Sunflower",f"s3://{s3bucketname}/images/Sunflower.jpg"),
      (4,"Wild Frangipani",f"s3://{s3bucketname}/images/Wild_Frangipani.jpg")
     ]

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Each image file has some propoerties like: Path, Content**
# MAGIC 
# MAGIC **We will be calling *"resized_image_binary" UDF on Content column* to convert file to JPEG and of a specific size**

# COMMAND ----------

flowers_df = spark.read.format("binaryFile").load(f"s3://{s3bucketname}/images") \
  .select(
    col("path"),
    col("content"),
    resized_image_binary(col("content")).cast("binary").alias("resized_binary")
  ) \
  .withColumn("resized_image_base64",
            concat(lit('data:image/jpeg;base64,'), base64(col("resized_binary"))))


display(flowers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Next, we will join both original data source dataframe and image file dataframe to form a single record to be saved in the delta table**

# COMMAND ----------

import pyspark.sql.functions as F

final_df = df.join(flowers_df, F.element_at(F.split("Location","\-"),-1) == F.element_at(F.split("path","\-"),-1),'left')



# COMMAND ----------

# MAGIC %md
# MAGIC **Remove Old Location If Exists**

# COMMAND ----------

#spark.sql("DROP TABLE Flower_Images")
dbutils.fs.rm(f"s3://{s3bucketname}/flowerimages/",recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Save final dataframe as DELTA TABLE**

# COMMAND ----------

final_df.write.option("path", f"s3://{s3bucketname}/flowerimages").mode("overwrite").saveAsTable("Flower_Images")
display(spark.table("Flower_Images"))

# COMMAND ----------

display(spark.sql("SHOW GRANT ON CATALOG main"))

# COMMAND ----------

display(spark.sql("SHOW GRANT ON SCHEMA external_loc"))

# COMMAND ----------

display(spark.sql("SHOW GRANT ON EXTERNAL LOCATION `uc_demo_ext_tables`"))

