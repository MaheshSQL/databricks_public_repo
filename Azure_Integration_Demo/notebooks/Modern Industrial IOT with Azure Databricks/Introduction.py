# Databricks notebook source
# MAGIC %md <a href='$../Azure Integrations Start Here'>Home</a>

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 10; padding-top: 30px;  padding-bottom: 30px;">
# MAGIC   <img src="https://gsethistorageaccount.blob.core.windows.net/images/slide1.png" style="width: 500" >
# MAGIC </div>
# MAGIC 
# MAGIC <br></br>
# MAGIC <br></br>

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 10; padding-top: 30px;  padding-bottom: 30px;">
# MAGIC   <img src="https://gsethistorageaccount.blob.core.windows.net/images/slide2.jpg" alt='GrabNGoInfo Logo' style="width: 50px" >
# MAGIC </div>
# MAGIC 
# MAGIC <br></br>
# MAGIC <br></br>

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 10; padding-top: 30px;  padding-bottom: 30px;">
# MAGIC   <img src="https://gsethistorageaccount.blob.core.windows.net/images/slide3.png" alt='GrabNGoInfo Logo' style="width: 50px" >
# MAGIC </div>
# MAGIC 
# MAGIC <br></br>
# MAGIC <br></br>

# COMMAND ----------

# MAGIC %md
# MAGIC ##The challenges of building an IoT solution
# MAGIC The Industrial Internet of Things  (IIoT) has grown over the last few years as a grassroots technology stack being piloted predominantly in the oil & gas industry to wide scale adoption and production use across manufacturing, chemical, utilities, transportation and energy sectors. Traditional IoT systems like Scada, Historians and even Hadoop do not provide the big data analytics capabilities needed by most organizations to predictively optimize their industrial assets due to the following factors.
# MAGIC 
# MAGIC <img src="https://gsethistorageaccount.blob.core.windows.net/images/table1.png" width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://gsethistorageaccount.blob.core.windows.net/images/graphic1.png" width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://gsethistorageaccount.blob.core.windows.net/images/graphic2.png" width=900>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Components of a modern industrial IoT solution
# MAGIC Components commonly seen in a modern industrial IoT solution include:
# MAGIC 
# MAGIC <ul>
# MAGIC   <li>API/SDK Support (in silicon or software library)</li>
# MAGIC   <li>Streaming Endpoints (with autoscale support)</li>
# MAGIC   <li>Batch ETL/ELT (bulk loads when live telemetry not available)</li>
# MAGIC   <li>Storage (cost effective and scalable)</li>
# MAGIC   <li>Data Integration (historical correlation, enrichment via FACT tables</li>
# MAGIC   <li>Reporting and Dashboards</li>
# MAGIC   <li>Control Systems Integration</li>
# MAGIC   <li>Data Science and Machine Learning</li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Designing a scalable, resilient cloud based architecture to support IoT
# MAGIC 
# MAGIC The architecture below illustrates a modern, best-of-breed platform used by many organizations that leverages all that Azure has to offer for IIoT analytics.
# MAGIC 
# MAGIC <img src="https://sguptasa.blob.core.windows.net/random/iiot_blog/end_to_end_architecture.png" width=900>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://gsethistorageaccount.blob.core.windows.net/images/Delta-Lake-Logo_pos-hor-CMYK.png" width=600>
# MAGIC 
# MAGIC A key component of this architecture is the Azure Data Lake Store (ADLS), which enables the write-once, access-often analytics pattern in Azure. However, Data Lakes alone do not solve the real-world challenges that come with time-series streaming data. The Delta storage format provides a layer of resiliency and performance on all data sources stored in ADLS. Specifically for time-series data, Delta provides the following advantages over other storage formats on ADLS:
# MAGIC 
# MAGIC <img src="https://jokdemoresourcessa.blob.core.windows.net/iotimages/table2.png" width=900>

# COMMAND ----------

# MAGIC %md <a href='$./IIoT End-to-End Pt1'>Next</a>
