# AutoLoader

This project is to demostrate Databricks Autoloader capability to process data. There are multiple notebooks, each desribes below:



## Autoloader_Write_Multiple_Delta_Tables
This notebook is to demonstrate as how to read **Single Input File** (Json this case), **Extract Values** and load then into **Different Entities / Multiple Delta Tabes** 

<img width="1031" alt="image" src="https://user-images.githubusercontent.com/95003669/167319180-912a1e6e-2543-40c9-b9b8-fd462d80e46c.png">


### Sample Datasets
* This notebook uses 2 sample data file, present in **SampleDataSet** folder

  * Member_1.json
  * Member_2.json

* Cmd3 of this notebook has variables to define
  * Path for Source File that Autoloader will read
  * Path for saving Schema definition - Autoloader will use this location to keep Schema information
  * Path for saving Bronze Delta Table
  * Path for saving Checkpoint definition - Autoloder uses this information to saving metadata for incremental file processing

* Cmd4 of this notebook has a function that will be called by **Autoloader WriteStream's FOREACHBATCH**
  This function by defaults takes 2 Parameters which is
    * Incoming Dataset Micro-Batch
    * BatchId (Auto generated)

  As we are writing to multiple delta tables, its **Idempotent Multi-Write Table** (desribes in below link), hence forth we are passing Transaction ID and Application ID as Unique values so that in case a write fails aborultply, then at next run, system should able to correctly process data and avoid writng duplicate records.
  
  https://docs.databricks.com/delta/delta-streaming.html#idempotent-multi-table-writes 
  

* Cmd5 of this notebook, we are using **Autoloader's ReadStream** to read inut JSON file and use **Autoloader's WriteStream** to use **FOREACHBATCH** and call **Function Degined in Cmd4** to write data into multiple delta tables.

* Cmd6 of this notebook, we are displaying newly created DELTA Tables 

## Autoloader Documentation and Other important links

https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html

https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html 

https://databricks.com/blog/2020/02/24/introducing-databricks-ingest-easy-data-ingestion-into-delta-lake.html

https://www.youtube.com/watch?v=kNG-5uRY4o8 
