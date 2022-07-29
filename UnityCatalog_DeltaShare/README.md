# High Level Design of Unity Catalog Demo


![image](https://user-images.githubusercontent.com/95003669/168706922-ac2b77f5-46f7-44a8-8d47-11572080ad0f.png)

## Requirements

1. We need total 4 users for the demo purposes

      * Admin User

      * User 1 - Act as AMEA User (in this demo)

      * User 2 - Act as EMEA User (in this demo)

      * External User - For Delta Share
  
2.  We need minimum 1 workspace for the demo purposes
3.  We need minimum 3 clusters for the demo purposes
       * **Security Mode** for **Admin Cluster** Should be = **SINGLE USER** 
          This is required as **2.Load Data notebook** has few code snippets written in SQL & Python
          
          ![image](https://user-images.githubusercontent.com/95003669/174960164-ce540c02-adf0-4328-a05a-b3c754f49859.png)

          
       * **Security Mode** for **User 1 Cluster** Should be = **USER ISOLATION**

          ![image](https://user-images.githubusercontent.com/95003669/161189561-224aadf0-b50f-42b7-ad0f-38b3baeeee37.png)

       * **Security Mode** for **User 2 Cluster** should be = **SINGLE USER**

          ![image](https://user-images.githubusercontent.com/95003669/161189761-d6b3f3e3-279e-4f92-abcc-2ee6d707b1a0.png)

     Refer this link to know more about Security Modes : https://docs.databricks.com/data-governance/unity-catalog/key-concepts.html#cluster-security-mode 
     
4.  Grant User 1 and User 2 acess to seprate clusters (To demostrate seperation of workload)

     Here in below screenshot, on AMEA cluster we have granted access to AMEA User/Group and Admin User only
     ![image](https://user-images.githubusercontent.com/95003669/174963427-963fbc69-e69f-4c7f-8e51-914989194c30.png)

     Similartily on EMEA cluster grant access to EMEA User/Group and Admin User only

5.  Admin user will 

       * Create Metastore
       
       * Add/Attach workspace with Metastore
       
       * Create Catalog
       
       * Create Database/Schema
       
       * Create Tables
       
       * Populate Data in Tables
       
       * Grant permissions to User 1 and 2
       
       * Create Delta Share and Grant access on it
       
## Notebooks

### Run Notebook 1,2,3 Via Admin Cluster

**1. Create Catalog, Database.sql**

     - Login as Admin User to run this notebook. Once executed, it will:
     -    Create a catalog, Database/Schema and Grants USAGE and CREATE permission on Catalog to User 1 & 2 

**2. Load Data.sql**

     - Login as Admin User to run this notebook. Once executed, it will:
     -    Create few tables, populate data, Optimize iit (Z-Order) and then create Dimenstion & Facts (Dimensional Model) and Views 

**3. Grant Permission On Entities.sql**

     - Login as Admin User to run this notebook. Once executed, it will:
     -    Grant USAGE permisison on Catalog and Database/Schema to User 1 & 2
     -    Grant SELECT permissions on Tables to User 1 & 2

**4. Dynamic Query Test.sql**

     - Open 2 different sessions (each for User 1 & 2)
     - Run this notebook in each of the session
     - We will see that for User 1 (AMEA) all statements will execute successfully (Including SELECT on view)
     - This is because **Cluster Security Mode** is **User Isolation** which when select on view will impersonate orignal user's access and return results
     - Things will fail for User 2 (EMEA) as **Cluster Security Mode** is **Single User** which does not allow impersonation and need access on undelying table as well on which view is created

**5. Cleanup Notebook.sql**

     - Login as Admin User to run this notebbok. Once executed, it will drop all Tables, Databse/Schema and Catalog

**6. External Locations Demo.sql**

     - Need to have **S3 Bucket, IAM Roles Permissions granted and added as Storgae Credential in admin console before hand**
     - Login as Admin User to run this notebook. Once executed, it will:
     -    **Create External Location** which is pointing to a **S3 Storage Bucket** using an **Storage Credential (using IAM Role)**
     -    Will then create a Database/Schema and a table descibing location of S3 bucket. This tabel will be saved in the underlying S3 bucket that we have specified with Create Table. 

**7. Unity Catalog- Simple Demo.sql**

     - Login as Admin User to run this notebook or grant appropriate permissions to either User 1 or 2
     - This script will demonstrate how to create Catalog, Database/Schema, Grant Varied Permisisons to Internal Created Roles, Migrate existing Hive Table to Unity Catalog etc..

**8. Delta Share.sql**

     - This will demonstarte DELTA SHARE feature of databricks
     - Login as Admin User to run this notebook. Once executed, it will:
     -    Create a Share
     -    Add tables to Share
     -    Create Receipient (To access data externally)
     -    Grant access on Share to Receipient
     -    Provide a URL to download Configuration file (to be shared with external receipient to access data)

**9. Delta Sharing Receiver.sql**

     - This notebook to be executed by the external user whom we have granted access to the data
     - Notebook will download Configuration file, save it to external storage (ADLS GEN2 on this case)
     - Will access that file to connect with Delta Share and then read data from both Managed Table and External Table and query it using Pyspark SQL, SQL and Pandas

**10. Mount_Directories.py**

     - This file is used by "9. Delta sharing Received.sql" where by it mount few directories in Databricks. These directories are in Azure Storgage.
     - This file uses Azure Key Vaule based token to access Storage so hence you need to change this as per your environment
     - Please change the storage location and Azure Key Vault information in this script before execution (as per your environment)


## End-To-End View 
Below diagram showcae end-to-end view of this solution starting from Metastore, Catalog, Schema, Tables, Data Model and Permissios assigned to each user.

![image](https://user-images.githubusercontent.com/95003669/168709012-235aa8dc-bfe0-4d9e-82e3-b31c9141a3c2.png)
