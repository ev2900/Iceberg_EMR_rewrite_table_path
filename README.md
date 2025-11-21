# Iceberg EMR Serverless - rewrite_table_path

<img width="275" alt="map-user" src="https://img.shields.io/badge/cloudformation template deployments-000-blue"> <img width="85" alt="map-user" src="https://img.shields.io/badge/views-0000-green"> <img width="125" alt="map-user" src="https://img.shields.io/badge/unique visits-000-green">

As of Apache Iceberg version 1.9.0 a procedure ```rewrite_table_path``` was added. This procedure will update the absolute path reference in the Iceberg metadata files and stage this update copy (of the metadata files) in preparation for table migration. 

If you have a scenario where you need to move an Iceberg table from one S3 bucket to another this procedure is required to updated the absolute path references from the previous S3 bucket name to the new S3 bucket name.

It is not possible to call the ```rewrite_table_path``` from AWS Glue. This example demonstrates how to use EMR serverless to with the ```rewrite_table_path``` procedure.

# Example using EMR Serverless

Launch the CloudFormation stack below to deploy an S3 bucket (with Iceberg JARs and scripts) downloaded, an EMR serverless application and an EMR studio.

> [!WARNING]
> The CloudFormation stack creates several IAM roles that have ADMIN permissions. This is not appropriate for production deployments. Scope these roles down before using this CloudFormation in production.


[![Launch CloudFormation Stack](https://sharkech-public.s3.amazonaws.com/misc-public/cloudformation-launch-stack.png)](https://console.aws.amazon.com/cloudformation/home#/stacks/new?stackName=iceberg-rewrite-table-path&templateURL=https://sharkech-public.s3.amazonaws.com/misc-public/emr_s_rewrite_table_path.yaml)

In this example I will assume that you already have an Iceberg table created. For this example, the name of the table is ```sampledataicebergtable``` it is registered with the Glue Data Catalog under the database ```iceberg```

If you do not already have an iceberg table created. You can deploy the CloudFormation stack in the repo. [Iceberg_Glue_register_table](https://github.com/ev2900/Iceberg_Glue_register_table) and run the Glue script titled ```0 Create Iceberg Table```

## Update Spark Script

We will submit the [spark job](https://github.com/ev2900/Iceberg_EMR_rewrite_table_path/blob/main/rewrite_table_path.py) calling the ```rewrite_table_path``` procedure via. the EMR studio UI. Before we submit the job we need to update the [spark job](https://github.com/ev2900/Iceberg_EMR_rewrite_table_path/blob/main/rewrite_table_path.py).

Specifically in the Spark session we need to update 
* ```<s3_bucket_name>``` with the name of the S3 bucket that has the source Iceberg table metadata

For my example, my spark session is 

```
spark = SparkSession.builder \
    .appName("IcebergIntegration") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://iceberg-register-table-s3-vkkdzbwgrztp/iceberg/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()
```

Specifically in the Spark SQL query we need to update 
* ```<database_name>.<table_name>```
* ```source_prefix => 's3://<s3_bucket_name>/<s3_file_path_to_iceberg_metadata_of_table_to_be_migrated>'```
* ``` source_prefix => 's3://<s3_bucket_name>/<s3_file_path_to_iceberg_metadata_of_table_to_be_migrated>'```
* ```staging_location => 's3://<s3_bucket_name>/<path_the_updated_metadata_to_be_written_to>'```

For my example, the query is 

```
query = f"""
CALL glue_catalog.system.rewrite_table_path(
  table => 'glue_catalog.iceberg.sampledataicebergtable',
  source_prefix => 's3://iceberg-register-table-s3-vkkdzbwgrztp/iceberg/iceberg.db/sampledataicebergtable',
  target_prefix => 's3://iceberg-migrate-to-bucket-fkjd43es/iceberg/iceberg.db/sampledataicebergtable',
  staging_location => 's3://iceberg-migrate-to-bucket-fkjd43es/iceberg/iceberg.db/sampledataicebergtable/metadata')
"""
```

Once you update the script you will need to upload / replace the generic script in the S3 bucket deployed via. the CloudFormation template.

<img width="700" alt="quick_setup" src="https://github.com/ev2900/Iceberg_EMR_rewrite_table_path/blob/main/README/readme_1.png">

## Log into EMR Studio

Navigate the EMR service page and then to studios and click on the studio access URL. 

<img width="700" alt="quick_setup" src="https://github.com/ev2900/Iceberg_EMR_rewrite_table_path/blob/main/README/readme_0.png">

## Submit Spark Job

From the EMR Studio UI select the existing serverless application 

<img width="700" alt="quick_setup" src="https://github.com/ev2900/Iceberg_EMR_rewrite_table_path/blob/main/README/readme_2.png">

Then select submit job run

<img width="700" alt="quick_setup" src="https://github.com/ev2900/Iceberg_EMR_rewrite_table_path/blob/main/README/readme_3.png">

On the submit job page we will configure our Spark Job submission. Update the following aspects
* Job name: ```rewrite_table_path```
* Runtime role: Select ```iceberg-rewrite-table-path-EMRServerlessRunTimeRole-xxxx```
* Script location: _Select the s3 path with the updated script from the first step_
* Spark properties: key = ```--conf``` | value = ```spark.jars=s3://<s3_bucket_name>/jars/iceberg-aws-bundle-1.10.0.jar,s3://<s3_bucket_name>/jars/iceberg-spark-runtime-3.5_2.12-1.10.0.jar``` with ```<s3_bucket_name>``` replaced with the S3 bucket created by the CloudFormation template

<img width="700" alt="quick_setup" src="https://github.com/ev2900/Iceberg_EMR_rewrite_table_path/blob/main/README/readme_4.png">

Press the **Submit job run** button.

You can check the status of the job, once it shows as status of _Success_

<img width="700" alt="quick_setup" src="https://github.com/ev2900/Iceberg_EMR_rewrite_table_path/blob/main/README/readme_5.png">

You can check the staging location S3 bucket. You will see the updated metadata files.

<img width="700" alt="quick_setup" src="https://github.com/ev2900/Iceberg_EMR_rewrite_table_path/blob/main/README/readme_6.png">

From this point you can copy the data files to this same folder and register the updated metadata with the Glue data catalog via. the ```register_table``` procedure. A GitHub repo with an example of running the ```register_table``` procedure can be found [HERE](https://github.com/ev2900/Iceberg_Glue_register_table)
