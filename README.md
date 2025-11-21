# Iceberg EMR Serverless - rewrite_table_path

<img width="275" alt="map-user" src="https://img.shields.io/badge/cloudformation template deployments-000-blue"> <img width="85" alt="map-user" src="https://img.shields.io/badge/views-0000-green"> <img width="125" alt="map-user" src="https://img.shields.io/badge/unique visits-000-green">

As of Apache Iceberg version 1.9.0 a procedure ```rewrite_table_path``` was added. This procedure will updated the absolute path reference in the Iceberg metadata files and stage this update copy (of the metadata files) in preparation for table migration. 

If you have a scenario where you need to move an Iceberg table from one S3 bucket to another this procedure is required to updated the absolute path references from the previous S3 bucket name to the new S3 bucket name.

It is not possible to call the ```rewrite_table_path``` from AWS Glue. This example demonstrates how to use EMR serverless to with the ```rewrite_table_path``` procedure.

# Example using EMR Serverless

Launch the CloudFormation stack below to deploy an S3 bucket (with Iceberg JARs and scripts) downloaded, an EMR serverless application and an EMR studio.

[![Launch CloudFormation Stack](https://sharkech-public.s3.amazonaws.com/misc-public/cloudformation-launch-stack.png)](https://console.aws.amazon.com/cloudformation/home#/stacks/new?stackName=iceberg-rewrite-table-path&templateURL=https://sharkech-public.s3.amazonaws.com/misc-public/emr_s_rewrite_table_path.yaml)

In this example I will assume that you already have an Iceberg table created. For this example the name of the table is ```sampledataicebergtable``` it is registered with the Glue Data Catalog under the database ```iceberg```


