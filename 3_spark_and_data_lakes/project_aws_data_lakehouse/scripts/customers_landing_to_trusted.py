import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customers landing zone
Customerslandingzone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dataeng-udacity/customers/landing/"],
        "recurse": True,
    },
    transformation_ctx="Customerslandingzone_node1",
)

# Script generated for node Privacy filter
Privacyfilter_node2 = Filter.apply(
    frame=Customerslandingzone_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Privacyfilter_node2",
)

# Script generated for node Customers trusted zone
Customerstrustedzone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Privacyfilter_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dataeng-udacity/customers/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Customerstrustedzone_node3",
)

job.commit()
