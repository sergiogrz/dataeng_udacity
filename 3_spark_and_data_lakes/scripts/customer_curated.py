import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer landing
Accelerometerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dataeng-udacity-spark/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometerlanding_node1",
)

# Script generated for node Customer trusted
Customertrusted_node1676658169834 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="Customertrusted_node1676658169834",
)

# Script generated for node Customer privacy filter
Customerprivacyfilter_node2 = Join.apply(
    frame1=Accelerometerlanding_node1,
    frame2=Customertrusted_node1676658169834,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Customerprivacyfilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1676658770478 = DropFields.apply(
    frame=Customerprivacyfilter_node2,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1676658770478",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1676658770478,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dataeng-udacity-spark/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
