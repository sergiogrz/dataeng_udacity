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

# Script generated for node Customers curated zone
Customerscuratedzone_node1676761026206 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customers_curated",
    transformation_ctx="Customerscuratedzone_node1676761026206",
)

# Script generated for node Step trainer landing zone
Steptrainerlandingzone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dataeng-udacity/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Steptrainerlandingzone_node1",
)

# Script generated for node Join step trainer customers
Joinsteptrainercustomers_node2 = Join.apply(
    frame1=Steptrainerlandingzone_node1,
    frame2=Customerscuratedzone_node1676761026206,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Joinsteptrainercustomers_node2",
)

# Script generated for node Drop Fields
DropFields_node1676761147863 = DropFields.apply(
    frame=Joinsteptrainercustomers_node2,
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
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1676761147863",
)

# Script generated for node Step trainer trusted zone
Steptrainertrustedzone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1676761147863,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dataeng-udacity/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Steptrainertrustedzone_node3",
)

job.commit()
