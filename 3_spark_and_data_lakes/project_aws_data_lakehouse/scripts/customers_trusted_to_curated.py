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

# Script generated for node Accelerometer trusted zone
Accelerometertrustedzone_node1676752641419 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="Accelerometertrustedzone_node1676752641419",
    )
)

# Script generated for node Customers trusted zone
Customerstrustedzone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customers_trusted",
    transformation_ctx="Customerstrustedzone_node1",
)

# Script generated for node Customers with accelerometer
Customerswithaccelerometer_node2 = Join.apply(
    frame1=Customerstrustedzone_node1,
    frame2=Accelerometertrustedzone_node1676752641419,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Customerswithaccelerometer_node2",
)

# Script generated for node Filter
Filter_node1676752806252 = DropFields.apply(
    frame=Customerswithaccelerometer_node2,
    paths=["z", "user", "y", "x", "timestamp"],
    transformation_ctx="Filter_node1676752806252",
)

# Script generated for node Customers curated zone
Customerscuratedzone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1676752806252,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dataeng-udacity/customers/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="Customerscuratedzone_node3",
)

job.commit()
