import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step trainer trusted zone
Steptrainertrustedzone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="Steptrainertrustedzone_node1",
)

# Script generated for node Accelerometer trusted zone
Accelerometertrustedzone_node1676762328854 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="Accelerometertrustedzone_node1676762328854",
    )
)

# Script generated for node Join step trainer - accelerometer
Steptrainertrustedzone_node1DF = Steptrainertrustedzone_node1.toDF()
Accelerometertrustedzone_node1676762328854DF = (
    Accelerometertrustedzone_node1676762328854.toDF()
)
Joinsteptraineraccelerometer_node2 = DynamicFrame.fromDF(
    Steptrainertrustedzone_node1DF.join(
        Accelerometertrustedzone_node1676762328854DF,
        (
            Steptrainertrustedzone_node1DF["sensorreadingtime"]
            == Accelerometertrustedzone_node1676762328854DF["timestamp"]
        ),
        "left",
    ),
    glueContext,
    "Joinsteptraineraccelerometer_node2",
)

# Script generated for node Drop Fields
DropFields_node1676762595336 = DropFields.apply(
    frame=Joinsteptraineraccelerometer_node2,
    paths=["user", "timestamp"],
    transformation_ctx="DropFields_node1676762595336",
)

# Script generated for node Machine learning curated
Machinelearningcurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1676762595336,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dataeng-udacity/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="Machinelearningcurated_node3",
)

job.commit()
