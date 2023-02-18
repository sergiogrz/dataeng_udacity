import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customers trusted zone
Customerstrustedzone_node1676746290922 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dataeng-udacity/customers/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customerstrustedzone_node1676746290922",
)

# Script generated for node Accelerometer landing zone
Accelerometerlandingzone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dataeng-udacity/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometerlandingzone_node1",
)

# Script generated for node Customer privacy filter
Customerprivacyfilter_node2 = Join.apply(
    frame1=Accelerometerlandingzone_node1,
    frame2=Customerstrustedzone_node1676746290922,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Customerprivacyfilter_node2",
)

# Script generated for node Consent date filter
SqlQuery709 = """
select * from customer_privacy_filter
where timeStamp >= shareWithResearchAsOfDate;

"""
Consentdatefilter_node1676746908751 = sparkSqlQuery(
    glueContext,
    query=SqlQuery709,
    mapping={"customer_privacy_filter": Customerprivacyfilter_node2},
    transformation_ctx="Consentdatefilter_node1676746908751",
)

# Script generated for node Drop Fields
DropFields_node1676746611421 = DropFields.apply(
    frame=Consentdatefilter_node1676746908751,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1676746611421",
)

# Script generated for node Accelerometer trusted zone
Accelerometertrustedzone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1676746611421,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dataeng-udacity/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Accelerometertrustedzone_node3",
)

job.commit()
