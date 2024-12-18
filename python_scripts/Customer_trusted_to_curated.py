import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer trusted
customertrusted_node1734389886288 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customertrusted_node1734389886288")

# Script generated for node Accelerometer landing
Accelerometerlanding_node1734389930786 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="Accelerometerlanding_node1734389930786")

# Script generated for node Join
Join_node1734389971982 = Join.apply(frame1=Accelerometerlanding_node1734389930786, frame2=customertrusted_node1734389886288, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1734389971982")

# Script generated for node SQL Query
SqlQuery8373 = '''
select distinct customername, email, phone,birthday,
serialnumber,registrationdate,lastupdatedate,
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate
from myDataSource
'''
SQLQuery_node1734390120928 = sparkSqlQuery(glueContext, query = SqlQuery8373, mapping = {"myDataSource":Join_node1734389971982}, transformation_ctx = "SQLQuery_node1734390120928")

# Script generated for node customer curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734390120928, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734386472599", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customercurated_node1734390356002 = glueContext.getSink(path="s3://stedi-lakehouse-sh/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customercurated_node1734390356002")
customercurated_node1734390356002.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customercurated_node1734390356002.setFormat("json")
customercurated_node1734390356002.writeFrame(SQLQuery_node1734390120928)
job.commit()