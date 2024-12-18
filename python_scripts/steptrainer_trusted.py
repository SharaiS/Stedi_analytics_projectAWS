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

# Script generated for node steptrainer_landing
steptrainer_landing_node1734443683924 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-sh/steptrainer/landing/"], "recurse": True}, transformation_ctx="steptrainer_landing_node1734443683924")

# Script generated for node customer_curated
customer_curated_node1734443735928 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-sh/customer/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1734443735928")

# Script generated for node SQL Query
SqlQuery8589 = '''
SELECT DISTINCT s.*
FROM steptrainer_landing AS s
INNER JOIN customer_curated AS c
ON s.serialnumber = c.serialnumber;

'''
SQLQuery_node1734444650742 = sparkSqlQuery(glueContext, query = SqlQuery8589, mapping = {"steptrainer_landing":steptrainer_landing_node1734443683924, "customer_curated":customer_curated_node1734443735928}, transformation_ctx = "SQLQuery_node1734444650742")

# Script generated for node steptrainer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734444650742, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734386472599", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
steptrainertrusted_node1734392636423 = glueContext.getSink(path="s3://stedi-lakehouse-sh/steptrainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1734392636423")
steptrainertrusted_node1734392636423.setCatalogInfo(catalogDatabase="stedi",catalogTableName="steptrainer_trusted")
steptrainertrusted_node1734392636423.setFormat("json")
steptrainertrusted_node1734392636423.writeFrame(SQLQuery_node1734444650742)
job.commit()