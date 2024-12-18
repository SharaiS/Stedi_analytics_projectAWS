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

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1734441789947 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="Accelerometertrusted_node1734441789947")

# Script generated for node steptrainer_trusted
steptrainer_trusted_node1734444993596 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="steptrainer_trusted2", transformation_ctx="steptrainer_trusted_node1734444993596")

# Script generated for node SQL Query
SqlQuery8367 = '''
select 
    s.*,
    a.x,
    a.y,
    a.z
from steptrainer_trusted as s
join accelerometer_trusted as a
on s.sensorreadingtime = a.timestamp
'''
SQLQuery_node1734441892042 = sparkSqlQuery(glueContext, query = SqlQuery8367, mapping = {"accelerometer_trusted":Accelerometertrusted_node1734441789947, "steptrainer_trusted":steptrainer_trusted_node1734444993596}, transformation_ctx = "SQLQuery_node1734441892042")

# Script generated for node machine learning curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1734441892042, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734440083516", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machinelearningcurated_node1734441903093 = glueContext.getSink(path="s3://stedi-lakehouse-sh/machinelearning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machinelearningcurated_node1734441903093")
machinelearningcurated_node1734441903093.setCatalogInfo(catalogDatabase="stedi",catalogTableName="ml_curated")
machinelearningcurated_node1734441903093.setFormat("glueparquet", compression="snappy")
machinelearningcurated_node1734441903093.writeFrame(SQLQuery_node1734441892042)
job.commit()