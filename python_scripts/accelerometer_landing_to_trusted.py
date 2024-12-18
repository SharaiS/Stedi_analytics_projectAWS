import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node customer_trusted
customer_trusted_node1734391358976 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1734391358976")

# Script generated for node accelerometer landing
accelerometerlanding_node1734391175360 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-sh/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometerlanding_node1734391175360")

# Script generated for node customer privacy filter
customerprivacyfilter_node1734391427244 = Join.apply(frame1=customer_trusted_node1734391358976, frame2=accelerometerlanding_node1734391175360, keys1=["email"], keys2=["user"], transformation_ctx="customerprivacyfilter_node1734391427244")

# Script generated for node Drop Fields
DropFields_node1734391550006 = DropFields.apply(frame=customerprivacyfilter_node1734391427244, paths=["registrationdate", "customername", "birthday", "sharewithfriendsasofdate", "sharewithpublicasofdate", "lastupdatedate", "timestamp", "email", "serialnumber", "phone", "sharewithresearchasofdate"], transformation_ctx="DropFields_node1734391550006")

# Script generated for node accelerometer trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1734391550006, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734386472599", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometertrusted_node1734391722075 = glueContext.getSink(path="s3://stedi-lakehouse-sh/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometertrusted_node1734391722075")
accelerometertrusted_node1734391722075.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted1")
accelerometertrusted_node1734391722075.setFormat("json")
accelerometertrusted_node1734391722075.writeFrame(DropFields_node1734391550006)
job.commit()