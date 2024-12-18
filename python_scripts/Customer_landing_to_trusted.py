import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node  S3 bucket
S3bucket_node1732040992149 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-sh/customer/landing/"], "recurse": True}, transformation_ctx="S3bucket_node1732040992149")

# Script generated for node Apply mapping
Applymapping_node1732041047354 = ApplyMapping.apply(frame=S3bucket_node1732040992149, mappings=[("customername", "string", "customername", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialnumber", "string"), ("registrationdate", "bigint", "registrationdate", "long"), ("lastupdatedate", "bigint", "lastupdatedate", "long"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "long")], transformation_ctx="Applymapping_node1732041047354")

# Script generated for node PrivacyFilter
PrivacyFilter_node1732041088311 = Filter.apply(frame=Applymapping_node1732041047354, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="PrivacyFilter_node1732041088311")

# Script generated for node trusted customer zone
trustedcustomerzone_node1732041198080 = glueContext.write_dynamic_frame.from_options(frame=PrivacyFilter_node1732041088311, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lakehouse-sh/customer/trusted/", "partitionKeys": []}, transformation_ctx="trustedcustomerzone_node1732041198080")

job.commit()