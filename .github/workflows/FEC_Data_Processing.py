

import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

# Initialize Glue and Spark Contexts
args = getResolvedOptions(sys.argv, ['FEC_Data_Processing'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['FEC_Data_Processing'], args)
# Define schemas
contribution_schema = StructType([
    StructField("CMTE_ID", StringType(), True),
    StructField("AMNDT_IND", StringType(), True),
    StructField("RPT_TP", StringType(), True),
    StructField("TRANSACTION_PGI", StringType(), True),
    StructField("IMAGE_NUM", StringType(), True),
    StructField("TRANSACTION_TP", StringType(), True),
    StructField("ENTITY_TP", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("ZIP_CODE", StringType(), True),
    StructField("EMPLOYER", StringType(), True),
    StructField("OCCUPATION", StringType(), True),
    StructField("TRANSACTION_DT", StringType(), True),
    StructField("TRANSACTION_AMT", DoubleType(), True),
    StructField("OTHER_ID", StringType(), True),
    StructField("TRAN_ID", StringType(), True),
    StructField("FILE_NUM", LongType(), True),
    StructField("MEMO_CD", StringType(), True),
    StructField("MEMO_TEXT", StringType(), True),
    StructField("SUB_ID", LongType(), False)
])

committee_schema = StructType([
    StructField("CMTE_ID", StringType(), True),
    StructField("CMTE_NAME", StringType(), True),
    StructField("CMTE_TRES_NM", StringType(), True),
    StructField("CMTE_ST1", StringType(), True),
    StructField("CMTE_ST2", StringType(), True),
    StructField("CMTE_CITY", StringType(), True),
    StructField("CMTE_ST", StringType(), True),
    StructField("CMTE_ZIP", StringType(), True),
    StructField("CMTE_DSGN", StringType(), True),
    StructField("CMTE_TP", StringType(), True),
    StructField("CMTE_PTY_AFFILIATION", StringType(), True),
    StructField("CMTE_FILING_FREQ", StringType(), True),
    StructField("ORG_TP", StringType(), True),
    StructField("CONNECTED_ORG_NM", StringType(), True),
    StructField("CAND_ID", StringType(), True)
])

candidate_schema = StructType([
    StructField("CAND_ID", StringType(), False),
    StructField("CAND_NAME", StringType(), True),
    StructField("CAND_PTY_AFFILIATION", StringType(), True),
    StructField("CAND_ELECTION_YR", IntegerType(), True),
    StructField("CAND_OFFICE_ST", StringType(), True),
    StructField("CAND_OFFICE", StringType(), True),
    StructField("CAND_OFFICE_DISTRICT", StringType(), True),
    StructField("CAND_ICI", StringType(), True),
    StructField("CAND_STATUS", StringType(), True),
    StructField("CAND_PCC", StringType(), True),
    StructField("CAND_ST1", StringType(), True),
    StructField("CAND_ST2", StringType(), True),
    StructField("CAND_CITY", StringType(), True),
    StructField("CAND_ST", StringType(), True),
    StructField("CAND_ZIP", StringType(), True)
])

# Read data from S3
candidate_df = spark.read.option("header", "false").option("delimiter", "|").schema(candidate_schema).csv("s3://sample1.buck/cn/cn20.txt")
committee_df = spark.read.option("header", "false").option("delimiter", "|").schema(committee_schema).csv("s3://sample1.buck/cm/cm20.txt")
indiv_df = spark.read.option("header", "false").option("delimiter", "|").schema(contribution_schema).csv("s3://sample1.buck/indiv/itcont.txt")

# Join operations
half_join = indiv_df.join(committee_df, indiv_df["CMTE_ID"] == committee_df["CMTE_ID"], "inner")
master_df = half_join.join(candidate_df, half_join["CAND_ID"] == candidate_df["CAND_ID"], "inner")

# Drop duplicate columns
master_df = master_df.drop("CAND_ID", "CMTE_ID")

# Save result to S3 as Parquet
master_df.coalesce(1).write.mode("overwrite").parquet("s3://sample1.output/sampleoutput/glue_sample_output/")

job.commit()
