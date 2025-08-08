import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

# Initialize Glue and Spark Contexts
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ----------------------------
# Define SchemasFG
# -------------------ERTY---------

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

# ----------------------------
# Read Files from S3
# -----------NBH -----------------

indiv_df = (
    spark.read
    .option("header", "false")
    .option("delimiter", "|")
    .schema(contribution_schema)
    .csv("s3://cicd-bucket-050/indiv/itcont.txt")
)

committee_df = (
    spark.read
    .option("header", "false")
    .option("delimiter", "|")
    .schema(committee_schema)
    .csv("s3://cicd-bucket-050/cm-1/cm20.txt")
)

candidate_df = (
    spark.read
    .option("header", "false")
    .option("delimiter", "|")
    .schema(candidate_schema)
    .csv("s3://cicd-bucket-050/cn-01/cn20.txt")
)

# ----------------------------
# Join the DataFrames
# ----------------------------

half_join = indiv_df.join(
    committee_df,
    indiv_df["CMTE_ID"] == committee_df["CMTE_ID"],
    "inner"
)

final_join = half_join.join(
    candidate_df,
    half_join["CAND_ID"] == candidate_df["CAND_ID"],
    "inner"
)

# ----------------------------
# Final Selection
# ----------------------------

final_df = final_join.select(
    indiv_df["*"],
    committee_df["CMTE_PTY_AFFILIATION"].alias("ComitteParty_AFFILIATION"),
    candidate_df["CAND_PTY_AFFILIATION"].alias("CandidateParty_AFFILIATION")
)

# ----------------------------
# Write Output to S3
# ----------------------------

final_df.coalesce(1).write.mode("overwrite").parquet("s3://doneeeeeeeeeeeeeeeeeeeeee/master_of_master/")


# ----------------------------
# Commit Job
# ----------------------------


job.commit()
