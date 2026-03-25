from pyspark.sql import functions as f
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    df_enrollment_reason = inputs['df_enrollment_reason']

    logging.info("Executando transformações na tabela Enrollment Reason")

    df_enrollment_reason = (df_enrollment_reason.select(
        f.col("code").alias("COD_MTVO_EVSO").cast("String"),
        f.col("description").alias("DSC_MTVO_EVSO").cast("string"),
        f.col("removed").alias("IND_RMVD").cast("Boolean"),
        f.col("created_by").alias("USR_CRCO").cast("string"),
        f.col("created_date").alias("DAT_CRCO").cast("timestamp"),
        f.col("last_modified_by").alias("USR_ULTM_MDFO").cast("string"),
        f.col("last_modified_date").alias("DAT_ULTM_MDFO").cast("timestamp"),
        f.col("open_to_student").alias("IND_ABRO_ALNO").cast("boolean"),
        f.col("student_description").alias("DSC_SBMT_EVSO").cast("string")
    ))
    return df_enrollment_reason
