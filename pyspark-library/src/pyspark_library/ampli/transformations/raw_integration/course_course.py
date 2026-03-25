from pyspark.sql import functions as f
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):

    df_course_course = inputs['df_course_course']

    logging.info("Executando transformações na tabela Course")

    df_course_course = (df_course_course.select(
        f.col("id").alias("COD_CURS").cast("string"),
        f.col("code").alias("COD_COD_CURS").cast("string"),
        f.col("name").alias("NOM_CURS").cast("string"),
        f.col("summary").alias("DSC_RSMO").cast("string"),
        f.col("workload").alias("DSC_CRGA_HRRA").cast("integer"),
        f.col("duration").alias("DSC_DURC_CURS").cast("string"),
        f.col("mandatory_internship").alias("IND_ESTG_OBGT").cast("boolean"),
        f.col("course_structure_document").alias("DSC_DOCT_ESTR_CURS").cast("string"),
        f.col("status").alias("STA_STTS_CURS").cast("string"),
        f.col("removed").alias("IND_RMVD_CURS").cast("boolean"),
        f.col("created_by").alias("RGT_CRCO_CURS").cast("string"),
        f.col("created_date").alias("DAT_CRCO_CURS").cast("string"),
        f.col("last_modified_by").alias("USR_ULTM_MDFO_CURS").cast("string"),
        f.col("last_modified_date").alias("DAT_ULTM_STCO_CURS").cast("string"),
        f.col("course_type_code").alias("IND_TIP_CURS").cast("string"),
        f.col("url_code").alias("DSC_CDGO_URL_CURS").cast("string"),
        f.col("image_id").alias("COD_IMGM_CURS").cast("string"),
        f.col("partner_id").alias("COD_PCRA_CURS").cast("string"),
        f.col("mec_code").alias("COD_CDGO_MEC").cast("string")
        ))

    return df_course_course