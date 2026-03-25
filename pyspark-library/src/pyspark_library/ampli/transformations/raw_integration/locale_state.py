from pyspark.sql import functions as f
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):

    df_locale_state = inputs['df_locale_state']

    logging.info("Executando transformações na tabela City")

    df_locale_state = (df_locale_state.select(
        f.col("id").alias("COD_ESTD").cast("integer"),
        f.col("name").alias("NOM_ESTD").cast("string"),
        f.col("acronym").alias("DSC_ACNM").cast("string"),
        f.col("country_id").alias("COD_PAIS").cast("integer"),
        f.col("name").alias("LCL_ENDR").cast("string")
    ))

    return df_locale_state