from pyspark.sql import functions as f
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):

    df_locale_city = inputs['df_locale_city']

    logging.info("Executando transformações na tabela City")

    df_locale_city = (df_locale_city.select(
        f.col("id").alias("COD_CDDE").cast("integer"),
        f.col("name").alias("NOM_CDDE").cast("string"),
        f.col("capital").alias("IND_CPTL").cast("boolean"),
        f.col("state_id").alias("COD_ESTD").cast("integer"),
        f.col("latitude").alias("LCL_LTTD").cast("float"),
        f.col("longitude").alias("CL_LNGT").cast("float"),
        f.col("name").alias("LCL_ENDR").cast("string"),
    ))

    return df_locale_city