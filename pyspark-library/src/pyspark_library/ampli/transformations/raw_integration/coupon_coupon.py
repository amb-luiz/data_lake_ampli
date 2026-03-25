from pyspark.sql import functions as f
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):

    df_coupon_coupon = inputs['df_coupon_coupon']

    logging.info("Executando transformações na tabela Coupon")

    df_coupon_coupon = (df_coupon_coupon.select(
        f.col("code").alias("COD_COD_CPOM").cast("string"),
        f.col("description").alias("DSC_CPOM").cast("string"),
        f.col("discount").alias("VLR_TIPO_DESC").cast("float"),
        f.col("initial_amount").alias("NUM_QTDE_INCL").cast("integer"),
        f.col("amount_available").alias("NUM_QTDE_DSPN").cast("integer"),
        f.col("start_date").alias("DAT_INI_VGNC").cast("timestamp"),
        f.col("expiration_date").alias("DAT_FIM_VGNC").cast("timestamp"),
        f.col("type").alias("TIP_CLCL_DESC").cast("string"),
        f.col("removed").alias("IND_RMVD").cast("boolean"),
        f.col("created_by").alias("USR_CRCO").cast("string"),
        f.col("created_date").alias("DAT_CRCO").cast("timestamp"),
        f.col("last_modified_by").alias("USR_MDFR").cast("string"),
        f.col("last_modified_date").alias("DAT_ULTM_MDFO").cast("timestamp"),
        f.col("number_of_instalments").alias("NUM_PCLA").cast("integer"),
        f.col("category").alias("DSC_CTGR").cast("string")
    ))

    return df_coupon_coupon
