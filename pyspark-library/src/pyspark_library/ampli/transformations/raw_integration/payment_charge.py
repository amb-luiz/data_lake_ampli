from pyspark.sql import functions as f
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    df_payment_charge = inputs['df_payment_charge']

    logging.info(
        "Executando transformações na tabela Payment_charge")

    df_payment_charge = (df_payment_charge.select(
        f.col("id").alias("COD_TAXA_PGTO").cast("string"),
        f.col("gateway_id").alias("COD_GWAY").cast("integer"),
        f.col("origin_id").alias("COD_ORIG").cast("string"),
        f.col("code").alias("COD_COD_TAXA_PGTO").cast("string"),
        f.col("recurring_charge_id").alias("COD_PGTO_RCNT").cast("string"),
        f.col("gateway_order_id").alias("COD_GWAY_PDDO").cast("string"),
        f.col("customer_id").alias("COD_CLI").cast("string"),
        f.col("credit_card_id").alias("COD_CRTO_CRDT").cast("string"),
        f.col("currency").alias("MOE_MOED_CRNT").cast("string"),
        f.col("amount").alias("VLR_VLR_PGTO").cast("float"),
        f.col("payment_method").alias("MTD_MTDO_PGTO").cast("string"),
        f.col("due_date").alias("DAT_DATA_VCTO").cast("timestamp"),
        f.col("instructions").alias("DSC_INTC").cast("string"),
        f.col("status").alias("STA_STTS_CBRN").cast("string"),
        f.col("paid_amount").alias("VLR_VLR_PAGO").cast("float"),
        f.col("paid_at").alias("DAT_DATA_PGTO").cast("timestamp"),
        f.col("removed").alias("IND_RMVD").cast("boolean"),
        f.col("created_by").alias("USR_CRCO").cast("string"),
        f.col("created_date").alias("DAT_DATA_CRCO").cast("timestamp"),
        f.col("last_modified_by").alias("USR_ULTM_MDFO").cast("string"),
        f.col("last_modified_date").alias("DAT_ULTM_MDFO").cast("timestamp")
    ))
    return df_payment_charge
