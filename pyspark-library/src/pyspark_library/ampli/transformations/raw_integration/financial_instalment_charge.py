from pyspark.sql import functions as f
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    df_financial_instalment_charge = inputs['df_financial_instalment_charge']

    logging.info(
        "Executando transformações na tabela Financial Instalment Charge")

    df_financial_instalment_charge = (df_financial_instalment_charge.select(
        f.col("id").alias("COD_TAXA_PCLA_FNCR").cast("string"),
        f.col("instalment_id").alias("COD_PRST").cast("float"),
        f.col("status").alias("STA_ALNO_FNCR").cast("string"),
        f.col("charge_id").alias("COD_CBRN").cast("string"),
        f.col("created_date").alias("DAT_CBRN").cast("timestamp"),
        f.col("due_date").alias("DAT_VCTO").cast("timestamp"),
        f.col("payment_method").alias("MTD_MTDO_PGTO").cast("string"),
        f.col("updated_date").alias("DAT_ATLZ").cast("timestamp")
    ))
    return df_financial_instalment_charge
