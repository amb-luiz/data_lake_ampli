from pyspark.sql import functions as f
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    """       CASE
        WHEN i.status = 'PAID' THEN 'Pago'
        WHEN i.status = 'OPEN' THEN 'Aberto'
        WHEN i.status = 'PENDING' THEN 'Pendente'
        WHEN i.status = 'CANCELLED' THEN 'Cancelado'
        WHEN i.status = 'WROTE_OFF' THEN 'Abonado'
        ELSE i.status
    END AS 'Status Pgto'"""

    df_financial_instalment = inputs['df_financial_instalment']

    logging.info("Executando transformações na tabela Financial Instalment")

    df_financial_instalment = (df_financial_instalment.select(
        f.col("id").alias("COD_PCLA").cast("integer"),
        f.col("payment_plan_id").alias("COD_PLNO_PGTO").cast("string"),
        f.col("instalment_number").alias("NUM_PCLA").cast("integer"),
        f.col("payment_method").alias("MDO_MTDO_PGTO").cast("string"),
        f.col("amount").alias("VLR_PCLA").cast("double"),
        f.col("discount").alias("DSC_DESC_PCLA").cast("double"),
        f.col("interest").alias("JUR_JURO_PCLA").cast("double"),
        f.col("due_date").alias("DAT_VCTO_PCLA").cast("timestamp"),
        f.col("type").alias("DSC_TIPO_CBRN").cast("string"),
        f.col("boleto_html").alias("COD_BLQT_HTML").cast("string"),
        f.col("boleto_bar_code").alias("COD_BARA_BLQT").cast("string"),
        f.col("boleto_qr_code").alias("COD_QR_BLQT").cast("string"),
        f.col("boleto_code").alias("COD_BLQT").cast("string"),
        f.col("boleto_pdf").alias("DOC_BLQT_PDF").cast("string"),
        f.col("status").alias("STA_PCLA").cast("string"),
        f.col("removed").alias("IND_PCLA_RMVD").cast("boolean"),
        f.col("created_by").alias("USR_CRCO_PCLA").cast("string"),
        f.col("created_date").alias("DAT_CRCO_PCLA").cast("timestamp"),
        f.col("last_modified_by").alias("USR_ULTM_MDFO_PCLA").cast("string"),
        f.col("last_modified_date").alias("DAT_ULTM_MDFO_PCLA").cast("timestamp"),
        f.col("coupon_code").alias("COD_CPOM_PCLA").cast("string"),
        f.col("fine").alias("VLR_MLTA_PCLA").cast("double"),
        f.col("credit_card_id").alias("COD_CRTO_CRDT_PCLA").cast("string"),
        f.col("city_name").alias("NOM_CDDE").cast("string"),
        f.col("state_acronym").alias("SGL_UF").cast("string"),
        f.col("renewed_due_date").alias("DAT_VCTO_RNVD").cast("timestamp"),
        f.col("paid_at").alias("DAT_PGTO_PCLA").cast("timestamp"),
        f.col("paid_amount").alias("VLR_PGTO").cast("double"),
        f.col("reason").alias("DSC_PGTO").cast("string"),
        f.col("other_increment").alias("VLR_OTRO_ICMT").cast("double")
    )
        .withColumn("STA_PGATO_PCLA",
                    f.when(
                        f.col("STA_PCLA") == ("PAID"),
                        f.lit("Pago"))
                    .when(
                        f.col("STA_PCLA") == f.lit("OPEN"),
                        f.lit("Aberto"))
                    .when(
                        f.col("STA_PCLA") == f.lit("PENDING"),
                        f.lit("Pendente"))
                    .when(
                        f.col("STA_PCLA") == f.lit("CANCELLED"),
                        f.lit("Cancelado"))
                    .when(
                        f.col("STA_PCLA") == f.lit("WROTE_OFF"),
                        f.lit("Abonado"))
                    .otherwise(f.col("STA_PCLA")))
        .withColumn("DAT_CNCA_FTRM", f.concat_ws("/", f.month(f.col("DAT_VCTO_PCLA")), f.year(f.col("DAT_VCTO_PCLA"))))
        .withColumn("VLR_ULTM_ATLZ",
                    (f.col("VLR_PCLA") - f.col("DSC_DESC_PCLA") + f.col("JUR_JURO_PCLA") + f.col("VLR_OTRO_ICMT")))
        .withColumn("VLR_CLCO_DESC", f.when(((f.col("VLR_PCLA") - f.col("DSC_DESC_PCLA")) == f.lit(0)),
                                                   f.lit(0.01))
                    .otherwise((f.col("VLR_PCLA") - f.col("DSC_DESC_PCLA"))))
        .withColumn("QTD_ITEM",
                  f.when(f.col("DSC_DESC_PCLA") == 0, 1).otherwise(2))

    )

    return df_financial_instalment
