from pyspark.sql import functions as f
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    
    df_payment_credit_card = inputs['df_payment_credit_card']

    logging.info("Executando transformações na tabela Payment Credit Card")

    df_payment_credit_card = (df_payment_credit_card.select(
        f.col("id").alias("COD_CRTO_CRDT").cast("string"),
        f.col("customer_id").alias("COD_CLI").cast("string"),
        f.col("holder_name").alias("NOM_PRTD").cast("string"),
        f.col("holder_document").alias("DOC_PRTD").cast("timestamp"),
        f.col("first_six_digits").alias("NUM_SEIS_PRMR_DIGT").cast("string"),
        f.col("last_four_digits").alias("NUM_ULTM_QTRO_DIGT").cast("boolean"),
        f.col("brand").alias("MRC_MRCA_CRTO").cast("string"),
        f.col("expiration_month").alias("NUM_MES_EXPR").cast("string"),
        f.col("expiration_year").alias("NUM_ANO_EXPR").cast("string"),
        f.col("status").alias("STA_STTS_FORM_PGMT_CRDT").cast("string"),
        f.col("address_postal_code").alias("END_CEP").cast("string"),
        f.col("address_street").alias("END_ENDR").cast("string"),
        f.col("address_number").alias("END_NMRO").cast("string"),
        f.col("address_complement").alias("END_CMPL").cast("string"),
        f.col("address_neighbourhood").alias("END_BRRO").cast("string"),
        f.col("address_city").alias("END_CDDE").cast("string"),
        f.col("address_state").alias("END_ESTD").cast("string"),
        f.col("address_country").alias("END_PAIS").cast("string"),
        f.col("removed").alias("IND_RMVD_FORM_PGMT_CRDT").cast("string"),
        f.col("created_by").alias("USR_CRCO_FORM_PGMT_CRDT").cast("string"),
        f.col("created_date").alias("DAT_ENTR_FORM_PGMT_CRDT").cast("string"),
        f.col("last_modified_by").alias("USR_ULTM_MDFO_FORM_PGMT").cast("string"),
        f.col("last_modified_date").alias("DAT_ULTM_STCO_FORM_PGMT").cast("string")
    ))
       
    return df_payment_credit_card