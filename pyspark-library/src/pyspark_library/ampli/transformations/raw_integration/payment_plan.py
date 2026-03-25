from pyspark.sql import functions as f
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    df_payment_plan = inputs['df_payment_plan']

    df_payment_plan = (
        df_payment_plan
            .select(            
                    f.col("id").alias("COD_PLNO_PGTO").cast("string"),
                    f.col("student_id").alias("COD_ALNO").cast("string"),
                    f.col("recurring_charge_id").alias("COD_CBRN_RCNT").cast("string"),
                    f.col("course_price_id").alias("COD_PRCO_CURS").cast("string"),
                    f.col("amount").alias("VLR_PLNO_PGTO").cast("float"),
                    f.col("billing_day").alias("DAT_DIA_CBRN").cast("integer"),
                    f.col("payment_method").alias("MTD_MTDO_PGTO").cast("string"),
                    f.col("number_of_instalments").alias("NUM_NMRO_PCLA").cast("integer"),
                    f.col("recurring").alias("IND_RCRC").cast("boolean"),
                    f.col("removed").alias("IND_RMVD").cast("boolean"),
                    f.col("created_by").alias("USR_CRCO_PP").cast("string"),
                    f.col("created_date").alias("DAT_CRCO").cast("timestamp"),
                    f.col("last_modified_by").alias("USR_ULTM_MDFO_PP").cast("string"),
                    f.col("last_modified_date").alias("DAT_ULTM_MDFO").cast("timestamp"),
                    f.col("course_id").alias("ID_CURS").cast("string"),
                    f.col("recurring_credit_card_id").alias("COD_CRTO_RCNT").cast("string"),
                    f.col("coupon_code").alias("COD_CPOM").cast("string"),
                    f.col("coupon_number_of_instalments").alias("NUM_CPOM_PCLA").cast("integer"),
                    f.col("input_coupon_code").alias("NOM_COD_CPOM").cast("string"),
                    f.col("input_coupon_number_of_instalments").alias("NUM_NMRO_CPOM_PCLA").cast("integer"))
            .withColumn("ORI_TSTE_GRTS", f.when(f.col("COD_ALNO").isNull(), 
                                                f.lit("SIM"))
                                            .otherwise(f.lit("NAO")))    
        )
    return df_payment_plan
