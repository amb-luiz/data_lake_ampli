from pyspark.sql import functions as f
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    df_student = inputs['df_student']
    df_student = (df_student
        .select(
                f.col("id").alias("COD_ALNO").cast("string"),
                f.col("full_name").alias("NOM_ALNO").cast("string"),
                f.col("phone_country_code").alias("COD_DDI_TLFN").cast("string"),
                f.col("phone_area_code").alias("COD_DDD_TLFN").cast("string"),
                f.col("phone_number").alias("NUM_TLFN").cast("string"),
                f.col("email").alias("EML_ALNO").cast("string"),
                f.col("birth_date").alias("DAT_NSCM").cast("string"),
                f.col("gender").alias("GDR_ALNO").cast("string"),
                f.col("national_identity").alias("LCL_NSCM_ALNO").cast("string"),
                f.col("issuing_authority").alias("ORG_EMSS").cast("string"),
                f.col("national_identity_state_id").alias("COD_NACL_ESTD").cast("integer"),
                f.col("document").alias("NUM_CPF").cast("string"),
                f.col("driver_license").alias("DOC_CNH_ALNO").cast("string"),
                f.col("country_of_birth").alias("CID_NSCM_ALNO").cast("string"),
                f.col("state_of_birth_id").alias("COD_ESTD_NSCM_ALNO").cast("integer"),
                f.col("city_of_birth_id").alias("COD_CDDE_NSCM").cast("integer"),
                f.col("postal_code").alias("NUM_CEP").cast("string"),
                f.col("street").alias("NOM_RUA").cast("string"),
                f.col("address_number").alias("NUM_ENDR").cast("string"),
                f.col("address_complement").alias("DSC_CMPL_ENDR").cast("string"),
                f.col("neighbourhood").alias("NOM_BRRO").cast("string"),
                f.col("city_id").alias("COD_CDDE").cast("integer"),
                f.col("state_id").alias("COD_ESTD").cast("integer"),
                f.col("marital_status").alias("EST_CVIL_ALNO").cast("string"),
                f.col("mother_name").alias("NOM_MAE").cast("string"),
                f.col("father_name").alias("NOM_PAI").cast("string"),
                f.col("guardian_name").alias("REP_LGAL").cast("string"),
                f.col("contract_signed").alias("IND_CNTR_ASNA").cast("boolean"),
                f.col("contract_id").alias("COD_CNTR").cast("string"),
                f.col("contract_url").alias("CTT_URL").cast("string"),
                f.col("removed").alias("IND_RMVD").cast("boolean"),
                f.col("created_by").alias("USR_CRCO").cast("string"),
                f.col("created_date").alias("DAT_CRCO").cast("timestamp"),
                f.col("last_modified_by").alias("USR_ULTM_MDFO").cast("string"),
                f.col("last_modified_date").alias("DAT_ULTM_MDFO").cast("timestamp"),
                f.col("cpf_url").alias("COD_CPF_URL").cast("string"),
                f.col("photo_key").alias("COD_FOTO").cast("string"),
                f.col("bio").alias("DSC_PRFL").cast("string"),
                f.col("declared_race").alias("COR_ALNO").cast("string"),
                f.col("schooling_type").alias("ECL_NVL_INTC_ALNO").cast("string"),
                f.col("phone_carrier").alias("NOM_OPRD_CLLR").cast("string")
                )
        .withColumn("DSC_ENDR_ALNO",f.concat_ws(", ", f.col("NOM_RUA"), f.col("NUM_ENDR")))
    
        .withColumn("DSC_CMPL_ENDR_ALNO",f.concat_ws(", ", f.col("NUM_ENDR"), f.col("DSC_CMPL_ENDR")))
        .withColumn("NUM_IDDE_ALNO", 
                        f.floor(
                            f.datediff(f.current_date(), f.to_date(f.to_date("DAT_NSCM"))) / f.lit(365.25)
                        ))

        .withColumn("NUM_TLFN_ALNO", 
                        f.concat(
                            f.lit('('), f.col("COD_DDD_TLFN"), f.lit(')'), f.col("NUM_TLFN")))
        
        .withColumn("EML_ALNO", 
                                                        f.when(f.col("EML_ALNO").isNull(),
                                                                f.lit("kroton@kroton.com.br"))
                                                        .otherwise(f.col("EML_ALNO")))
        )
    

    return df_student
