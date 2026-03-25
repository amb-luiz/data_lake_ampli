from pyspark.sql import functions as f
import logging


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    """       (CASE
            WHEN ece.status = 'ATTENDING' THEN 'ATIVO'
            WHEN ece.status = 'CANCELLED' THEN 'CANCELADO'
            WHEN ece.status = 'SUSPENDED' THEN 'TRANCADO'
            WHEN ece.status = 'ABANDONED' THEN 'ABANDONADO'
            WHEN ece.status = 'TRANSFERRED' THEN 'TRANSFERIDO'
            WHEN ece.status = 'FINISHED' THEN 'FORMADO'
            ELSE ece.status
            END) AS 'Situação', - Alunado'"""

    """
              (CASE
           WHEN ece.status = 'SUSPENDED' THEN ece.suspension_start_date
           WHEN ece.status = 'CANCELLED'
                OR ece.status = 'ABANDONED' THEN ece.last_modified_date
           END AS 'Data Evasão', - Alunado)"""

    """    DATE_ADD(ece.start_date, INTERVAL 30 DAY) AS "Data Final do Free trial",
           IF(DATE_ADD(ece.start_date, INTERVAL 30 DAY) > CURDATE(), 'SIM','NAO') AS "Em Trial" – Alunado"""   

    df_enrollment_course = inputs['df_enrollment_course']

    logging.info("Executando transformações na tabela Enrollment Course")

    df_enrollment_course = (df_enrollment_course.select(
        f.col("id").alias("COD_INSC_CURS").cast("string"),
        f.col("student_id").alias("COD_ALNO").cast("string"),
        f.col("course_id").alias("COD_CURS").cast("string"),
        f.col("start_date").alias("DAT_INI_INSC_CURS").cast("timestamp"),
        f.col("end_date").alias("DAT_FIM_INSC_CURS").cast("timestamp"),
        f.col("status").alias("STA_STTS_INSC_CURS").cast("string"),
        f.col("removed").alias("IND_RMVD").cast("boolean"),
        f.col("created_by").alias("USR_CRCO_INSC_CURS").cast("string"),
        f.col("created_date").alias("DAT_ENTR_CURS").cast("string"),
        f.col("last_modified_by").alias("USR_ULTM_MDFO_INSC_CURS").cast("string"),
        f.col("last_modified_date").alias("DAT_ULTM_STCO_INSC_CURS"	).cast("string"),
        f.col("suspension_start_date").alias("DAT_INI_SSPO_CURS").cast("string"),
        f.col("suspension_end_date").alias("DAT_FIM_SSPO_CURS").cast("string"),
        f.col("estimated_end_date").alias("DAT_ESTM_FRMR").cast("string"),
        f.col("reason_code").alias("COD_RZAO_INSC_CURS").cast("string"),
        f.col("status_date").alias("SIT_STTS_DATA").cast("string"),
        f.col("transferred_to").alias("SIT_TRFC_PARA").cast("string"),
        f.col("transferred_from").alias("SIT_TRFC_DE").cast("string"),
        f.col("student_reason_elaboration").alias("DSC_ELAB_ALNO").cast("string")
    )
        .withColumn("DAT_EVSO_CURS",
                    f.when(
                        f.col("STA_STTS_INSC_CURS") == f.lit(
                            "SUSPENDED"),
                        f.lit(f.col("DAT_INI_SSPO_CURS")))
                    .when(
                        f.col("STA_STTS_INSC_CURS").isin("CANCELLED","ABANDONED"),
                        f.lit(f.col("DAT_ULTM_STCO_INSC_CURS")))
                    .otherwise(f.lit("")))

        .withColumn("STA_STCO_INSC_CURS",
                    f.when(
                        f.col("STA_STTS_INSC_CURS") == f.lit(
                            "ATTENDING"),
                        f.lit("Ativo"))
                    .when(
                        f.col("STA_STTS_INSC_CURS") == f.lit(
                            "CANCELLED"),
                        f.lit("Cancelado"))
                    .when(
                        f.col("STA_STTS_INSC_CURS") == f.lit(
                            "SUSPENDED"),
                        f.lit("Trancado"))
                    .when(
                        f.col("STA_STTS_INSC_CURS") == f.lit(
                            "ABANDONED"),
                        f.lit("Abandonado"))
                    .when(
                        f.col("STA_STTS_INSC_CURS") == f.lit(
                            "TRANSFERRED"),
                        f.lit("Transferido"))
                    .when(
                        f.col("STA_STTS_INSC_CURS") == f.lit(
                            "FINISHED"),
                        f.lit("Formado"))
                    .otherwise(f.col("STA_STTS_INSC_CURS")))
        
        .withColumn("DAT_FIM_TSTE_GRTS", f.date_add(f.to_date("SIT_STTS_DATA"),30))
        .withColumn("IND_TSTE_GRTS", f.when(f.to_date(f.col("DAT_FIM_TSTE_GRTS")) < f.current_date(),
                                                           f.lit(True))
                                                .otherwise(f.lit(False)))    
        )

    return df_enrollment_course