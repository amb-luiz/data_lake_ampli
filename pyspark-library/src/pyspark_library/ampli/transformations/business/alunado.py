from pyspark.sql import functions as f
import logging


def join_enrollment_course(student, enrollment_course):
    """student_student join enrollment_course on std.COD_ALUNO = eco.COD_ALUNO"""
    df_alunado = (student.join(enrollment_course,
                               student['COD_ALNO'] == enrollment_course['COD_ALNO'], "inner"))
    return df_alunado.drop(enrollment_course['COD_ALNO'])


def join_course(df_alunado, course):
    df_alunado = (df_alunado.join(course,
                                  df_alunado['COD_CURS'] == course['COD_CURS'], "inner"))
    return df_alunado.drop(course['COD_CURS'])


def join_city(df_alunado, locale_city):
    df_alunado = (df_alunado.join(locale_city,
                                  df_alunado['COD_CDDE_NSCM'] == locale_city['COD_CDDE'], "inner"))
    return df_alunado.drop(locale_city['COD_CDDE'])


def join_state(df_alunado, locale_state):
    df_alunado = (df_alunado.join(locale_state,
                                  df_alunado['COD_ESTD_NSCM_ALNO'] == locale_state['COD_ESTD'], "inner"))
    return df_alunado.drop(locale_state['COD_ESTD'])


def join_payment_plan(df_alunado, payment_plan):
    df_alunado = (df_alunado.join(payment_plan,
                                  df_alunado['COD_ALNO'] == payment_plan['COD_ALNO'], "left"))
    return df_alunado.drop(payment_plan['COD_ALNO'])


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    enrollment_course = inputs['enrollment_course']
    course = inputs['course']
    locale_city = inputs['locale_city']
    locale_state = inputs['locale_state']
    financial_payment_plan = inputs['financial_payment_plan']
    student = inputs['student']

    df_alunado = join_enrollment_course(student, enrollment_course)
    df_alunado = join_course(df_alunado, course)
    df_alunado = join_city(df_alunado, locale_city)
    df_alunado = join_state(df_alunado, locale_state)
    df_alunado = join_payment_plan(df_alunado, financial_payment_plan)
    
    df_alunado = (df_alunado.withColumn("LCL_ENDR_ALNO", f.concat_ws(' - ', f.col("DSC_ENDR_ALNO"), f.col("NOM_ESTD"),f.col("NOM_CDDE")))
    .withColumn("NUM_CURS_EM_MSES", f.substring(f.col("DSC_DURC_CURS"), 2, 2))
    )
    
    df_alunado = (
        df_alunado
            .withColumn("DAT_FIM_TSTE_GRTS", f.date_add(f.to_date("SIT_STTS_DATA"),30))
            .withColumn("IND_TSTE_GRTS", f.when(f.to_date(f.col("DAT_FIM_TSTE_GRTS")) < f.current_date(),
                                                           f.lit(True))
                                                .otherwise(f.lit(False)))  
    )
    
    df_alunado = (df_alunado
                        .filter(f.col("COD_CPOM") == f.lit("FTGROWTH21"))
                        .select("COD_ALNO",
                                "NOM_ALNO",
                                "NUM_CPF",
                                "NUM_IDDE_ALNO",
                                "NUM_TLFN_ALNO",
                                "EML_ALNO",
                                "LCL_ENDR_ALNO",
                                "STA_STCO_INSC_CURS",
                                "DAT_ULTM_STCO_INSC_CURS",
                                "DAT_ENTR_CURS",
                                "DAT_EVSO_CURS",
                                "COD_CURS",
                                "NOM_CURS",
                                "DAT_ESTM_FRMR",
                                "IND_TIP_CURS",
                                "NUM_CURS_EM_MSES",
                                "DAT_FIM_TSTE_GRTS",
                                "IND_TSTE_GRTS",
                                "ORI_TSTE_GRTS"))

    return df_alunado
