from pyspark.sql import functions as f
import logging

def join_payment_plan(enrollment_course, financial_payment_plan):
    df_vpex_nfse_capa = (enrollment_course.join(financial_payment_plan, 
        financial_payment_plan['COD_ALNO'] == enrollment_course['COD_ALNO'], "inner"))
    return df_vpex_nfse_capa.drop(enrollment_course['COD_ALNO'])

def join_financial_instalment(df_vpex_nfse_capa, financial_instalment):
    df_vpex_nfse_capa = (df_vpex_nfse_capa.join(financial_instalment,
        financial_instalment['COD_PLNO_PGTO'] == df_vpex_nfse_capa['COD_PLNO_PGTO'], 'inner'))
    return df_vpex_nfse_capa.drop(financial_instalment['COD_PLNO_PGTO']).drop(financial_instalment['NOM_CDDE'])

def join_student(df_vpex_nfse_capa, student):
    df_vpex_nfse_capa = (df_vpex_nfse_capa.join(student,
         student['COD_ALNO'] == df_vpex_nfse_capa['COD_ALNO'], 'inner'))
    return df_vpex_nfse_capa.drop(student['COD_ALNO']).drop(student['COD_ESTD'])

def join_city(df_vpex_nfse_capa, locale_city):
    df_vpex_nfse_capa = (df_vpex_nfse_capa.join(locale_city,
         locale_city['COD_CDDE'] == df_vpex_nfse_capa['COD_CDDE'], 'left'))
    return df_vpex_nfse_capa.drop(locale_city['COD_CDDE'])

def join_state(df_vpex_nfse_capa, locale_state):
    df_vpex_nfse_capa = (df_vpex_nfse_capa.join(locale_state,
         locale_state['COD_ESTD'] == df_vpex_nfse_capa['COD_ESTD'], 'left'))
    return df_vpex_nfse_capa.drop(locale_state['COD_ESTD'])

def join_coupon(df_vpex_nfse_capa, coupon):
    df_vpex_nfse_capa = (df_vpex_nfse_capa.join(coupon,
         coupon['COD_COD_CPOM'] == df_vpex_nfse_capa['COD_CPOM_PCLA'], 'left'))
    return df_vpex_nfse_capa

def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):

    enrollment_course = inputs['enrollment_course']
    financial_payment_plan = inputs['financial_payment_plan']
    financial_instalment = inputs['financial_instalment']
    student = inputs['student']
    locale_city = inputs['locale_city']
    locale_state = inputs['locale_state']
    coupon = inputs['coupon']

    df_vpex_nfse_capa = join_payment_plan(enrollment_course, financial_payment_plan)
    df_vpex_nfse_capa = join_financial_instalment(df_vpex_nfse_capa, financial_instalment)
    df_vpex_nfse_capa = join_student(df_vpex_nfse_capa, student)
    df_vpex_nfse_capa = join_city(df_vpex_nfse_capa, locale_city)
    df_vpex_nfse_capa = join_state(df_vpex_nfse_capa, locale_state)
    df_vpex_nfse_capa = join_coupon(df_vpex_nfse_capa, coupon)

    df_vpex_nfse_capa = (
        df_vpex_nfse_capa
        .withColumn("COD_UNDE", f.lit("AMP1"))
        .withColumn("COD_CDDE", f.lit(""))
        .withColumn("IND_TIP_PSSA", f.lit(""))
        .withColumn("INSC_ESTD", f.lit(""))
        .withColumn("INSC_MNCP", f.lit(""))
        .withColumn("EML_RSPL", f.lit(""))
        .withColumn("CTRO_CUST", f.lit("50010005809"))
        .withColumn("DAT_EMSS", f.lit(""))
        .withColumn("COD_CURS", f.lit("03"))
    )

    df_vpex_nfse_capa = (
        df_vpex_nfse_capa
        .filter(
            (f.col("STA_STCO_INSC_CURS") =='Ativo') &
            (f.col("STA_PCLA") != 'Cancelado')
        )
        .select(
            f.col('COD_UNDE'),
            f.col('NUM_CPF'),
            f.col('NOM_ALNO'),
            f.col('DSC_ACNM'),
            f.col('COD_CDDE'),
            f.col('NOM_CDDE'),
            f.col('NOM_BRRO'),
            f.col('NOM_RUA'),
            f.col('NUM_ENDR'),
            f.col('COD_DDD_TLFN'),
            f.col('NUM_TLFN'),
            f.col('NUM_CEP'),
            f.col('DSC_CMPL_ENDR_ALNO'),
            f.col('EML_ALNO'),
            f.col('IND_TIP_PSSA'),
            f.col('INSC_ESTD'),
            f.col('INSC_MNCP'),
            f.col('DAT_NSCM'),
            f.col('REP_LGAL'),
            f.col('EML_RSPL'),
            f.col('COD_PCLA'),
            f.col('CTRO_CUST'),
            f.col('DAT_EMSS'),
            f.col('DAT_VCTO_PCLA'),
            f.col('DAT_PGTO_PCLA'),
            f.col('DAT_VCTO_RNVD'),
            f.col('DAT_CNCA_FTRM'),
            f.col('COD_CURS'),
            f.col('QTD_ITEM'),
            f.col('VLR_CLCO_DESC'),
            f.col('STA_STCO_INSC_CURS'),
            f.col('STA_PGATO_PCLA'),
            f.col('COD_CPOM_PCLA'),
            f.col('DSC_CPOM')
        )
    )

    return df_vpex_nfse_capa