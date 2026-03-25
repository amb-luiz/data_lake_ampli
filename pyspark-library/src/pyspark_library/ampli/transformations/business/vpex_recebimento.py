from pyspark.sql import functions as f
import logging

def join_payment_plan(enrollment_course, financial_payment_plan):
    df_vpex_recebimento = (enrollment_course.join(financial_payment_plan,
        (financial_payment_plan['COD_ALNO'] == enrollment_course['COD_ALNO']), "left"))
    return df_vpex_recebimento.drop(enrollment_course['COD_ALNO'])

def join_payment_plan_coup(enrollment_course, financial_payment_plan):
    df_vpex_recebimento = (enrollment_course.join(financial_payment_plan,
        (financial_payment_plan['COD_ALNO'] == enrollment_course['COD_ALNO'])
            & (financial_payment_plan['COD_CPOM'] == 'FTGROWTH21'), "left"))
    return df_vpex_recebimento.drop(enrollment_course['COD_ALNO'])

def join_financial_instalment(df_vpex_recebimento, financial_instalment):
    df_vpex_recebimento = (df_vpex_recebimento.join(financial_instalment,
        financial_instalment['COD_PLNO_PGTO'] == df_vpex_recebimento['COD_PLNO_PGTO'], 'inner'))
    return df_vpex_recebimento.drop(financial_instalment['COD_PLNO_PGTO'])

def join_course(df_vpex_recebimento, course):
    df_vpex_recebimento = (df_vpex_recebimento.join(course,
         course['COD_CURS'] == df_vpex_recebimento['COD_CURS'], 'left'))
    return df_vpex_recebimento.drop(course['COD_CURS'])

def join_financial_instalment_charge(df_vpex_recebimento, financial_instalment_charge):
    df_vpex_recebimento = (df_vpex_recebimento.join(financial_instalment_charge,
         financial_instalment_charge['COD_PRST'] == df_vpex_recebimento['COD_PCLA'], 'left'))
    return df_vpex_recebimento

def join_payment_charge(df_vpex_recebimento, payment_charge):
    df_vpex_recebimento = (
        df_vpex_recebimento.join(payment_charge,
            (payment_charge['COD_TAXA_PGTO'] == df_vpex_recebimento['COD_CBRN'])
                & ((payment_charge['STA_STTS_CBRN'] == f.lit("Pago")) | (df_vpex_recebimento['VLR_PGTO'] == 0)),'left'))
    return df_vpex_recebimento

def join_payment_credit_card(df_vpex_recebimento, payment_credit_card):
    df_vpex_recebimento = (df_vpex_recebimento.join(payment_credit_card,
         payment_credit_card['COD_CRTO_CRDT'] == df_vpex_recebimento['COD_CRTO_CRDT'], 'left'))
    return df_vpex_recebimento.drop(payment_credit_card['COD_CRTO_CRDT'])

def join_coupon(df_vpex_recebimento, coupon):
    df_vpex_recebimento = (df_vpex_recebimento.join(coupon,
         coupon['COD_COD_CPOM'] == df_vpex_recebimento['COD_CPOM_PCLA'], 'left'))
    return df_vpex_recebimento.drop(coupon['NUM_PCLA'])


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):

    enrollment_course = inputs['enrollment_course']
    financial_payment_plan = inputs['financial_payment_plan']
    financial_instalment = inputs['financial_instalment']
    course = inputs['course']
    financial_instalment_charge = inputs['financial_instalment_charge']
    payment_charge = inputs['payment_charge']
    payment_credit_card = inputs['payment_credit_card']
    coupon = inputs['coupon']

    df_vpex_recebimento = join_payment_plan(enrollment_course, financial_payment_plan)
    df_vpex_recebimento = join_payment_plan_coup(enrollment_course, financial_payment_plan)
    df_vpex_recebimento = join_financial_instalment(df_vpex_recebimento, financial_instalment)
    df_vpex_recebimento = join_course(df_vpex_recebimento, course)
    df_vpex_recebimento = join_financial_instalment_charge(df_vpex_recebimento, financial_instalment_charge)
    df_vpex_recebimento = join_payment_charge(df_vpex_recebimento, payment_charge)
    df_vpex_recebimento = join_payment_credit_card(df_vpex_recebimento, payment_credit_card)
    df_vpex_recebimento = join_coupon(df_vpex_recebimento, coupon)


    df_vpex_recebimento = (
        df_vpex_recebimento
        .withColumn("DAT_FIM_TSTE_GRTS", f.when((f.col("COD_ALNO").isNotNull()) &
                                                            (f.col("NUM_PCLA") == f.lit(1)),
                                        f.col("DAT_FIM_TSTE_GRTS"))
                                .otherwise(f.lit(" ")))
        .withColumn("IND_EM_TSTE_GRTS", f.when((f.col("COD_ALNO").isNotNull()) &
                                                                    (f.col("NUM_PCLA") == f.lit(1)) & 
                                                                    (f.col("IND_TSTE_GRTS")),
                                                                    f.lit(True))
                                                        .otherwise(f.lit(False)))
    )

    df_vpex_recebimento = (
        df_vpex_recebimento
        .filter(
            f.col('STA_PGATO_PCLA').isin('Aberto', 'Pendente', 'Cancelado' )
        )
        .select (
            f.col('COD_ALNO'),
            f.col('STA_STCO_INSC_CURS'),
            f.col('DAT_EVSO_CURS'),
            f.col('COD_COD_CURS'),
            f.col('NOM_CURS'),
            f.col('IND_TIP_CURS'),
            f.col('DAT_ENTR_CURS'),
            f.col('DAT_CNCA_FTRM'),
            f.col('NUM_PCLA'),
            f.col('DAT_VCTO_PCLA'),
            f.col('DAT_PGTO_PCLA'),
            f.col("DAT_PGTO_PCLA").alias("DAT_CMPC_PCLA"),
            f.col('MDO_MTDO_PGTO'),
            f.col('STA_PGATO_PCLA'),
            f.col('STA_STTS_CBRN'),
            f.col('VLR_PLNO_PGTO'),
            f.col('VLR_PCLA'),
            f.col('DSC_DESC_PCLA'),
            f.col('COD_CPOM_PCLA'),
            f.col('DSC_CPOM'),
            f.col('DAT_INI_VGNC'),
            f.col('DAT_FIM_VGNC'),
            f.col('TIP_CLCL_DESC'),
            f.col('VLR_TIPO_DESC'),
            f.col('JUR_JURO_PCLA'),
            f.col('VLR_MLTA_PCLA'),
            f.col('VLR_OTRO_ICMT'),
            f.col('VLR_ULTM_ATLZ'),
            f.col('VLR_PGTO'),
            f.col('ORI_TSTE_GRTS'),
            f.col('DAT_FIM_TSTE_GRTS'),
            f.col('IND_EM_TSTE_GRTS'),
            f.col('COD_CBRN'),
            f.col('DAT_CBRN'),
            f.col('COD_CRTO_RCNT'),
            f.col('MRC_MRCA_CRTO')
        )

    )
      
    return df_vpex_recebimento