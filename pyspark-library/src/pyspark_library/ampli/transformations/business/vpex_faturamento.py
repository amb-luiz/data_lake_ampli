from pyspark.sql import functions as f
import logging

"""
SELECT ce.COD_ALUNO,
       c.cod_codigo_curso,
       c.nom_curso,
       c.tip_tipo_curso,
       ce.dat_entrada_curso,
       i.dat_vencimento_parcela,
       i.num_parcela,
       i.mdo_metodo_pagamento,
       i.vlr_parcela,
       i.dsc_desconto_parcela,
       i.cod_cupom_parcela,
       coup.dsc_cupom,
       coup.dat_inicio_vigencia,
       coup.dat_fim_vigencia,
       coup.tip_calculo_desconto,
       coup.vlr_tipo_desconto,
       i.jur_juros_parcela,
       i.vlr_multa_parcela,
       i.vlr_outros_incrementos,
       i.vlr_pagamento,
       i.dat_pagamento_parcela'
FROM enrollment.course_enrollment ce
LEFT JOIN financial.payment_plan trial ON trial.COD_ALUNO = ce.COD_ALUNO
AND trial.COD_PARCELA = 'FTGROWTH21'
LEFT JOIN financial.payment_plan pp ON ce.COD_ALUNO = pp.COD_ALUNO
INNER JOIN financial.instalment i ON i.COD_PLANO_PAGAMENTO = pp.COD_PLANO_PAGAMENTO
LEFT JOIN course.course c ON ce.COD_CURSO = c.COD_CURSO
LEFT JOIN financial.instalment_charge ic ON ic.cod_prestacao = i.COD_PARCELA
LEFT JOIN coupon.coupon coup ON i.COD_PARCELA = coup.cod_codigo_cupom
 WHERE (MONTH(i.dat_vencimento_parcela) IN ({{ Meses }})
     AND YEAR(i.dat_vencimento_parcela) IN ({{ Ano }}))
"""


def join_payment_plan(enrollment_course, payment_plan):
    df_vpex_faturamento = (
        enrollment_course.join(payment_plan,
                               (payment_plan['COD_ALNO'] ==
                                enrollment_course['COD_ALNO'])
                               & (payment_plan['COD_CPOM'] == 'FTGROWTH21'), how='left')
    )
    df_vpex_faturamento = (
        enrollment_course.join(payment_plan,
                               enrollment_course['COD_ALNO'] == payment_plan['COD_ALNO'], how='left')
    )

    return df_vpex_faturamento.drop(payment_plan['COD_ALNO'])


def join_financial_instalment(instalment, df_vpex_faturamento):
    df_vpex_faturamento = (df_vpex_faturamento.join(instalment,
                                                    df_vpex_faturamento['COD_PLNO_PGTO'] == instalment['COD_PLNO_PGTO'], "inner")
                           )
    return df_vpex_faturamento.drop(instalment['COD_PLNO_PGTO'])


def join_course_course(df_vpex_faturamento, course_course):
    df_vpex_faturamento = (
        df_vpex_faturamento.join(course_course,
                                 df_vpex_faturamento['COD_CURS'] == course_course['COD_CURS'], how='left')
    )
    return df_vpex_faturamento.drop(course_course['COD_CURS'])


def join_financial_instalment_charge(df_vpex_faturamento, instalment_charge):
    df_vpex_faturamento = (df_vpex_faturamento.join(instalment_charge,
                                                    df_vpex_faturamento['COD_PCLA'] == instalment_charge['COD_PRST'], "left"))
    return df_vpex_faturamento.drop(instalment_charge['COD_PRST'])


def join_coupon_coupon(df_vpex_faturamento, coupon_coupon):
    df_vpex_faturamento = (df_vpex_faturamento.join(coupon_coupon,
                                                    df_vpex_faturamento['COD_PCLA'] == coupon_coupon['COD_COD_CPOM'], "left"))

    return df_vpex_faturamento.drop(coupon_coupon['COD_COD_CPOM']).drop(coupon_coupon['NUM_PCLA'])


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    payment_plan = inputs['payment_plan']
    instalment = inputs['instalment']
    course_course = inputs['course_course']
    instalment_charge = inputs['instalment_charge']
    coupon_coupon = inputs['coupon_coupon']
    enrollment_course = inputs['enrollment_course']

    df_vpex_faturamento = join_payment_plan(enrollment_course, payment_plan)
    df_vpex_faturamento = join_financial_instalment(
        instalment, df_vpex_faturamento)
    df_vpex_faturamento = join_course_course(
        df_vpex_faturamento, course_course)
    df_vpex_faturamento = join_financial_instalment_charge(
        df_vpex_faturamento, instalment_charge)
    df_vpex_faturamento = join_coupon_coupon(
        df_vpex_faturamento, coupon_coupon)
    
    
    df_vpex_faturamento = (df_vpex_faturamento.withColumn("DAT_FIM_TSTE_GRTS", f.when((f.col("COD_ALNO").isNotNull()) & 
                                                            (f.col("NUM_PCLA") == f.lit(1)),
                                        f.col("DAT_FIM_TSTE_GRTS"))
                                .otherwise(f.lit(" "))))
    
    
    df_vpex_faturamento = (df_vpex_faturamento.withColumn("IND_EM_TSTE_GRTS", f.when((f.col("COD_ALNO").isNotNull()) &
                                                                (f.col("NUM_PCLA") == f.lit(1)) & 
                                                                (f.col("IND_TSTE_GRTS")),
                                                                f.lit(True))
                                                        .otherwise(f.lit(False))))   
    

    df_vpex_faturamento = (
        df_vpex_faturamento.select(
            f.col('COD_ALNO'),
            f.col('STA_STCO_INSC_CURS'),
            f.col('DAT_EVSO_CURS'),
            f.col('COD_COD_CURS'),
            f.col('NOM_CURS'),
            f.col('IND_TIP_CURS'),
            f.col('DAT_ENTR_CURS'),
            f.col('DAT_VCTO_PCLA'),
            f.col('DAT_CNCA_FTRM'),
            f.col('NUM_PCLA'),
            f.col('MDO_MTDO_PGTO'),
            f.col('STA_PGATO_PCLA'),
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
            f.col('DAT_PGTO_PCLA')
        )
    )

    return df_vpex_faturamento
