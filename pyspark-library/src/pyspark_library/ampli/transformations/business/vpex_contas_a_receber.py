from pyspark.sql import functions as f
"""
VPEX CONTAS A RECEBER

SELECT DISTINCT ce.cod_aluno,
        ce.sta_situacao_insc_curso,
        ce.dat_evasao_curso,
        c.cod_curso,
        c.nom_curso,
        c.tip_tipo_curso,
        ce.dat_entrada_curso,
        i.dat_competencia_faturamento,
        i.NUM_PARCELA,
        i.dat_vencimento_parcela,
        i.dat_pagamento_parcela AS "data_Pagamento",
        i.dat_pagamento_parcela AS "data_de_baixa",
        i.mdo_metodo_pagamento,
        i.sta_pagamento_parcela,
        pc.sta_status_cobranca,
        pp.vlw_plano_pagamento,
        i.vlr_parcela,
        i.dsc_parcela,
        i.COD_CUPOM_PARCELA,
        coup.dsc_cupom,
        coup.dat_inicio_vigencia,
        coup.dat_fim_vigencia,
        coup.tip_calculo_desconto,
        coup.vlr_tipo_desconto,
        i.jur_juros_parcela,
        i.vlr_multa_parcela,
        i.vlr_outros_incrementos,
        i.vlr_ultima_atualizacao,
        i.vlr_pagamento,
        pp.ori_teste_gratis,
    IF(trial.cod_aluno IS NOT NULL AND i.NUM_PARCELA = 1, DATE_ADD(ce.start_date, INTERVAL 30 DAY),null) AS "Data Final do Free trial",
    IF(trial.cod_aluno IS NOT NULL and i.NUM_PARCELA = 1 and DATE_ADD(ce.start_date, INTERVAL 30 DAY) > CURDATE(), 'SIM','NAO') AS "Em Trial",
        pp.cod_cartao_recorrente, cc.mrc_marca_cartao
FROM enrollment_course ce
LEFT JOIN financial_payment_plan pp
    ON ce.cod_aluno = pp.cod_aluno
LEFT JOIN financial_payment_plan trial
    ON trial.cod_aluno = ce.cod_aluno
        AND trial.COD_CUPOM = 'FTGROWTH21'
LEFT JOIN financial_instalment i
    ON i.COD_PLANO_PAGAMENTO = pp.COD_PLANO_PAGAMENTO
LEFT JOIN course_course c
    ON ce.cod_curso = c.cod_curso
LEFT JOIN financial_instalment_charge ic
    ON ic.COD_PRESTACAO = i.COD_PARCELA
LEFT JOIN payment_charge pc
    ON ic.cod_taxa_parcela_financeira = pc.COD_TAXA_PAGAMENTO
LEFT JOIN payment_credit_card cc
    ON cc.COD_CARTAO_CREDITO = pc.COD_CARTAO_CREDITO
LEFT JOIN coupon_coupon coup
    ON i.COD_CUPOM_PARCELA = coup.COD_CODIGO_CUPOM
WHERE i.sta_pagamento_parcela IN ('Aberto','Pendente')
ORDER BY  i.NUM_PARCELA 
"""

def join_financial_payment_plan(enrollment_course, payment_plan):
    """LEFT JOIN financial.payment_plan pp ON ce.cod_aluno = pp.cod_aluno"""
    df_contas_a_receber = (enrollment_course.join(payment_plan, 
                               (payment_plan['COD_ALNO'] == enrollment_course['COD_ALNO']), "left"))
    df_contas_a_receber = (enrollment_course.join(payment_plan, 
                               (payment_plan['COD_ALNO'] == enrollment_course['COD_ALNO']) 
                                & (payment_plan['COD_CPOM'] == 'FTGROWTH21'), "left"))  
    return df_contas_a_receber.drop(payment_plan['COD_ALNO'])
                            
def join_financial_instalment(df_contas_a_receber, instalment):
    """LEFT JOIN financial.instalment i ON i.COD_PLANO_PAGAMENTO = pp.COD_PLANO_PAGAMENTO"""
    df_contas_a_receber = (df_contas_a_receber.join(instalment, 
                               df_contas_a_receber['COD_PLNO_PGTO'] == instalment['COD_PLNO_PGTO'], "left"))
    return df_contas_a_receber.drop(instalment['COD_PLNO_PGTO'])

def join_course(df_contas_a_receber, course):
    """LEFT JOIN course.course c ON ce.cod_curso = c.cod_curso"""
    df_contas_a_receber = (df_contas_a_receber.join(course, 
                                 df_contas_a_receber['COD_CURS'] == course['COD_CURS'], "left"))
    return df_contas_a_receber.drop(course['COD_CURS'])

def join_instalment_charge(df_contas_a_receber, instalment_charge):
    """LEFT JOIN financial.instalment_charge ic ON ic.COD_PRESTACAO = i.COD_PARCELA"""
    df_contas_a_receber = (df_contas_a_receber.join(instalment_charge, 
                               df_contas_a_receber['COD_PCLA'] == instalment_charge['COD_PRST'], "left"))
    return df_contas_a_receber.drop(instalment_charge['COD_PRST'])

def join_payment_charge(df_contas_a_receber, payment_charge):
    """LEFT JOIN payment.charge pc on ic.COD_COBRANCA = pc.COD_TAXA_PAGAMENTO"""
    df_contas_a_receber = (df_contas_a_receber.join(payment_charge, 
                               df_contas_a_receber['COD_CBRN'] == payment_charge['COD_TAXA_PGTO'], "left"))
    return df_contas_a_receber.drop(payment_charge['COD_TAXA_PGTO'])
    
def join_payment_credit_card(df_contas_a_receber, payment_credit_card):
    """LEFT JOIN payment.credit_card cc on cc.COD_CARTAO_CREDITO = pc.COD_CARTAO_CREDITO"""
    df_contas_a_receber = (df_contas_a_receber.join(payment_credit_card, 
                                 df_contas_a_receber['COD_CRTO_CRDT'] == payment_credit_card['COD_CRTO_CRDT'], "left"))
    return df_contas_a_receber.drop(payment_credit_card['COD_CRTO_CRDT'])

def join_coupon(df_contas_a_receber, coupon):
    """LEFT JOIN coupon.coupon coup ON i.COD_CUPOM_PARCELA = coup.COD_CODIGO_CUPOM"""
    df_contas_a_receber = (df_contas_a_receber.join(coupon, 
                                 df_contas_a_receber['COD_CPOM_PCLA'] == coupon['COD_COD_CPOM'], "left"))
    return df_contas_a_receber.drop(coupon['COD_COD_CPOM']).drop(coupon['NUM_PCLA'])

def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    enrollment_course = inputs['enrollment_course']
    payment_plan = inputs['payment_plan']
    instalment = inputs['instalment']
    course = inputs['course']
    instalment_charge = inputs['instalment_charge']
    payment_charge = inputs['payment_charge']
    payment_credit_card = inputs['payment_credit_card']
    coupon = inputs['coupon']

    df_contas_a_receber = join_financial_payment_plan(enrollment_course, payment_plan)
    df_contas_a_receber = join_financial_instalment(df_contas_a_receber, instalment)
    df_contas_a_receber = join_course(df_contas_a_receber, course)
    df_contas_a_receber = join_instalment_charge(df_contas_a_receber, instalment_charge)
    df_contas_a_receber = join_payment_charge(df_contas_a_receber, payment_charge)
    df_contas_a_receber = join_payment_credit_card(df_contas_a_receber, payment_credit_card)
    df_contas_a_receber = join_coupon(df_contas_a_receber, coupon)

    """ IF(trial.cod_aluno IS NOT NULL AND i.NUM_PARCELA = 1, 
            DATE_ADD(ce.start_date, INTERVAL 30 DAY),
    null) 
    AS "Data Final do Free trial",
    """
    df_contas_a_receber = (df_contas_a_receber.withColumn("DAT_FIM_TSTE_GRTS", f.when((f.col("COD_ALNO").isNotNull()) & 
                                                            (f.col("NUM_PCLA") == f.lit(1)),
                                        f.col("DAT_FIM_TSTE_GRTS"))
                                .otherwise(f.lit(" "))))

    """IF(trial.cod_aluno IS NOT NULL and i.NUM_PARCELA = 1 and DATE_ADD(ce.start_date, INTERVAL 30 DAY) > '<' CURDATE(), 
        'SIM',
    'NAO') AS "Em Trial"""
    df_contas_a_receber = (df_contas_a_receber.withColumn("IND_EM_TSTE_GRTS", f.when((f.col("COD_ALNO").isNotNull()) &
                                                                    (f.col("NUM_PCLA") == f.lit(1)) & 
                                                                    (f.col("IND_TSTE_GRTS")),
                                                                    f.lit(True))
                                                        .otherwise(f.lit(False))))                
    
    df_contas_a_receber = (
        df_contas_a_receber
            .filter(f.col("STA_PGATO_PCLA").isin('Aberto', 'Pendente'))
            .select(f.col("COD_ALNO"),
                    f.col("STA_STCO_INSC_CURS"),
                    f.col("DAT_EVSO_CURS"),
                    f.col("COD_CURS"),
                    f.col("NOM_CURS"),
                    f.col("IND_TIP_CURS"),
                    f.col("DAT_ENTR_CURS"),
                    f.col("DAT_CNCA_FTRM"),
                    f.col("NUM_PCLA"),
                    f.col("DAT_VCTO_PCLA"),
                    f.col("DAT_PGTO_PCLA"),
                    f.col("DAT_PGTO_PCLA").alias("DAT_CMPC_PCLA"),
                    f.col("MDO_MTDO_PGTO"),
                    f.col("STA_PGATO_PCLA"),
                    f.col("STA_STTS_CBRN"),
                    f.col("STA_PCLA"),
                    f.col("VLR_PLNO_PGTO"),
                    f.col("VLR_PCLA"),
                    f.col("DSC_DESC_PCLA"),
                    f.col("COD_CPOM_PCLA"),
                    f.col("DSC_CPOM"),
                    f.col("DAT_INI_VGNC"),
                    f.col("DAT_FIM_VGNC"),
                    f.col("TIP_CLCL_DESC"),
                    f.col("VLR_TIPO_DESC"),
                    f.col("JUR_JURO_PCLA"),
                    f.col("VLR_MLTA_PCLA"),
                    f.col("VLR_OTRO_ICMT"),
                    f.col("VLR_ULTM_ATLZ"),
                    f.col("VLR_PGTO"),
                    f.col("ORI_TSTE_GRTS"),
                    f.col("DAT_FIM_TSTE_GRTS"),
                    f.col("IND_TSTE_GRTS"),
                    f.col("COD_CRTO_RCNT"), 
                    f.col("MRC_MRCA_CRTO")))

    return df_contas_a_receber