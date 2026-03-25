from pyspark.sql import functions as f
import logging


def join_enrollment_course(std, ece):
    """JOIN enrollment_course ece
    ON ece.cod_aluno = std.cod_aluno"""
    return (std.join(ece, ece['COD_ALNO'] == std['COD_ALNO']).drop(ece['COD_ALNO']))


def join_course(std, cc):
    """JOIN course_course cc
    ON cc.cod_curso = ece.cod_curso"""
    return (std.join(cc, cc['COD_CURS'] == std['COD_CURS']).drop(cc['COD_CURS']))


def join_city(std, city):
    """JOIN locale_city city
    ON city.COD_CIDADE = std.COD_CIDADE"""
    return (std.join(city, city['COD_CDDE'] == std['COD_CDDE']).drop(city["COD_CDDE"]).drop(city['COD_ESTD']))


def join_state(std, state):
    """JOIN locale_state state
    ON city.COD_ESTADO = state.COD_ESTADO"""
    return (std.join(state, std['COD_ESTD'] == state['COD_ESTD']).drop(state['COD_ESTD']))


def join_enrollment_reason(std, er):
    """LEFT JOIN enrollment_reason er
    ON er.COD_MTVO_EVSO = ece.COD_RZAO_INSC_CURS / """
    return (std.join(er, er['COD_MTVO_EVSO'] == std['COD_RZAO_INSC_CURS'], "left")
            )


def join_payment_plan(std, ppi):
    """LEFT JOIN 
    (SELECT pp.cod_aluno,
         max(i.num_parcela) AS num_parcela
    FROM financial_payment_plan pp
    INNER JOIN financial_instalment i
        ON i.COD_PLANO_PAGAMENTO = pp.COD_PLANO_PAGAMENTO
    WHERE i.sta_pagamento_parcela NOT IN ('CANCELLED','PENDING')
    GROUP BY  pp.cod_aluno) ppi
    ON ppi.cod_aluno = std.cod_aluno"""
    std = (std.join(ppi, ppi['COD_ALNO'] == std['COD_ALNO'], "left")
              .drop(ppi['COD_ALNO']))
    return std


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    course = inputs['course']
    student = inputs['student']
    enrollment_course = inputs['enrollment_course']
    locale_city = inputs['locale_city']
    locale_state = inputs['locale_state']
    enrollment_reason = inputs['enrollment_reason']
    payment_plan = inputs['payment_plan']
    financial_instalment = inputs['financial_instalment']

    std = join_enrollment_course(student, enrollment_course)
    std = join_course(std, course)
    std = join_city(std, locale_city)
    std = join_state(std, locale_state)

    """Prepaire join between financial_instalment and payment_play, so it moves forward to join_payment_plan function"""
    ppi = (payment_plan.join(financial_instalment,
                             financial_instalment['COD_PLNO_PGTO'] == payment_plan['COD_PLNO_PGTO'])
           .filter(f.col("STA_PGATO_PCLA").isin('Cancelado', 'Pendente') == False)
           .select("COD_ALNO", "NUM_PCLA")
           .groupBy(f.col("COD_ALNO"))
           .agg(f.max(f.col("NUM_PCLA")).alias("NUM_PCLA")))

    std = join_payment_plan(std, ppi)
    std = join_enrollment_reason(std, enrollment_reason)

    """concat(std.nom_rua,', ',std.num_endereco,' - ',city.nom_cidade,' - ',state.nom_estado) AS "Endereco","""
    std = (std.withColumn("LCL_ENDR_ALNO",
                          f.concat_ws(' - ',
                                      f.col("DSC_ENDR_ALNO"),
                                      f.col("NOM_ESTD"),
                                      f.col("NOM_CDDE"))
                          )
           .withColumn("NUM_CURS_EM_MSES", f.substring(f.col("DSC_DURC_CURS"), 2, 2)))

    std = (std.select("COD_ALNO",
                      "NOM_ALNO",
                      "NUM_CPF",
                      "LCL_ENDR_ALNO",
                      "NUM_IDDE_ALNO",
                      "NUM_TLFN_ALNO",
                      "EML_ALNO",
                      "STA_STCO_INSC_CURS",
                      "DAT_ENTR_CURS",
                      "DAT_EVSO_CURS",
                      "COD_CURS",
                      "NOM_CURS",
                      "IND_TIP_CURS",
                      "NUM_CURS_EM_MSES",
                      "DAT_ESTM_FRMR",
                      "COD_RZAO_INSC_CURS",
                      "COD_MTVO_EVSO",
                      "DSC_MTVO_EVSO",
                      "DSC_SBMT_EVSO",
                      "DSC_ELAB_ALNO",
                      "NUM_PCLA")
           .filter(f.col("STA_STTS_INSC_CURS").isin('Trancado', 'Cancelado', 'Abandonado') == False))
    return std
