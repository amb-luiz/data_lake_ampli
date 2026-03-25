from pyspark.sql import functions as f
import logging

"""
SELECT i.id as 'Nro Unico Ampli',
       'AMP1' as cd_unidade,
       'MENS' as operacao,
       format(i.amount,2,'pt_BR') as valor,
       1 as cd_operacao,
       1 as cod_item
  FROM financial.payment_plan pp
 INNER JOIN financial.instalment i on pp.id = i.payment_plan_id
 WHERE MONTH(i.due_date) in ({{ Mês Competencia }}) and year(i.due_date) = {{ Ano Competência }}
 UNION
SELECT i.id as 'Nro Unico Ampli',
       'AMP1' as cd_unidade,
       'CONVENIOS' as operacao,
       format(i.amount,2,'pt_BR') as valor,
       1 as cd_operacao,
       1 as cod_item
  FROM financial.payment_plan pp
 INNER JOIN financial.instalment i on pp.id = i.payment_plan_id
 WHERE MONTH(i.due_date) in ({{ Mês Competencia }}) and year(i.due_date) = {{ Ano Competência }}
   AND i.discount <> 0
"""


def join_financial_instalment(financial_instalment, payment_plan):
    df_vpex_nfse_itens = (
        payment_plan.join(financial_instalment,
                          payment_plan['COD_PLNO_PGTO'] == financial_instalment['COD_PLNO_PGTO'], how='inner')
    )
    return df_vpex_nfse_itens.drop(financial_instalment['COD_PLNO_PGTO'])


def execute(spark, inputs, parameters=None, dynamic_parameters=None, write=True):
    financial_instalment = inputs['financial_instalment']
    payment_plan = inputs['financial_payment_plan']

    df_vpex_nfse_itens = join_financial_instalment(
        financial_instalment, payment_plan)

    df_vpex_nfse_itens = (
        df_vpex_nfse_itens.select(
            f.col('COD_PCLA').alias('NRO_UNCO_AMPLI'),
            f.col('VLR_PCLA').alias('VLR_VLR')
        )
    )

    df_vpex_nfse_itens = (
        df_vpex_nfse_itens
        .withColumn("COD_UNDE", f.lit("AMP1"))
        .withColumn("COD_OPRC", f.lit("MENS"))
        .withColumn("COD_COD_OPRC", f.lit(1))
        .withColumn("COD_ITEM", f.lit(1))
    )

    df_vpex_nfse_itens2 = join_financial_instalment(
        financial_instalment, payment_plan)

    df_vpex_nfse_itens2 = (
        df_vpex_nfse_itens2.filter(
            financial_instalment['DSC_DESC_PCLA'] != 0
        )
    )

    df_vpex_nfse_itens2 = (
        df_vpex_nfse_itens2.select(
            f.col('COD_PCLA').alias('NRO_UNCO_AMPLI'),
            f.col('VLR_PCLA').alias('VLR_VLR')
        )
    )

    df_vpex_nfse_itens2 = (
        df_vpex_nfse_itens2
        .withColumn("COD_UNDE", f.lit("AMP1"))
        .withColumn("COD_OPRC", f.lit("CONVENIOS"))
        .withColumn("COD_COD_OPRC", f.lit(1))
        .withColumn("COD_ITEM", f.lit(1))
    )

    df_vpex_nfse_itens = (
        df_vpex_nfse_itens.union(df_vpex_nfse_itens2)
    )
    return df_vpex_nfse_itens
