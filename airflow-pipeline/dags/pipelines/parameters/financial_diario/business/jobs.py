alunado = {
    "DatasetName": "alunado",
    "Active": True,
    "NumOfDpus": 4,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.business.alunado",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "enrollment_course",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "ENROLLMENT_COURSE",
                },
                {
                    "InputAlias": "course",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "COURSE_COURSE",
                },
                {
                    "InputAlias": "locale_city",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "LOCALE_CITY",
                },
                {
                    "InputAlias": "locale_state",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "LOCALE_STATE",
                },
                {
                    "InputAlias": "financial_payment_plan",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_PAYMENT_PLAN",
                },
                {
                    "InputAlias": "student",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "STUDENT_STUDENT",
                }


            ],
            "Output": {"MergeType": "Replace", "Coalesce": 1},
        }
    ],
}

vpex_nfse_itens = {
    "DatasetName": "vpex_nfse_itens",
    "Active": True,
    "NumOfDpus": 4,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.business.vpex_nfse_itens",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "financial_payment_plan",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_PAYMENT_PLAN",
                },
                {
                    "InputAlias": "financial_instalment",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_INSTALMENT",
                }


            ],
            "Output": {"MergeType": "Replace", "Coalesce": 1},
        }
    ],
}

vpex_abandono = {
    "DatasetName": "vpex_abandono",
    "Active": True,
    "NumOfDpus": 4,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.business.vpex_abandono",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "enrollment_course",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "ENROLLMENT_COURSE",
                },
                {
                    "InputAlias": "enrollment_reason",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "ENROLLMENT_REASON",
                },
                {
                    "InputAlias": "course",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "COURSE_COURSE",
                },
                {
                    "InputAlias": "locale_city",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "LOCALE_CITY",
                },
                {
                    "InputAlias": "locale_state",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "LOCALE_STATE",
                },
                {
                    "InputAlias": "payment_plan",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_PAYMENT_PLAN",
                },
                {
                    "InputAlias": "student",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "STUDENT_STUDENT",
                },
                {
                    "InputAlias": "financial_instalment",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_INSTALMENT",
                }


            ],
            "Output": {"MergeType": "Replace", "Coalesce": 1},
        }
    ],
}

vpex_contas_a_receber = {
    "DatasetName": "vpex_contas_a_receber",
    "Active": True,
    "NumOfDpus": 4,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.business.vpex_contas_a_receber",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "payment_plan",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_PAYMENT_PLAN",
                },
                {
                    "InputAlias": "instalment",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_INSTALMENT",
                },
                {
                    "InputAlias": "course",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "COURSE_COURSE",
                },
                {
                    "InputAlias": "instalment_charge",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_INSTALMENT_CHARGE",
                },
                {
                    "InputAlias": "payment_charge",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "PAYMENT_CHARGE",
                },
                {
                    "InputAlias": "payment_credit_card",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "PAYMENT_CREDIT_CARD",
                },
                {
                    "InputAlias": "coupon",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "COUPON_COUPON",
                },
                {
                    "InputAlias": "enrollment_course",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "ENROLLMENT_COURSE",
                }


            ],
            "Output": {"MergeType": "Replace", "Coalesce": 1},
        }
    ],
}

vpex_faturamento = {
    "DatasetName": "vpex_faturamento",
    "Active": True,
    "NumOfDpus": 4,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.business.vpex_faturamento",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "payment_plan",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_PAYMENT_PLAN",
                },
                {
                    "InputAlias": "instalment",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_INSTALMENT",
                },
                {
                    "InputAlias": "course_course",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "COURSE_COURSE",
                },
                {
                    "InputAlias": "instalment_charge",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_INSTALMENT_CHARGE",
                },
                {
                    "InputAlias": "coupon_coupon",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "COUPON_COUPON",
                },
                {
                    "InputAlias": "enrollment_course",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "ENROLLMENT_COURSE",
                }


            ],
            "Output": {"MergeType": "Replace", "Coalesce": 1},
        }
    ],
}

vpex_nfse_capa = {
    "DatasetName": "vpex_nfse_capa",
    "Active": True,
    "NumOfDpus": 4,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.business.vpex_nfse_capa",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "enrollment_course",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "ENROLLMENT_COURSE",
                },
                {
                    "InputAlias": "financial_payment_plan",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_PAYMENT_PLAN",
                },
                {
                    "InputAlias": "financial_instalment",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_INSTALMENT",
                },
                {
                    "InputAlias": "student",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "STUDENT_STUDENT",
                },
                {
                    "InputAlias": "locale_city",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "LOCALE_CITY",
                },
                {
                    "InputAlias": "locale_state",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "LOCALE_STATE",
                },
                {
                    "InputAlias": "coupon",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "COUPON_COUPON",
                }                


            ],
            "Output": {"MergeType": "Replace", "Coalesce": 1},
        }
    ],
}

vpex_recebimento = {
    "DatasetName": "vpex_recebimento",
    "Active": True,
    "NumOfDpus": 4,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.business.vpex_recebimento",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "enrollment_course",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "ENROLLMENT_COURSE",
                },
                {
                    "InputAlias": "financial_payment_plan",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_PAYMENT_PLAN",
                },
                {
                    "InputAlias": "financial_instalment",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_INSTALMENT",
                },
                {
                    "InputAlias": "course",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "COURSE_COURSE",
                },
                {
                    "InputAlias": "financial_instalment_charge",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "FINANCIAL_INSTALMENT_CHARGE",
                },
                {
                    "InputAlias": "payment_charge",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "PAYMENT_CHARGE",
                },
                {
                    "InputAlias": "payment_credit_card",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "PAYMENT_CREDIT_CARD",
                },
                {
                    "InputAlias": "coupon",
                    "Delete": False,
                    "ReadFormat": "parquet",
                    "RepartitionValue": 40,
                    "DatasetName": "COUPON_COUPON"
                },

            ],
            "Output": {"MergeType": "Replace", "Coalesce": 1},
        }
    ],
}