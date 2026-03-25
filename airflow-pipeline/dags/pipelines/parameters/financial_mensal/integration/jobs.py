coupon = {
    'DatasetName': 'COUPON_COUPON',
    'Active': True,
    # 'SourceSystem': "coupon",
    'NumOfDpus': 8,
    'Stages':[
        {
            'ExternalFunction':{
                'Module': 'spark_pyutil_functions.datamask',
                'Parameters': [
                    {
                        "MaskFields": []
					}
				]
            },
            "Output": {
                'Coalesce':18
            }
        },
        {
            'ExternalFunction': {
                'Module': 'pyspark_library.ampli.transformations.raw_integration.coupon_coupon',
                'Parameters': []
            },
            'Input': [
                {
                    'InputAlias': "df_coupon_coupon",
                    'Delete': False,
                    'ReadFormat': 'parquet',
                    'RepartitionValue': 100,
                    'DatasetName': "COUPON_COUPON"
                }
            ],
            'Output': {
                'MergeType': 'Replace/Partition',
                'Coalesce': 1,
                'PartitionKeyList': []
            }
        }
    ]
}

course = {
    "DatasetName": "COURSE_COURSE",
    "Active": True,
    #"SourceSystem": "course",
    "NumOfDpus": 8,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "spark_pyutil_functions.datamask",
                "Parameters": [
                    {
                        "MaskFields": []
                    }
                ],
            },
            "Output": {"Coalesce": 18},
        },
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.raw_integration.course_course",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "df_course_course",
                    "Delete": False, 
                    "ReadFormat": "parquet",
                    "RepartitionValue": 100,
                    "DatasetName": "COURSE_COURSE"}
            ],
            "Output": {
                "MergeType": "Replace/Partition",
                "Coalesce": 1,
                "PartitionKeyList": [],
            },
        },
    ],
}

enrollment_course = {
    "DatasetName": "ENROLLMENT_COURSE",
    "Active": True,
    #"SourceSystem": "enrollment_course",
    "NumOfDpus": 8,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "spark_pyutil_functions.datamask",
                "Parameters": [
                    {
                        "MaskFields": []
                    }
                ],
            },
            "Output": {"Coalesce": 18},
        },
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.raw_integration.enrollment_course",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "df_enrollment_course",
                    "Delete": False, 
                    "ReadFormat": "parquet",
                    "RepartitionValue": 100,
                    "DatasetName": "ENROLLMENT_COURSE"}
            ],
            "Output": {
                "MergeType": "Replace/Partition",
                "Coalesce": 1,
                "PartitionKeyList": [],
            },
        },
    ],
}

enrollment_reason = {
    "DatasetName": "ENROLLMENT_REASON",
    "Active": True,
    #"SourceSystem": "enrollment_reason",
    "NumOfDpus": 8,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "spark_pyutil_functions.datamask",
                "Parameters": [
                    {
                        "MaskFields": []
                    }
                ],
            },
            "Output": {"Coalesce": 18},
        },
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.raw_integration.enrollment_reason",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "df_enrollment_reason",
                    "Delete": False, 
                    "ReadFormat": "parquet",
                    "RepartitionValue": 100,
                    "DatasetName": "ENROLLMENT_REASON"}
            ],
            "Output": {
                "MergeType": "Replace/Partition",
                "Coalesce": 1,
                "PartitionKeyList": [],
            },
        },
    ],
}

#TO DO 
# MASK FIELDS
financial_instalment = {
    "DatasetName": "FINANCIAL_INSTALMENT",
    "Active": True,
    #"SourceSystem": "financial_instalment",
    "NumOfDpus": 8,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "spark_pyutil_functions.datamask",
                "Parameters": [
                    {
                        "MaskFields": []
                    }
                ],
            },
            "Output": {"Coalesce": 18},
        },
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.raw_integration.financial_instalment",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "df_financial_instalment",
                    "Delete": False, 
                    "ReadFormat": "parquet",
                    "RepartitionValue": 100,
                    "DatasetName": "FINANCIAL_INSTALMENT"}
            ],
            "Output": {
                "MergeType": "Replace/Partition",
                "Coalesce": 1,
                "PartitionKeyList": [],
            },
        },
    ],
}

financial_instalment_charge = {
    "DatasetName": "FINANCIAL_INSTALMENT_CHARGE",
    "Active": True,
    #"SourceSystem": "financial_instalment_charge",
    "NumOfDpus": 8,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "spark_pyutil_functions.datamask",
                "Parameters": [
                    {
                        "MaskFields": []
                    }
                ],
            },
            "Output": {"Coalesce": 18},
        },
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.raw_integration.financial_instalment_charge",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "df_financial_instalment_charge",
                    "Delete": False, 
                    "ReadFormat": "parquet",
                    "RepartitionValue": 100,
                    "DatasetName": "FINANCIAL_INSTALMENT_CHARGE"}
            ],
            "Output": {
                "MergeType": "Replace/Partition",
                "Coalesce": 1,
                "PartitionKeyList": [],
            },
        },
    ],
}

financial_payment_plan = {
    "DatasetName": "FINANCIAL_PAYMENT_PLAN",
    "Active": True,
    #"SourceSystem": "financial_payment_plan",
    "NumOfDpus": 8,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "spark_pyutil_functions.datamask",
                "Parameters": [
                    {
                        "MaskFields": []
                    }
                ],
            },
            "Output": {"Coalesce": 18},
        },
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.raw_integration.payment_plan",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "df_payment_plan",
                    "Delete": False, 
                    "ReadFormat": "parquet",
                    "RepartitionValue": 100,
                    "DatasetName": "FINANCIAL_PAYMENT_PLAN"}
            ],
            "Output": {
                "MergeType": "Replace/Partition",
                "Coalesce": 1,
                "PartitionKeyList": [],
            },
        },
    ],
}

locale_city = {
    'DatasetName': 'LOCALE_CITY',
    'Active': True,
    # 'SourceSystem': "locale",
    'NumOfDpus': 8,
    'Stages':[
        {
            'ExternalFunction':{
                'Module': 'spark_pyutil_functions.datamask',
                'Parameters': [
                    {
                        "MaskFields": []
					}
				]
            },
            "Output": {
                'Coalesce':2
            }
        },
        {
            'ExternalFunction': {
                    'Module': 'pyspark_library.ampli.transformations.raw_integration.locale_city',
                    'Parameters': []
            },
            'Input': [
                {
                    'InputAlias': "df_locale_city",
                    'Delete': False,
                    'ReadFormat': 'parquet',
                    'RepartitionValue': 100,
                    'DatasetName': "LOCALE_CITY"
                }
            ],
            'Output': {
                'MergeType': 'Replace/Partition',
                'Coalesce': 1,
                'PartitionKeyList': []
            }
        }
    ]
}

locale_state = {
    'DatasetName': 'LOCALE_STATE',
    'Active': True,
    # 'SourceSystem': "locale",
    'NumOfDpus': 8,
    'Stages':[
        {
            'ExternalFunction':{
                'Module': 'spark_pyutil_functions.datamask',
                'Parameters': [
                    {
                        "MaskFields": []
					}
				]
            },
            "Output": {
                'Coalesce':2
            }
        },
        {
            'ExternalFunction': {
                    'Module': 'pyspark_library.ampli.transformations.raw_integration.locale_state',
                    'Parameters': []    
            },
            'Input':[
                {
                    'InputAlias': "df_locale_state",
                    'Delete': False,
                    'ReadFormat': 'parquet',
                    'RepartitionValue': 100,
                    'DatasetName': "LOCALE_STATE"
                }
            ],
            'Output': {
                'MergeType': 'Replace/Partition',
                'Coalesce': 1,
                'PartitionKeyList': []
            }
        }
    ]
}

payment_charge = {
    "DatasetName": "PAYMENT_CHARGE",
    "Active": True,
    #"SourceSystem": "payment_charge",
    "NumOfDpus": 8,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "spark_pyutil_functions.datamask",
                "Parameters": [
                    {
                        "MaskFields": []
                    }
                ],
            },
            "Output": {"Coalesce": 18},
        },
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.raw_integration.payment_charge",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "df_payment_charge",
                    "Delete": False, 
                    "ReadFormat": "parquet",
                    "RepartitionValue": 100,
                    "DatasetName": "PAYMENT_CHARGE"}
            ],
            "Output": {
                "MergeType": "Replace/Partition",
                "Coalesce": 1,
                "PartitionKeyList": [],
            },
        },
    ],
}


#TO DO 
# MASK FIELDS
payment_credit_card = {
    "DatasetName": "PAYMENT_CREDIT_CARD",
    "Active": True,
    #"SourceSystem": "payment_credit_card",
    "NumOfDpus": 8,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "spark_pyutil_functions.datamask",
                "Parameters": [
                    {
                        "MaskFields": []
                    }
                ],
            },
            "Output": {"Coalesce": 18},
        },
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.raw_integration.payment_credit_card",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "df_payment_credit_card",
                    "Delete": False, 
                    "ReadFormat": "parquet",
                    "RepartitionValue": 100,
                    "DatasetName": "PAYMENT_CREDIT_CARD"}
            ],
            "Output": {
                "MergeType": "Replace/Partition",
                "Coalesce": 1,
                "PartitionKeyList": [],
            },
        },
    ],
}

#TO DO 
# MASK FIELDS
student = {
    "DatasetName": "STUDENT_STUDENT",
    "Active": True,
    #"SourceSystem": "STUDENT_STUDENT",
    "NumOfDpus": 8,
    "Stages": [
        {
            "ExternalFunction": {
                "Module": "spark_pyutil_functions.datamask",
                "Parameters": [
                    {
                        "MaskFields": []
                    }
                ],
            },
            "Output": {"Coalesce": 18},
        },
        {
            "ExternalFunction": {
                "Module": "pyspark_library.ampli.transformations.raw_integration.student",
                "Parameters": [],
            },
            "Input": [
                {
                    "InputAlias": "df_student",
                    "Delete": False, 
                    "ReadFormat": "parquet",
                    "RepartitionValue": 100,
                    "DatasetName": "STUDENT_STUDENT"}
            ],
            "Output": {
                "MergeType": "Replace/Partition",
                "Coalesce": 1,
                "PartitionKeyList": [],
            },
        },
    ],
}