job_rent_diario= {
    'DatasetName': 'RentDiario',
    "SourceSystem": "mock",
    "AliasSystem": "mk",
    'Stages':[
        {
            'ExternalFunction':{
                'Module': 'spark_pyutil_functions.datamask',
                'Parameters': [
	                {
						"MaskFields": [
						    {
								"MaskFieldName": "id_hash",
								"FieldName": "id",
								"DomainName": "user_id",
								"Active": True,
								"FormatRE": ".*",
								"KeepMaskName": False
							},
							{
								"MaskFieldName": "credit_card_hash",
								"FieldName": "credit_card",
								"DomainName": "credit_card",
								"Active": True,
								"FormatRE": ".*",
								"KeepMaskName": False
							}
						]
					}
				]
            },
            "Output": {
                'Coalesce':2
            }
        },
        {
            'ExternalFunction': {
                    'Module': 'spark_pyutil_functions.rename_cast',
                    'Parameters': [
                        {"ColumnName":"id", "NewColumnName":"id_rent_replace", "NewDataType": "String"},
					    {"ColumnName":"movie", "NewColumnName":"nm_movie", "NewDataType": "String"},
					    {"ColumnName":"movie_genres", "NewColumnName":"nm_movie_genres", "NewDataType": "String"}
                    ]    
                }
        }
    ]
}

job_user_diario = {
    'DatasetName': 'UserDiario',
    "SourceSystem": "mock",
    "AliasSystem": "mc",
    'Stages':
    [
    {
        'ExternalFunction':{
            'Module': 'spark_pyutil_functions.datamask',
            'Parameters': [
	                {
						"MaskFields": [
						    {
								"MaskFieldName": "id_hash",
								"FieldName": "id",
								"DomainName": "user_id",
								"Active": True,
								"FormatRE": ".*",
								"KeepMaskName": False
							}
						]
					}
				]
        }
    },
    {
        'ExternalFunction': {
                'Module': 'spark_pyutil_functions.rename_cast',
                'Parameters': [
                    {"ColumnName":"id", "NewColumnName":"id_rent_replace", "NewDataType": "String"}
                ]    
        }
    }
    ]
}
