job_rent_user_diario = {
    'DatasetName':'RentUserDiario',
    'Stages':[
    {
        'Input': [
            {'DatasetName': 'RentDiario', 'InputAlias': 'df_input'},
            {'DatasetName': 'RentUserMensal', 'InputAlias': 'df_input'},
            {'DatasetName': 'UserDiario', 'InputAlias': 'df_user'},
            ],
        'ExternalFunction':{
            "Module": "pyspark_library.dummy",
            }
    }
    ]
}

