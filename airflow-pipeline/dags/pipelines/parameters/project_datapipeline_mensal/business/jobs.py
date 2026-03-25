job_rent_user_mensal = {
    'DatasetName':'RentUserMensal',
    'NumOfDpus': 10,
    'Stages':[
    {
        'Input': [
            {'DatasetName': 'RentMensal', 'InputAlias': 'df_input'},
            {'DatasetName': 'UserMensal', 'InputAlias': 'df_user'},
            ],
        'ExternalFunction':{
            "Module": "pyspark_library.dummy",
            }
    }
    ]
}

