class NERModelTraining:
    principle_file_location = 'C:/Users/DR06/OneDrive - Pfizer/Desktop/auto/out'
    map_file_location = 'C:/Users/DR06/OneDrive - Pfizer/Desktop/auto/map_file.json'
    mongodb_connection = 'mongodb://127.0.0.1:27017/'
    db_name = 'IDAT_DB'
    collection = 'SDTM_Auto'
    collection_output = 'SDTM_Training_Output'
    mongodb_cols = ['Table_Name', 'DATA', 'Domain_Name']
    cmd1 = 'python -m spacy init config config.cfg --lang en --pipeline ner --optimize efficiency --force'
    # cmd2 = 'python -m spacy train config.cfg --output ./output_new --paths.train ./train.spacy --paths.dev ./train.spacy'
    cmd2_part1 = 'python -m spacy train config.cfg --output ./output_'
    cmd2_part2 = ' --paths.train ./train.spacy --paths.dev ./train.spacy'
    value_float = 'float'
    value_int = 'int'
    value_date = 'date'
    value_str = 'str'
    principles_keys = ['val_type', 'nullable', 'length range', 'uniqueness']
    value_yes = 'Y'
    value_no = 'N'
    value_unique = 'unique'
    value_duplicated = 'duplicated'
    file_names = ['Principles', 'Map_File']
    

class NERModelLiveData:
    mongodb_connection = 'mongodb://127.0.0.1:27017/'
    db_name = 'IDAT_DB'
    collection = 'SDTM_LIVE_DATA'
    #collection_principle = 'SDTM_Training_Output'
    collection_output = 'SDTM_NER_Prediction'
    multicol = 'RACE'
    multicol_val = 'MULTIPLE'
    mongodb_cols = ['Table_Name', 'DATA', 'Domain_Name']
    file_names = ['Principles', 'Map_File']
    principles_keys = ['val_type', 'nullable', 'length range', 'uniqueness']
    value_unique = 'unique'
    value_no = 'N'
    value_float = 'float'
    value_int = 'int'
    value_date = 'date'
    value_str = 'str'
    spacy_model_path = './output_DM/model-best/'
