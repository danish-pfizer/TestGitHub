class NERModelTraining:
    cmd1 = 'python -m spacy init config config.cfg --lang en --pipeline ner --optimize efficiency --force'
    cmd2_part1 = 'python -m spacy train config.cfg --output ./output_'
    cmd2_part2 = ' --paths.train ./train.spacy --paths.dev ./train.spacy'
    collection = 'Test'
    db_name = 'Test1'
    file_names = ['Principles', 'Map_File', 'SDTM_Metadata']
    mongodb_cols = ['Table_Name', 'DATA', 'Domain_Name']
    mongodb_connection = 'mongodb://127.0.0.1:27017/'
    optional_list_end = ['_C', '_ND', '_DTS', '_DTR', 'FROZENSTATE', 'LOCKEDSTATE', 'NOTDONESTATE', 'DATEDATACHANGED', 'SKEY', 'VISITREFNAME', 'VISITINDEX']
    principles_keys = ['val_type', 'nullable', 'length range', 'uniqueness', 'Target Cols']
    sdtm_columns = ['Domain Prefix', 'Variable Name', 'Variable Name (minus domain prefix)']
    temp_collection = 'Temp'
    train_table = 'Training_Dataset'
    value_date = 'date'
    value_duplicated = 'duplicated'
    value_float = 'float'
    value_int = 'int'
    value_no = 'N'
    value_str = 'str'
    value_unique = 'unique'
    value_yes = 'Y'

class NERModelLiveData:
    collection_live = 'Test_Live'
    collection_output = 'Target_Dataset'
    prereq_domain_list = ['DM', 'DS', 'DD', 'EC']
    # collection_output = 'SDTM_NER_Prediction'
    multicol = 'RACE'
    multicol_val = 'MULTIPLE'
    spacy_model_name = '/model-best/'
    spacy_model_path = './output_'
    address = './PODS/PODS_ODS_ADDRESS_V.csv'
    country = './PODS/PODS_ODS_COUNTRY_V.csv'
    contact_info = './PODS/PODS_ODS_CONTACT_INFO_V.csv'
    person = 'PODS/PODS_ODS_PERSON_V.csv'
    study_alias = 'PODS/PODS_ODS_STUDY_ALIAS_V.csv'
    curr_dom_list = ['Death Details (DD)', 'Demography (DM)', 'Disposition (DS)', 'Exposure as Collected (EC)']
    dom_list = ['Adverse Events (AE)', 'Death Details (DD)', 'Demography (DM)', 'Disposition (DS)', 'Exposure as Collected (EC)']
    all_domains_list = ['AB', 'AE', 'BC', 'CD', 'CE', 'CM', 'DA', 'DD', 'DM', 'DS', 'DV', 'EC', 'EF',
                        'EG', 'FA', 'GH', 'HO', 'IE', 'IJ', 'IS', 'KL', 'LB', 'MB', 'MH', 'MI', 'MN',
                        'MO', 'MS', 'OP', 'PC', 'PE', 'PP', 'PR', 'QR', 'QS', 'RP', 'RS', 'SC', 'SE',
                        'SR', 'SS', 'ST', 'SU', 'SV', 'TA', 'TD', 'TE', 'TI', 'TR', 'TS', 'TU', 'TV',
                        'TX', 'UV', 'VS', 'WX', 'YZ']

class MetadataAssumptionsDomains:
    assumptions_training_file = 'Assumptions_261_No_Domain.txt'
    assumption_cols = ['Assumptions', 'Modified Assumptions']
    assumption_domain_train = 'ASSUMPTIONS_DOMAIN_Trng_Data.txt'
    assumption_list = ['Var Definition', 'Var Reference', 'Var Other', 'Domain Definition', 'Domain Reference',
                       'Domain Other']
    assumption_vars = ['VAR_DEF', 'VAR_REF', 'DOMAIN_DEF', 'DOMAIN_REF']
    cat_col_values = ['Topic', 'QcatA', 'QcatB', 'Identifier', 'Timing']
    cdisc_pkl = 'cdisc_model_pkl'
    cmd1 = 'python -m spacy init config config.cfg --lang en --pipeline ner --optimize accuracy --force'
    cmd1_domain = 'python -m spacy init config config_domain.cfg --lang en --pipeline ner --optimize accuracy --force'
    cmd2 = 'python -m spacy train config.cfg --output ./output_Assumptions --paths.train ./train.spacy --paths.dev ./validation.spacy'
    cmd2_domain = 'python -m spacy train config_domain.cfg --output ./output_Assumptions_Domain --paths.train ./train_domain.spacy --paths.dev ./validation_domain.spacy'
    colList = ['Domain Prefix', 'Variable Name', 'Variable Label', 'Type', 'Role',
               'CDISC Notes (for domains) Description (for General Classes)', 'Core',
               'Operator_Modified', 'Controlled Terms or Format', 'CDISC Submission Value',
               'Names', 'Variable Name (minus domain prefix)', 'Section_4_data']
    colList1 = ['Domain Prefix', 'Variable Name', 'Variable Label', 'Type', 'Role',
                'CDISC Notes (for domains) Description (for General Classes)', 'Core',
                'Operator_Modified']
    core_val_list = ['Req', 'N', 'Y']
    ct_col_name = 'Controlled Terms, Codelist or Format'
    domain_dict = {
        'CO': ['Comments'],
        'DM': ['Demographics model', 'Demographics domain', 'Demographics'],
        'SE': ['Subject Elements', 'Subject Element'],
        'SV': ['Subject Visits'],
        'CM': ['Concomitant and Prior Medications/Therapies', 'Concomitant and Prior Medications',
               'Concomitant Medications/Therapies', 'Concomitant Medications/Therapy',
               'Concomitant Medications', 'Concomitant Medication'],
        'EC': ['Exposure as Collected'],
        'EX': ['Exposure domain', 'Exposure'],
        'SU': ['Substance Use'],
        'PR': ['Procedures'],
        'AE': ['Adverse Events', 'Adverse Event', 'AEs'],
        'CE': ['Clinical Events', 'Clinical Event'],
        'DS': ['Disposition'],
        'DV': ['Protocol Deviations', 'Deviations domain'],
        'MH': ['Medical History'],
        'HO': ['Healthcare Encounters'],
        'DA': ['Drug Accountability'],
        'DD': ['Death Details'],
        'EG': ['ECG Test Results'],
        'IE': ['Inclusion/Exclusion Criterion Not Met'],
        'IS': ['Immunogenicity Specimen Assessments'],
        'LB': ['Laboratory Test Results'],
        'MB': ['Microbiology Specimen'],
        'MI': ['Microscopic Findings'],
        'MO': ['Morphology'],
        'MS': ['Microbiology Susceptibility Test'],
        'PC': ['PK Concentrations'],
        'PP': ['PK Parameters'],
        'PE': ['Physical Examination'],
        'QS': ['Questionnaire Supplements', 'Questionnaire Supplement', 'Questionnaires', 'Questionnaire'],
        'RP': ['Reproductive System Findings'],
        'RS': ['Disease Response'],
        'SC': ['Subject Characteristics'],
        'SS': ['Subject Status'],
        'TU': ['Tumor Identification'],
        'TR': ['Tumor Results'],
        'VS': ['Vital Signs', 'Vital Sign'],
        'FA': ['Findings About Events or Interventions', 'Findings About Events and Interventions', 'Findings About'],
        'SR': ['Skin Response'],
        'TA': ['Trial Arms'],
        'TD': ['Trial Disease Assessment'],
        'TE': ['Trial Elements', 'Trial Element'],
        'TV': ['Trial Visits'],
        'TI': ['Trial Inclusion/Exclusion Criteria'],
        'TS': ['Trial Summary'],
        'MA': ['Macroscopic Findings'],
        'OM': ['Organ Measurements']
    }
    domain_dict_hv = {
        'EX': ["exposed", "exposure"],
        'DS': ["disposition"],
    }
    keywords = ["screen failure", "informed consent", "lost to follow up"]
    merged_col_name = 'Merged'
    meta_output_file = 'SDTMIG_Metadata.json'
    meta_output_file1 = 'metadata_29062022.json'
    model_pkl = 'model_pkl'
    mongodb_cols = ['Table_Name', 'DATA', 'Domain_Name']
    operations_col_name = 'Operations'
    operations_keys = ['variables', 'operators', 'CT', 'Nullable', 'Assumption_Variables', 'Hidden_Variables']
    QcatA = ['Synonym Qualifier', 'Variable Qualifier', 'Grouping Qualifier']
    QcatB = ['Record Qualifier', 'Result Qualifier']
    sdtm_cols = ['Structure', 'Cat_Role', 'Value Type', 'VAR']
    spacy_domain_model_path = './output_Assumptions_Domain/model-best/'
    spacy_model_path = './output_Assumptions/model-best/'
    structure_col_values = ['DM', 'One-One', 'One-Many']
    suffix_dict_hv = {
        'STDTC': "Start Date/time",
        'ENDTC': "End Date/time",
        'DTC': "Date/time",
        'TPT': "planned time point",
        'PRESP': "pre-specified",
        'DTHDTC': "death date"
    }
    terminology_col_name = 'CDISC Submission Value'
    value_type_col_values = ['Unique', 'Independent', 'Can be Repeated']
    variable_keys = ['datatype', 'Value type', 'operations', 'Assumptions']
    var_hv = ['STAT', 'DY', 'OCCUR', 'ENTPT', 'TPTNUM', 'ELTM', 'RF', 'RFX', 'PRESP']
