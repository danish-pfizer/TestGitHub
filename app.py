import flask
from flask import render_template, request, send_file
from werkzeug.utils import secure_filename
import pyreadstat
import pymongo
import Attribute_Values as Av
import pandas as pd
import json
import gridfs
import ast
import os
from collections import Counter
from dateutil.parser import parse
from fuzzywuzzy import fuzz
import numpy as np
import cx_Oracle
import OC_db_config as oc_config
from datetime import datetime
import traceback

c1 = Av.NERModelTraining()
c2 = Av.NERModelLiveData()
c3 = Av.MetadataAssumptionsDomains()

app = flask.Flask(__name__)

client = pymongo.MongoClient(c1.mongodb_connection)
mydb = client[c1.db_name]
temp_collection = mydb[c1.temp_collection]
mycollection_output = mydb[c2.collection_output]

def create_Folder(folderName):
    path = './Temp'
    isExist = os.path.exists(path)
    if not isExist:
        os.makedirs(path)
    path = './Results'
    isExist = os.path.exists(path)
    if not isExist:
        os.makedirs(path)
    path = './Results/' + folderName
    isExist = os.path.exists(path)
    if not isExist:
        os.makedirs(path)

@app.route('/')
def index():
    create_Folder('Training_Files')
    create_Folder('LiveData_Files')
    create_Folder('Target_Files')
    return render_template('Home.html')

@app.route('/Dashboard')
def Dashboard():
    return render_template('Dashboard.html')

@app.route('/Training')
def Training():
    return render_template('Home.html')

@app.route('/LiveData')
def LiveData():
    return render_template('LiveData.html')

@app.route('/SdtmigDecoder')
def SdtmigDecoder():
    return render_template('SdtmigDecoder.html')

@app.route('/', methods = ['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        filename = []
        for f in request.files.getlist('files'):
            filename.append(secure_filename(f.filename))
            path = 'Temp/' + f.filename
            f.save(path)
        df1 = pd.DataFrame()
        for i in filename:
            i1 = 'Temp/' + i
            df2 = read_data(i1)
            frames = [df2, df1]
            df1 = pd.concat(frames)
        df1 = df1.fillna('')
        df1.to_excel("Results/Training_Files/Training_Dataset.xlsx", index=False)
        return render_template('Home.html', message='Local SAS File(s) Successfully Uploaded for Training', curr_dom_list=c2.curr_dom_list, dom_list=c2.dom_list)

@app.route('/oracleConnect', methods = ['GET', 'POST'])
def connect_oracle():
    if request.method == 'POST':
        global ocuser, ocpassword
        ocuser = request.form['ocuser']
        ocpassword = request.form['ocpassword']
        ocdns = request.form['ocdns']
        try:
            con = cx_Oracle.connect(ocuser, ocpassword, 'prddw', encoding="UTF-8")
            return render_template('Home.html', message='UI has been connected to Oracle',
                                   curr_dom_list=c2.curr_dom_list, dom_list=c2.dom_list)
        except Exception as e:
            # print('There is an error in connecting to OC',e)
            return render_template('Home.html', message='Please Enter Valid User Credentials')


@app.route('/oracleConnect_Live', methods = ['GET', 'POST'])
def connect_oracle_Live():
    if request.method == 'POST':
        global ocuser, ocpassword
        ocuser = request.form['ocuser']
        ocpassword = request.form['ocpassword']
        ocdns = request.form['ocdns']
        try:
            con = cx_Oracle.connect(ocuser, ocpassword, 'prddw', encoding="UTF-8")
            return render_template('LiveData.html', message='UI has been connected to Oracle')
        except Exception as e:
            # print('There is an error in connecting to OC',e)
            return render_template('LiveData.html', message='Please Enter Valid User Credentials')


@app.route('/oc_ba_search', methods = ['GET', 'POST'])
def oc_ba_search():
    if request.method == 'POST':
        global study
        study = request.form['study']
        domain_oc = ''
        try:
            a = some(DB_type,DB_DNS,ocuser,ocpassword,domain_oc)
            ba_list = a.get_ba()
            ba_list = list(set(ba_list))
            return render_template('Home.html', ba_message='Select BA from below dropdown list',
                                   curr_dom_list=c2.curr_dom_list, ba_list=ba_list, dom_list=c2.dom_list)
        except Exception as e:
            print('There is an error in connecting to OC***',e)


@app.route('/oc_ba_search_live', methods = ['GET', 'POST'])
def oc_ba_search_live():
    if request.method == 'POST':
        global study
        study = request.form['study']
        study = study + '_SRDM'
        domain_oc = ''
        try:
            a = some(DB_type,DB_DNS,ocuser,ocpassword,domain_oc)
            ba_list = a.get_ba()
            ba_list = list(set(ba_list))
            return render_template('LiveData.html', ba_message='Select BA from below dropdown list',
                                   curr_dom_list=c2.curr_dom_list, ba_list=ba_list, dom_list=c2.dom_list)
        except Exception as e:
            print('There is an error in connecting to OC***',e)

@app.route('/train_oracle', methods = ['GET', 'POST'])
def train_oracle():
    if request.method == 'POST':
        global schemaLike, tableLike, numData, domain_oc
        schemaLike = request.form['schemaLike']
        tableLike = request.form['tableLike']
        numData = request.form['numDataset']
        domain_oc = request.form['domain']
        domain1 = domain_oc.split('(')[-1][0:2]
        domain_oc = domain1
        domain_oc = domain_oc.upper()
        global domain_name
        domain_name = domain_oc
        try:
            df1 = pd.DataFrame()
            df1.to_excel("Results/Training_Files/Training_Dataset.xlsx", index=False)
            a = some(DB_type,DB_DNS,ocuser,ocpassword,domain_oc)
            a.test_func()

        except Exception as e:
            print('There is an error in connecting to OC***',e)

        filename = 'SDTMIG-labelled.xlsx'
        df = pd.read_excel('Results/Training_Files/Training_Dataset.xlsx')
        principles_dict = {}
        for i in df.columns:
            principles_dict[i] = {}
        principles_dict = get_datatype(df, principles_dict)
        principles_dict = get_nullable(df, principles_dict)
        principles_dict = get_len_range(df, principles_dict)
        rds_principles = get_uniqueness(df, principles_dict)
        principles_final, domains, map_list_final = generate_final_principles(domain_oc)
        final_map = filterprinciples(principles_final, rds_principles)
        df_sdtm = pd.read_excel(filename)
        df1 = df_sdtm[df_sdtm[c1.sdtm_columns[0]] == domain_oc]
        var_tgt = list(df1[c1.sdtm_columns[1]])
        df2 = df_sdtm[df_sdtm[c1.sdtm_columns[0]].isna()]
        suffix_cols = list(df2[c1.sdtm_columns[2]])
        final_map1 = updatedomain(final_map, domains, suffix_cols, var_tgt, domain_oc)
        final_map = getfuzzratio(final_map1)
        final_map = remove_optional_mapping(final_map, map_list_final)
        for i in final_map.keys():
            final_map[i] = ','.join(final_map[i])
        dict1 = {}
        dict1[c1.mongodb_cols[0]] = 'Auto_Map'
        dict1[c1.mongodb_cols[1]] = final_map
        temp_collection.delete_many({})
        temp_collection.insert_one(dict1)
        return render_template('Home.html', automap='AutoMap File Generated. Click below button to Download.')

@app.route('/train_saslocal', methods = ['GET', 'POST'])
def train_saslocal():
    if request.method == 'POST':
        domain = request.form['domain']
        domain1 = domain.split('(')[-1][0:2]
        domain = domain1
        print(domain)
        global domain_name
        domain_name = domain.upper()
        domain = domain.upper()
        filename = 'SDTMIG-labelled.xlsx'
        df = pd.read_excel('Results/Training_Files/Training_Dataset.xlsx')
        principles_dict = {}
        for i in df.columns:
            principles_dict[i] = {}
        principles_dict = get_datatype(df, principles_dict)
        principles_dict = get_nullable(df, principles_dict)
        principles_dict = get_len_range(df, principles_dict)
        rds_principles = get_uniqueness(df, principles_dict)
        principles_final, domains, map_list_final = generate_final_principles(domain)
        final_map = filterprinciples(principles_final, rds_principles)
        df_sdtm = pd.read_excel(filename)
        df1 = df_sdtm[df_sdtm[c1.sdtm_columns[0]] == domain]
        var_tgt = list(df1[c1.sdtm_columns[1]])
        df2 = df_sdtm[df_sdtm[c1.sdtm_columns[0]].isna()]
        suffix_cols = list(df2[c1.sdtm_columns[2]])
        final_map1 = updatedomain(final_map, domains, suffix_cols, var_tgt, domain)
        final_map = getfuzzratio(final_map1)
        final_map = remove_optional_mapping(final_map, map_list_final)
        for i in final_map.keys():
            final_map[i] = ','.join(final_map[i])
        dict1 = {}
        dict1[c1.mongodb_cols[0]] = 'Auto_Map'
        dict1[c1.mongodb_cols[1]] = final_map
        temp_collection.delete_many({})
        temp_collection.insert_one(dict1)
        return render_template('Home.html', automap='AutoMap File Generated. Click below button to Download.')

@app.route('/download_mapfile')
def download_mapfile():
    all_records = temp_collection.find()
    df = pd.DataFrame(all_records)
    tds_dict = json.dumps(df[c1.mongodb_cols[1]][0], indent=0)
    f = c1.file_names[1] + ".json"
    with open(f, 'w') as fp:
        fp.write(str(tds_dict))
    return send_file(f,as_attachment=True)

@app.route('/uploadmapfile', methods = ['GET', 'POST'])
def upload_updated_map_file():
    if request.method == 'POST':
        collection_output = mydb[domain_name.upper()]
        f = request.files['mapfile']
        filename = secure_filename(f.filename)
        path = 'Temp/' + f.filename
        f.save(path)
        map_dict = read_map_details(path)
        dict1 = {}
        dict1[c1.mongodb_cols[2]] = c1.file_names[1] + '_' + domain_name
        dict1[c1.mongodb_cols[1]] = map_dict
        collection_output.delete_one({c1.mongodb_cols[2]: c1.file_names[1] + '_' + domain_name})
        collection_output.insert_one(dict1)

        df1 = pd.read_excel('Results/Training_Files/Training_Dataset.xlsx')
        map_dict1 = {}
        for i in map_dict.keys():
            if i in df1.columns:
                map_dict1[i] = map_dict[i]

        df2 = df1[map_dict1.keys()].reset_index(drop=True)
        principles_dict = {}
        for i in list(set(map_dict1.values())):
            principles_dict[i] = {}
        principles_dict = get_datatype_target(df2, principles_dict, map_dict)
        principles_dict = get_nullable_target(df2, principles_dict, map_dict)
        principles_dict = get_len_range_target(df2, principles_dict, map_dict)
        principles_dict = get_uniqueness_target(df2, principles_dict, map_dict)
        principle_file_loc = c1.file_names[0] + '_' + domain_name + '.json'
        principle_file_loc = 'Temp/' + principle_file_loc
        with open(principle_file_loc, 'w') as fp:
            fp.write(str(principles_dict))
        with open(principle_file_loc) as f:
            d1 = f.read()
        eval_dict = ast.literal_eval(d1)
        dict1 = {}
        dict1[c1.mongodb_cols[2]] = c1.file_names[0] + '_' + domain_name
        dict1[c1.mongodb_cols[1]] = eval_dict
        collection_output.delete_one({c1.mongodb_cols[2]: c1.file_names[0] + '_' + domain_name})
        collection_output.insert_one(dict1)
        return render_template('Home.html', trainingsuccess='Map File and Principle Metadata generated Successfully' , outputlocation=domain_name)

@app.route('/ReadLiveData', methods = ['GET', 'POST'])
def upload_file_liveData():
    if request.method == 'POST':
        filename = []
        for f in request.files.getlist('files'):
            filename.append(secure_filename(f.filename))
            path = 'Temp/' + f.filename
            f.save(path)
        for i in filename:
            i1 = 'Temp/' + i
            raw_ds = read_data(i1)
            if len(i.split('_')) > 1:
                tablename = i.split('_')[1].split('.')[0][:2]
            else:
                tablename = i.split('_')[-1].split('.')[0][:2]
            tablename = tablename.upper()
            raw_ds = raw_ds.fillna(0)
            path = 'Results/LiveData_Files/' + tablename + '.xlsx'
            raw_ds.to_excel(path, index=False)
        return render_template('LiveData.html', message='Local SAS File(s) Successfully Uploaded for LiveData', curr_dom_list=c2.curr_dom_list, dom_list=c2.dom_list)

@app.route('/downloadTargetDataset', methods = ['GET', 'POST'])
def downloadTargetDataset():
    if request.method == 'POST':
        filetype = request.form['filetype']
        filedomain = request.form['filedomain']
        if filetype == 'all' and filedomain == 'all':
            filetypes = ['xlsx', 'xpt']
            for i in range(len(all_domains)):
                for j in range(len(filetypes)):
                    f = './Results/Target_Files/' + all_domains[i] + '.' + filetypes[j]
                    return send_file(f, as_attachment=True)
        else:
            f = './Results/Target_Files/' + filedomain.upper() + '.' + filetype
            return send_file(f, as_attachment=True)

@app.route('/predict_saslocal', methods = ['GET', 'POST'])
def predict_saslocal():
    if request.method == 'POST':
        # domain_name = request.form['domains']
        domain_name = request.form.getlist('domains')
        l1 = []
        for i in domain_name:
            a = i.split('(')[-1][0:2]
            l1.append(a)
        domain_name = ','.join(l1)
        pred_domains = get_pred_domain_list(domain_name)
        derive_target_files(pred_domains)

        directmovecomplete = 'Variables derivation completed for domain(s): ' + ','.join(pred_domains).upper()
        domains = ','.join(pred_domains).upper().split(',')
        global all_domains
        all_domains = domains
        return render_template('LiveData.html', directmovemsg=directmovecomplete, domains=domains)

@app.route('/predict_oracle', methods = ['GET', 'POST'])
def predict_values_oracle():
    if request.method == 'POST':
        global schemaLike, numData, pred_domains
        schemaLike = request.form['schemaLike']
        # domain_name = request.form['domains']
        # numData = request.form['numDataset']
        numData = 1
        domain_name = request.form.getlist('domains')
        l1 = []
        for i in domain_name:
            a = i.split('(')[-1][0:2]
            l1.append(a)
        domain_name = ','.join(l1)
        pred_domains = get_pred_domain_list(domain_name)
        a = some(DB_type, DB_DNS, ocuser, ocpassword, pred_domains)
        a.test_func_live()
        derive_target_files(pred_domains)

        directmovecomplete = 'Variables derivation completed for domain(s): ' + ','.join(pred_domains).upper()
        domains = ','.join(pred_domains).upper().split(',')
        global all_domains
        all_domains = domains
        return render_template('LiveData.html', directmovemsg=directmovecomplete, domains=domains)

def get_pred_domain_list(domain_name):
    domains = domain_name.split(',')
    for i in c2.prereq_domain_list:
        ctr = 0
        for j in domains:
            if i.upper() == j.upper():
                ctr = 1
        if ctr == 0:
            domains.append(i)
    domains_final = []
    for i in domains:
        if i != '':
            domains_final.append(i)
    domains = []
    for i in domains_final:
        domains.append(i.upper())
    return domains

def derive_target_files(pred_domains):
    sdtmig_df = pd.DataFrame(pd.read_excel('SDTMIG-labelled.xlsx'))
    for i in pred_domains:
        principles_file_name = c1.file_names[0] + '_' + i
        map_file_name = c1.file_names[1] + '_' + i
        mycollection_inp = mydb[i]
        all_records = mycollection_inp.find()
        df = pd.DataFrame(all_records)
        for j in range(len(df)):
            if df[c1.mongodb_cols[2]][j] == map_file_name:
                map_dict = df[c1.mongodb_cols[1]][j]
            elif df[c1.mongodb_cols[2]][j] == principles_file_name:
                eval_dict = df[c1.mongodb_cols[1]][j]
        path = 'Results/LiveData_Files/' + i + '.xlsx'
        df = pd.read_excel(path)
        df1 = pd.DataFrame()
        df2 = pd.DataFrame()
        l1 = []
        l2 = []
        multicol = []
        for j in map_dict.keys():
            if map_dict[j] in l1:
                multicol.append(map_dict[j])
            else:
                l1.append(map_dict[j])
        for j in l1:
            if j not in multicol:
                l2.append(j)
        multicol = list(set(multicol))
        l1 = []
        for j in map_dict.keys():
            if map_dict[j] in multicol:
                l1.append(j)
        for j in map_dict.keys():
            if j in df.columns and map_dict[j] not in multicol:
                df1[map_dict[j]] = df[j]
            elif j in df.columns and map_dict[j] in multicol:
                df2[j] = df[j]
            elif j not in df.columns and map_dict[j] not in multicol:
                df1[map_dict[j]] = ''
        if len(multicol) > 0:
            df1[multicol[0]] = ''
            for k in range(df2.shape[0]):
                ctr1 = 0
                val = ''
                for j in df2.columns:
                    if df2[j][k] != '' and df2[j][k] == df2[j][k]:
                        ctr1 = ctr1 + 1
                        val = df2[j][k]
                if ctr1 > 1:
                    df1[multicol[0]][k] = c2.multicol_val
                else:
                    df1[multicol[0]][k] = val
        ner_pred_df = df1
        dict1 = {}
        for j in list(eval_dict.keys()):
            if j != '_id':
                dict1[j] = eval_dict[j]
        eval_dict = dict1
        ner_pred_df['DOMAIN'] = ner_pred_df['DOMAIN'].str[:2]
        # issues1 = ''
        # issues1 = validate_datatype(ner_pred_df, eval_dict, c1.principles_keys[0])
        # issues2 = validate_duplicated(ner_pred_df, eval_dict, c1.principles_keys[3])
        # issues3 = validate_nullable(ner_pred_df, eval_dict, c1.principles_keys[1])
        # issues4 = validate_len_range(ner_pred_df, eval_dict, c1.principles_keys[2])
        for i1 in range(len(ner_pred_df)):
            if 'SITEID' in ner_pred_df.columns:
                if ner_pred_df['SITEID'][i1] == 0:
                    ner_pred_df['SITEID'][i1] = '0000'
        # print(i)
        # print(ner_pred_df)
        if i == 'EC':
            EX_input = ner_pred_df
        elif i == 'DS':
            DS_input = ner_pred_df
        elif i == 'DD':
            DD_input = ner_pred_df
        elif i == 'DM':
            DM_input = ner_pred_df
        # all_issues = str(issues1) + str(issues2) + str(issues3) + str(issues4)
    # all_issues = all_issues.replace('True', '')
    address_v = pd.read_csv(c2.address, low_memory=False)
    country_v = pd.read_csv(c2.country, low_memory=False)
    contact_info_v = pd.read_csv(c2.contact_info, low_memory=False)
    person_v = pd.read_csv(c2.person, low_memory=False)
    study_alias_v = pd.read_csv(c2.study_alias, low_memory=False)
    input_list = [EX_input, DS_input, DD_input, DM_input]

    EX_input = general_function(EX_input)
    DS_input = general_function(DS_input)
    DD_input = general_function(DD_input)
    DM_input = general_function(DM_input)

    DM_input = generate_DM(EX_input, DS_input, DD_input, DM_input)
    # DM_input.to_excel("Results/Target_Files/DM_DirectMove.xlsx", index=False)
    # DM_input = DM_input.fillna('')

    pods_df = pods_rules(address_v, country_v, contact_info_v, person_v, study_alias_v, DM_input)

    DM_input[['STUDYID', 'SITEID']] = DM_input[['STUDYID', 'SITEID']].astype(str)
    if not pods_df.empty:
        DM_input = pd.merge(DM_input, pods_df, on=['STUDYID', 'SITEID'], how='left')
    else:
        DM_input = DM_input.assign(INVID=np.nan, INVNAME=np.nan, COUNTRY=np.nan)

    DM_ref_var = DM_input[['USUBJID', 'DMDTC', 'RFSTDTC', 'RFXENDTC']]
    EX_input_joined = pd.merge(EX_input, DM_ref_var, on='USUBJID', how='left').drop_duplicates()
    DS_input_joined = pd.merge(DS_input, DM_ref_var, on='USUBJID', how='left').drop_duplicates()
    DD_input_joined = pd.merge(DD_input, DM_ref_var, on='USUBJID', how='left').drop_duplicates()
    #
    EX_input_joined = EX_input_joined.fillna(0)
    DS_input_joined = DS_input_joined.fillna(0)
    DD_input_joined = DD_input_joined.fillna(0)
    DM_ref_var = DM_ref_var.fillna(0)

    EX_input['EPOCH'] = epoch_cal(EX_input_joined['ECSTDTC'], EX_input_joined['RFSTDTC'],
                                  EX_input_joined['RFXENDTC'])
    DS_input['EPOCH'] = epoch_cal(DS_input_joined['DSSTDTC'], DS_input_joined['RFSTDTC'],
                                  DS_input_joined['RFXENDTC'])
    DD_input['EPOCH'] = epoch_cal(DD_input_joined['DDDTC'], DD_input_joined['RFSTDTC'], DD_input_joined['RFXENDTC'])

    EX_input['ECSTDY'] = dy_cal(EX_input_joined['ECSTDTC'], EX_input_joined['RFSTDTC'])
    EX_input['ECENDY'] = dy_cal(EX_input_joined['ECENDTC'], EX_input_joined['RFSTDTC'])
    DM_input['DMDY'] = dy_cal(DM_ref_var['DMDTC'], DM_ref_var['RFSTDTC'])
    DS_input['DSSTDY'] = dy_cal(DS_input_joined['DSSTDTC'], DS_input_joined['RFSTDTC'])
    DD_input['DDDY'] = dy_cal(DD_input_joined['DDDTC'], DD_input_joined['RFSTDTC'])
    DD_input['DDSTRESC'] = STRESC(DD_input['DDORRES'])

    EX_input = sdtmig_col_validation(sdtmig_df, EX_input, EX_input['DOMAIN'][1])
    DM_input = sdtmig_col_validation(sdtmig_df, DM_input, DM_input['DOMAIN'][1])
    DS_input = sdtmig_col_validation(sdtmig_df, DS_input, DS_input['DOMAIN'][1])
    DD_input = sdtmig_col_validation(sdtmig_df, DD_input, DD_input['DOMAIN'][1])

    write(EX_input, "EC")
    write(DM_input, "DM")
    write(DS_input, "DS")
    write(DD_input, "DD")

    EX_input.to_excel("Results/Target_Files/EC.xlsx", index=False)
    DS_input.to_excel("Results/Target_Files/DS.xlsx", index=False)
    DD_input.to_excel("Results/Target_Files/DD.xlsx", index=False)
    DM_input.to_excel("Results/Target_Files/DM.xlsx", index=False)

    dir = 'Temp'
    for f in os.listdir(dir):
        os.remove(os.path.join(dir, f))
    db = pymongo.MongoClient().mygrid
    fs = gridfs.GridFS(db)
    for i in pred_domains:
        outdata = db.fs.files.find_one({'filename': i.upper()})
        if outdata != None:
            myid = outdata['_id']
            fs.delete(myid)
        path = 'Results/Target_Files/' + i.upper() + '.xlsx'
        filedata = open(path, 'rb')
        data = filedata.read()
        fs.put(data, filename=i.upper())

def read_data(file):
    data = pyreadstat.read_sas7bdat(file)
    return data[0]

def generate_final_principles(domain_name):
    domains = []
    map_list_final = []
    principles_list = []
    principles_final = {}
    for i in mydb.list_collection_names():
        if i != domain_name:
            mycollection = mydb[i]
            all_records = mycollection.find()
            df = pd.DataFrame(all_records)
            for j in range(len(df)):
                if c1.mongodb_cols[2] in list(df.columns):
                    if df[c1.mongodb_cols[2]][j].startswith(c1.file_names[0]):
                        domains.append(i)
                        dict1 = df[c1.mongodb_cols[1]][j]
                        for k in dict1.keys():
                            if k not in principles_list:
                                principles_list.append(k)
                                principles_final[k] = dict1[k]
                    if df[c1.mongodb_cols[2]][j].startswith(c1.file_names[1]):
                        dict1 = df[c1.mongodb_cols[1]][j]
                        for k in dict1.keys():
                            map_list_final.append(k)
    return principles_final, domains, map_list_final

def filterprinciples(principles_final, rds_principles):
    for i in rds_principles.keys():
        map_list = []
        for j in principles_final.keys():
            if rds_principles[i][c1.principles_keys[0]] == principles_final[j][c1.principles_keys[0]]:
                map_list.append(j)
        rds_principles[i][c1.principles_keys[4]] = map_list
    for i in rds_principles.keys():
        map_list = []
        for j in principles_final.keys():
            if j in rds_principles[i][c1.principles_keys[4]]:
                if rds_principles[i][c1.principles_keys[1]] == principles_final[j][c1.principles_keys[1]]:
                    map_list.append(j)
        rds_principles[i][c1.principles_keys[4]] = map_list
    for i in rds_principles.keys():
        map_list = []
        for j in principles_final.keys():
            if j in rds_principles[i][c1.principles_keys[4]]:
                if rds_principles[i][c1.principles_keys[3]].upper() == principles_final[j][c1.principles_keys[3]].upper():
                    map_list.append(j)
        rds_principles[i][c1.principles_keys[4]] = map_list
    final_map = {}
    for i in rds_principles.keys():
        final_map[i] = rds_principles[i][c1.principles_keys[4]]
    return final_map

def updatedomain(final_map, domains, suffix_cols, var_tgt, domain_name):
    final_map1 = {}
    for i in final_map.keys():
        l1 = final_map[i]
        l2 = []
        for j in l1:
            val = j
            prefix = j[0:2]
            suffix = j[2:]
            if (prefix in domains) and (suffix in suffix_cols):
                val = val.replace(prefix, domain_name)
            if val in var_tgt:
                l2.append(val)
        final_map1[i] = list(set(l2))
    return final_map1

def getfuzzratio(final_map1):
    for i in final_map1.keys():
        val1 = final_map1[i]
        ctr = 0
        l1 = []
        if len(val1) > 0:
            for j in val1:
                ctr1 = fuzz.ratio(i, j)
                if ctr1 > ctr:
                    ctr = ctr1
                    l1 = [j]
        final_map1[i] = l1
    return final_map1

def remove_optional_mapping(final_map, map_list_final):
    optional_dict = {}
    op_list1 = []
    for i in c1.optional_list_end:
        optional_dict[i] = 0
        for j in map_list_final:
            if j.endswith(i):
                optional_dict[i] = 1
        if optional_dict[i] == 0:
            op_list1.append(i)
    for i in op_list1:
        for j in final_map.keys():
            if j.endswith(i):
                final_map[j] = []
    return final_map

def get_datatype(rds, principles_dict):
    for i in rds.columns:
        principles_dict[i] = {}
        datatype_list = []
        for j in range(len(rds)):
            value = rds[i][j]
            if isinstance(value, float):
                val_type = c1.value_float
            elif str(value).count('.') == 1 and str(value).replace('.', '').isdigit() == True:
                val_type = c1.value_float
            elif str(value).isdigit() == True or isinstance(value, int):
                val_type = c1.value_int
            else:
                try:
                    parse(str(value), fuzzy=False)
                    val_type = c1.value_date
                except ValueError:
                    val_type = c1.value_str
            datatype_list.append(val_type)
        if len(set(datatype_list)) > 1:
            count_dict = Counter(datatype_list)
            sort_count_list = sorted(count_dict.items(), key=lambda x: x[1], reverse=True)
            principles_dict[i][c1.principles_keys[0]] = sort_count_list[0][0]
        else:
            principles_dict[i][c1.principles_keys[0]] = list(set(datatype_list))[0]
    return principles_dict

def get_nullable(rds, principles_dict):
    for i in rds.columns:
        ctr = 0
        for j in range(len(rds)):
            if rds[i][j] == '':
                ctr = ctr + 1
        if ctr == 0 and rds[i].isna().sum() == 0:
            principles_dict[i][c1.principles_keys[1]] = c1.value_no
        else:
            principles_dict[i][c1.principles_keys[1]] = c1.value_yes
    return principles_dict

def get_len_range(rds, principles_dict):
    for i in rds.columns:
        len_range_list = []
        min_len = rds[i].apply(str).apply(len).min()
        max_len = rds[i].apply(str).apply(len).max()
        len_range_list.append(min_len)
        len_range_list.append(max_len)
        principles_dict[i][c1.principles_keys[2]] = tuple(len_range_list)
    return principles_dict

def get_uniqueness(rds, principles_dict):
    for i in rds.columns:
        if len(rds[i].value_counts()) == len(rds):
            principles_dict[i][c1.principles_keys[3]] = c1.value_unique
        else:
            principles_dict[i][c1.principles_keys[3]] = c1.value_duplicated
    return principles_dict

def get_datatype_target(rds, principles_dict, map_dict):
    for i in principles_dict.keys():
        for j in rds.columns:
            if map_dict[j] == i:
                datatype_list = []
                for num in range(len(rds)):
                    value = rds[j][num]
                    if isinstance(value, float):
                        val_type = c1.value_float
                    elif str(value).count('.') == 1 and str(value).replace('.', '').isdigit() == True:
                        val_type = c1.value_float
                    elif str(value).isdigit() == True or isinstance(value, int):
                        val_type = c1.value_int
                    else:
                        try:
                            parse(str(value), fuzzy=False)
                            val_type = c1.value_date
                        except ValueError:
                            val_type = c1.value_str
                    datatype_list.append(val_type)
        if len(set(datatype_list)) > 1:
            count_dict = Counter(datatype_list)
            sort_count_list = sorted(count_dict.items(), key=lambda x: x[1], reverse=True)
            principles_dict[i][c1.principles_keys[0]] = sort_count_list[0][0]
        else:
            principles_dict[i][c1.principles_keys[0]] = list(set(datatype_list))[0]
    return principles_dict

def get_nullable_target(rds, principles_dict, map_dict):
    for i in principles_dict.keys():
        for j in rds.columns:
            if map_dict[j] == i:
                if rds[j].isna().sum() == 0 and len(rds[rds[j] == '']) == 0:
                    principles_dict[i][c1.principles_keys[1]] = c1.value_no
                else:
                    principles_dict[i][c1.principles_keys[1]] = c1.value_yes
    return principles_dict

def get_len_range_target(rds, principles_dict, map_dict):
    values = Counter(map_dict.values())
    mult = dict((key, val) for (key, val) in values.items() if val > 1)
    for key in mult.keys():
        mult[key] = [i for (i, j) in map_dict.items() if j == key]
    mult_occur = []
    for i in principles_dict.keys():
        for j in rds.columns:
            if i in mult.keys() and i not in mult_occur:
                len_range_list = []
                min_list = []
                max_list = []
                for src in mult[i]:
                    min_len = rds[src].apply(str).apply(len).min()
                    max_len = rds[src].apply(str).apply(len).max()
                    min_list.append(min_len)
                    max_list.append(max_len)
                len_range_list.append(min(min_list))
                len_range_list.append(max(max_list))
                principles_dict[i][c1.principles_keys[2]] = tuple(len_range_list)
                mult_occur.append(i)
            elif i not in mult_occur:
                if map_dict[j] == i:
                    len_range_list = []
                    min_len = rds[j].apply(str).apply(len).min()
                    max_len = rds[j].apply(str).apply(len).max()
                    len_range_list.append(min_len)
                    len_range_list.append(max_len)
                    principles_dict[i][c1.principles_keys[2]] = tuple(len_range_list)
    return principles_dict

def get_uniqueness_target(rds, principles_dict, map_dict):
    for i in principles_dict.keys():
        for j in rds.columns:
            if map_dict[j] == i:
                if len(rds[j].value_counts()) == rds.shape[0]:
                    principles_dict[i][c1.principles_keys[3]] = c1.value_unique
                else:
                    principles_dict[i][c1.principles_keys[3]] = c1.value_duplicated
    return principles_dict

def read_map_details(filename):
    with open(filename) as f:
        d1 = f.read()
    map_dict = ast.literal_eval(d1)
    map_dict1 = {}
    for key in map_dict.keys():
        if map_dict[key] != '':
            map_dict1[key] = map_dict[key]
    map_dict = map_dict1
    return map_dict

def evaluate_nullable(df, evaldict):
    eval_nullable = []
    for i in df.columns:
        for j in evaldict.keys():
            if i == j:
                if evaldict[i][c1.principles_keys[1]] == c1.value_no:
                    if df[i].isna().sum() != 0 or len(df[df[j] == '']) != 0:
                        eval_nullable.append(i)
                        #for k in range(len(df)):
                        #    if df[i][k].isna():
                        #        print('WARNING:', i, 'should not contain any null value. But record', k, 'is null')
    return eval_nullable

def evaluate_range(df, evaldict):
    eval_range = []
    for i in df.columns:
        for j in evaldict.keys():
            if i == j:
                df[i] = df[i].map(lambda x: str(x))
                x = len(min(df[j], key=len))
                y = len(max(df[j], key=len))
                if x < evaldict[i][c1.principles_keys[2]][0] and y > evaldict[i][c1.principles_keys[2]][1]:
                    eval_range.append(i)
    return eval_range

def evaluate_uniqueness(df, evaldict):
    eval_unique = []
    for i in df.columns:
        for j in evaldict.keys():
            if i == j:
                if evaldict[i][c1.principles_keys[3]] == c1.value_unique:
                    if len(df[i].value_counts()) < df.shape[0]:
                        eval_unique.append(i)
    return eval_unique

def evaluate_datatype(df, evaldict):
    eval_datatype = []
    for i in evaldict.keys():
        for j in df.columns:
            if i == j:
                datatype_list = []
                for num in range(len(df)):
                    value = df[j].iloc[num]
                    if isinstance(value, float):
                        val_type = c1.value_float
                    elif value.count('.') == 1 and value.replace('.', '').isdigit() == True:
                        val_type = c1.value_float
                    elif value.isdigit() == True or isinstance(value, int):
                        val_type = c1.value_int
                    else:
                        try:
                            parse(value, fuzzy=False)
                            val_type = c1.value_date
                        except ValueError:
                            val_type = c1.value_str
                    datatype_list.append(val_type)
        if len(set(datatype_list)) > 1:
            count_dict = Counter(datatype_list)
            sort_count_list = sorted(count_dict.items(), key=lambda x: x[1], reverse=True)
            if evaldict[i][c1.principles_keys[0]] != sort_count_list[0][0]:
                eval_datatype.append(i)
        else:
            if evaldict[i][c1.principles_keys[0]] != list(set(datatype_list))[0]:
                eval_datatype.append(i)
    return eval_datatype

def validate_datatype(data, meta, key):
    errors = ''
    def check_cell_type(data, var, cell_type):
        count = 0
        for index, val in data[var].iteritems():
            if isinstance(val, cell_type) == False:
                if pd.notnull(val) == False:
                    # print(count,': It is a missing value')
                    return True
                else:
                    # print(count ,'WARNING: Type mismatch in variable',i,'Source type: ',cell_type,'Target type:',type(val))
                    return False
                count += 1
        # self.counter1 = count

    for i in data.columns:
        for j in meta.keys():
            if i == j:
                # print('Variable: '+ i , '=====> Key_from_Meta: '+j)
                # print('--------------------')
                if meta[i][key] == c1.value_float:
                    if check_cell_type(data, i, float) == False:
                        for ind, val in data[i].iteritems():
                            if str(val).count('.') == 1 and str(val).replace('.', '').isdigit() == True:
                                return True
                            else:
                                return False
                                #cell_type = float
                                #count = self.counter1
                                print(count, 'WARNING: Type mismatch in variable', i, 'Source type: ', cell_type,
                                        'Target type:', type(val))
                elif meta[i][key] == c1.value_int:
                    if check_cell_type(data, i, float) == False:
                        for ind, val in data[i].iteritems():
                            if str(val).isdigit() == True:
                                return True
                            else:
                                return False
                                #cell_type = float
                                #count = self.counter1
                                print(count, 'WARNING: Type mismatch in variable', i, 'Source type: ', cell_type,
                                          'Target type:', type(val))
                elif meta[i][key] == c1.value_str:
                    check_cell_type(data, i, str)
                elif meta[i][key] == c1.value_date:
                    for index, val in data[i].iteritems():
                        try:
                            parse(val, fuzzy=False)
                            pass
                        except ValueError:
                            errors = errors + c1.principles_keys[0] + ' :' + str(index), 'WARNING: Type mismatch in variable', i, 'Source type: ', c1.value_date, 'Target type:', str(type(val)) + ';'
                            # print(c1.principles_keys[0] + ' :' + index, 'WARNING: Type mismatch in variable', i, 'Source type: ', c1.value_date,
                            #           'Target type:', type(val))
    return errors

def validate_duplicated(data, meta, key):
    errors = ''
    for i in data.columns:
        for j in meta.keys():
            if i == j:
                if meta[i][key] == c1.value_unique:
                    if len(data) != len(data[i].value_counts()):
                        s = data[i].value_counts()
                        s = s[s > 1]
                        for d_ind in s.index:
                            errors = errors + c1.principles_keys[3] + ' : WARNING: ' + i + ' Found', s.loc[d_ind], 'observations for', d_ind
                            errors = errors + ';'
                            # print(c1.principles_keys[3] + ' : WARNING: ' + i + ' Found', s.loc[d_ind], 'observations for', d_ind)
    return errors

def validate_nullable(data, meta, key):
    errors = ''
    for i in data.columns:
        for j in meta.keys():
            if i == j:
                if meta[i][key] == c1.value_no:
                    for ind, val in data[i].iteritems():
                        if pd.notnull(val) == False:
                            errors = errors + c1.principles_keys[1] + ' : WARNING:', i, 'should not contain any null value. But record', ind, 'is null' + ';'
                            # print(c1.principles_keys[1] + ' : WARNING:', i, 'should not contain any null value. But record', ind, 'is null')
    return errors

def validate_len_range(data, meta, key):
    errors = ''
    for i in data.columns:
        for j in meta.keys():
            if i == j:
                count = 0
                for ind, val in data[i].iteritems():
                    len_val = len(str(val))
                    if len_val in range(meta[i][c1.principles_keys[2]][0], meta[i][c1.principles_keys[2]][1] + 1):
                        pass
                    elif pd.notnull(val) == False:
                        pass
                    else:
                        # errors = errors + c1.principles_keys[2] + ' :' + i, count, 'WARNING: Length is exceeding from the source' + ';'
                        print(c1.principles_keys[2] + ' :' + i, count, 'WARNING: Length is exceeding from the source')
                    count += 1
    return errors

def validate_date(date_text):
    try:
        date_text = str(date_text).replace('T', ' ')
        if len(date_text.split()) == 2:
            date_converted = datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
            return date_converted.date()
        else:
            date_converted = datetime.strptime(date_text, '%Y-%m-%d')
            return date_converted.date()
    except ValueError:
        pass
        # raise ValueError(date_text,"->Incorrect data format, should be YYYY-MM-DD")

def epoch_cal(STDTC_list,RFSTDTC_list,RFXENDTC_list):
    epoch_list = []
    for STDTC_var,RFSTDTC_var,RFXENDTC_var in zip(STDTC_list,RFSTDTC_list,RFXENDTC_list):
        epoch_list.append(EPOCH_rule(STDTC_var,RFSTDTC_var,RFXENDTC_var))
    return epoch_list

def dy_cal(DTC_list,RFSTDTC_list):
    dy_list = []
    for DTC_var,RFSTDTC_var in zip(DTC_list,RFSTDTC_list):
        dy_list.append(DY_rule(DTC_var,RFSTDTC_var))
    return dy_list

def DY_rule(DTC_var,RFSTDTC_var):
    try:
        DY_var = ""
        if RFSTDTC_var!=0 and DTC_var!=0 :
            DTC_var = validate_date(DTC_var)
            RFSTDTC_var = validate_date(RFSTDTC_var)
            if DTC_var < RFSTDTC_var :
                DY_var = (DTC_var - RFSTDTC_var).days
                return DY_var
            else:
                DY_var = (DTC_var - RFSTDTC_var).days+1
                return DY_var
        else:
            return DY_var
    except Exception as e:
        print(DTC_var,RFSTDTC_var,traceback.format_exc())
        print(e)

def STRESC(ORRES_var):
    try:
        STRESC_list = []
        for i in ORRES_var:
            if isinstance(i, str):
                STRESC_list.append(i)
            else:
                STRESC_list.append(str(i))
        return STRESC_list
    except Exception as e:
        # print(DTC_var,RFSTDTC_var,traceback.format_exc())
        print(e)

def EPOCH_rule(STDTC_var,RFSTDTC_var,RFXENDTC_var):
    try:
        EPOCH_var =""
        STDTC_var = validate_date(STDTC_var)
        RFSTDTC_var = validate_date(RFSTDTC_var)
        RFXENDTC_var = validate_date(RFXENDTC_var)
        if STDTC_var and RFSTDTC_var and RFXENDTC_var:
            if STDTC_var < RFSTDTC_var:
                EPOCH_var = 'SCREENING'
                return EPOCH_var
            elif STDTC_var>=RFSTDTC_var and STDTC_var<=RFXENDTC_var:
                EPOCH_var = 'TREATMENT'
                return EPOCH_var
            elif STDTC_var > RFXENDTC_var:
                EPOCH_var = 'FOLLOW-UP'
                return EPOCH_var
        else:
            return EPOCH_var
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def SEQ_rule(USUBJID):
    try:
        seq_dict = {}
        SEQ_final = []
        counter = 0
        for i in USUBJID:
            #print(i)
            if i not in seq_dict.keys():
                counter=counter+1
                seq_dict.update({i:(counter)})
        return seq_dict
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def BLFL_rule(EXSTDTC_list,STDTC_var):
    try:
        BLFL_dict = {}
        EXSTDTC_list = list(filter(None, EXSTDTC_list))
        min_EXSTDTC = min(EXSTDTC_list)
        min_EXSTDTC = validate_date(min_EXSTDTC)
        print(min_EXSTDTC)
        for i in STDTC_var:
            i = validate_date(i)
            if i < min_EXSTDTC:
                BLFL_dict.update({str(i): 'Y'})
            else:
                BLFL_dict.update({str(i): ''})
        return BLFL_dict
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def USUBJID_rule(STUDYID, SITEID, SUBJID):
    try:
        if STUDYID and SITEID and SUBJID:
            USUBJID = (" ").join((str(STUDYID), str(SITEID), str(SUBJID)))
        elif STUDYID==0 or SITEID==0 or SUBJID==0:
            if not SITEID:
                USUBJID = (" ").join((str(STUDYID), str('0000'), str(SUBJID)))
            elif not STUDYID:
                USUBJID = (" ").join((str('0000'), str(SITEID), str(SUBJID)))
            elif not SUBJID:
                USUBJID = (" ").join((str(STUDYID), str(SITEID), str('0000')))
        else:
            USUBJID = ''
        return USUBJID
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def RFSTDTC_rule(EX_df, DS_df):
    try:
        EX_df = EX_df.dropna(subset=['ECSTDTC'])
        grouped_df = EX_df.join(EX_df.groupby('USUBJID')['ECSTDTC'].agg(['min']), on='USUBJID')
        EXSTDTC_min_df = grouped_df[['USUBJID','min']].copy().drop_duplicates()
        DSDCOD_df = DS_df[(DS_df['DSDECOD'] == 'ENROLLED') | (DS_df['DSDECOD'] == 'RANDOMIZED')]['USUBJID']
        result_df = EXSTDTC_min_df.merge(DSDCOD_df , on ='USUBJID',how= 'inner')
        result_df.rename(columns={'min':'RFSTDTC'},inplace=True)
        result_df.drop(result_df.index[result_df['RFSTDTC'] == ''], inplace=True)
        return result_df
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def RFICDTC_rule(DS_df):
    try:
        DSDCOD_df = DS_df[(DS_df['DSDECOD'] == 'INFORMED CONSENT')]
        grouped_df = DSDCOD_df.join(DSDCOD_df.groupby('USUBJID')['DSSTDTC'].agg(['min']), on='USUBJID')
        DSSTDTC_min_df = grouped_df[['USUBJID','min']].drop_duplicates()
        DSSTDTC_min_df.rename(columns={'min':'RFICDTC'},inplace=True)
        return DSSTDTC_min_df
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def DD_rules(DD_df):
    try:
        DD_df.drop(DD_df.index[DD_df['DDDTC'] == ''], inplace=True)
        DD_df_sorted = DD_df.sort_values(['USUBJID', 'DDDTC'], ascending=[True, True])
        grouped_df = DD_df_sorted.join(DD_df_sorted.groupby('USUBJID')['DDDTC'].agg(['max']), on='USUBJID')
        DDDTC_min_df = grouped_df[['USUBJID','max']].drop_duplicates()
        DDDTC_min_df.rename(columns={'max':'DTHDTC'},inplace=True)
        DDDTC_min_df['DTHFL'] = 'Y'
        df_merged = DD_df_sorted.merge(DDDTC_min_df,on=['USUBJID'])
        final = df_merged[['USUBJID','DDDTC','DTHDTC','DTHFL']]
        result = DD_df_sorted.merge(final,on=['USUBJID','DDDTC'],how='inner')
        return result[['USUBJID','DDDTC','DTHDTC','DTHFL']]
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def RFPENDTC_rule(DD_df):
    try:
        RFPENDTC = []
        DD_values = DD_rules(DD_df)
        for dtc,flag in zip(DD_values['DDDTC'],DD_values['DTHFL']):
            if flag == 'Y' and dtc:
                RFPENDTC.append(dtc)
            else:
                RFPENDTC.append('')
        DD_values['RFPENDTC']=RFPENDTC
        final_df = DD_values[['USUBJID','RFPENDTC']]
        return final_df
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def RFENDTC_rule(DS_df):
    try:
        DSDCOD_df = DS_df[(DS_df['DSDECOD'] == 'SCREEN FAILURE')]
        grouped_df = DSDCOD_df.join(DSDCOD_df.groupby('USUBJID')['DSSTDTC'].agg(['max']), on='USUBJID')
        DSSTDTC_min_df = grouped_df[['USUBJID','max']].drop_duplicates()
        DSSTDTC_min_df.rename(columns={'max':'RFENDTC'},inplace=True)
        return DSSTDTC_min_df
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def RFXSTDTC_rule(EX_df):
    try:
        EX_df = EX_df.dropna(subset=['ECSTDTC'])
        grouped_df = EX_df.join(EX_df.groupby('USUBJID')['ECSTDTC'].agg(['min']), on='USUBJID')
        EXSTDTC_min_df = grouped_df[['USUBJID','min']].drop_duplicates()
        EXSTDTC_min_df.rename(columns={'min':'RFXSTDTC'},inplace=True)
        EXSTDTC_min_df.drop(EXSTDTC_min_df.index[EXSTDTC_min_df['RFXSTDTC'] == ''], inplace=True)
        return EXSTDTC_min_df
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def RFXENDTC_rule(EX_df):
    try:
        EX_df = EX_df.dropna(subset=['ECSTDTC'])
        grouped_df = EX_df.join(EX_df.groupby('USUBJID')['ECSTDTC'].agg(['max']), on='USUBJID')
        EXSTDTC_max_df = grouped_df[['USUBJID','max']].drop_duplicates()
        EXSTDTC_max_df.rename(columns={'max':'RFXENDTC'},inplace=True)
        EXSTDTC_max_df.drop(EXSTDTC_max_df.index[EXSTDTC_max_df['RFXENDTC'] == ''], inplace=True)
        return EXSTDTC_max_df
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def pods_rules(address_v, country_v, contact_info_v, person_v, study_alias_v, DM_df):
    try:
        df_select_1 = address_v[address_v['DELETE_FLAG'] == 'N'][
            ['ADDRESS_ID', 'COUNTRY_NAME', 'STATE_PROVINCE_COUNTY']]
        df_joined_1 = pd.merge(df_select_1, country_v, on='COUNTRY_NAME', how="inner").drop_duplicates()
        df_filter = df_joined_1[df_joined_1['OBSOLETE_COUNTRY_FLG'] == 'N'][
            ['ADDRESS_ID', 'COUNTRY_NAME', 'STATE_PROVINCE_COUNTY', 'COUNTRY_ISO_CODE']]
        df_filter.rename(columns={'COUNTRY_ISO_CODE': 'COUNTRY'}, inplace=True)
        df_filter['ADDRESS_ID'] = df_filter['ADDRESS_ID'].apply(pd.to_numeric)

        contact_info_v_filtered = contact_info_v[(contact_info_v['CONTACT_ROLE'] == 'PRINCIPAL INVESTIGATOR') & (
                    contact_info_v['CONTACT_STATUS'] == 'ACTIVE') & (contact_info_v['PRIMARY_CONTACT'] == 'Y') & (
                                                             contact_info_v['DELETE_FLAG'] == 'N')]
        person_v_filtered = person_v[person_v['DELETE_FLAG'] == 'N']
        study_alias_v_filtered = study_alias_v[
            (study_alias_v['ALIAS_TYPE'] == 'PROTOCOL ID') & (study_alias_v['DELETE_FLAG'] == 'N')]

        ci_p_joined = pd.merge(contact_info_v_filtered, person_v_filtered, on='PERSON_ID',
                               how="inner").drop_duplicates()

        ci_p_sa_joined = pd.merge(ci_p_joined, study_alias_v_filtered, on='STUDY_ID', how="inner").drop_duplicates()
        ci_p_sa_selected = ci_p_sa_joined[
            ['PERSON_ID', 'STUDY_ID', 'STUDY_SITE_NUMBER', 'ADDRESS_ID', 'PERSON_FULL_NAME']].copy()
        ci_p_sa_selected.rename(
            columns={'STUDY_SITE_NUMBER': 'TRIAL_NO', 'PERSON_FULL_NAME': 'INVNAME', 'PERSON_ID': 'INVID'},
            inplace=True)
        ci_p_sa_selected['ADDRESS_ID'] = ci_p_sa_selected['ADDRESS_ID'].apply(pd.to_numeric)

        final_merged = pd.merge(ci_p_sa_selected, df_filter, on=['ADDRESS_ID'], how='inner').drop_duplicates()
        studyid_df = DM_df[['STUDYID', 'SITEID']].drop_duplicates()
        studyid_df[['STUDYID', 'SITEID']] = studyid_df[['STUDYID', 'SITEID']].astype(str)
        final_merged[['STUDY_ID', 'TRIAL_NO']] = final_merged[['STUDY_ID', 'TRIAL_NO']].astype(str)
        final_country = \
        pd.merge(studyid_df, final_merged, left_on=['STUDYID', 'SITEID'], right_on=['STUDY_ID', 'TRIAL_NO'],
                 how='left')[['STUDYID', 'SITEID', 'INVID', 'INVNAME', 'COUNTRY']].drop_duplicates()

        final_country = final_country.sort_values(by=['COUNTRY'])

        return final_country
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def general_function(source_input):
    try:
        USUBJID_final = []
        SEQ_final = []
        for STUDYID, SITEID, SUBJID in zip(source_input['STUDYID'],source_input['SITEID'],source_input['SUBJID']):
            USUBJID_final.append(USUBJID_rule(STUDYID, SITEID, SUBJID))
        source_input['USUBJID'] = USUBJID_final
        seq_dict = SEQ_rule(source_input['USUBJID'])
        for i in source_input['USUBJID']:
            for key,value in seq_dict.items():
                if i == key:
                    SEQ_final.append(value)
                elif i == '':
                    SEQ_final.append('')
        domain = source_input['DOMAIN'][1]
        source_input[domain+'SEQ'] = SEQ_final
        return source_input.drop_duplicates().copy()
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def generate_DM(EX_input,DS_input,DD_input,DM_input):
    try:
        RFSTDTC_df = RFSTDTC_rule(EX_input, DS_input)
        if not RFSTDTC_df.empty:
            DM_input = pd.merge(DM_input,RFSTDTC_df, on = 'USUBJID', how = 'left')
        else:
            DM_input['RFSTDTC'] = np.nan
        RFENDTC_df = RFENDTC_rule(DS_input)
        if not RFENDTC_df.empty:
            DM_input = pd.merge(DM_input,RFENDTC_df, on = 'USUBJID', how = 'left')
        else:
            DM_input['RFENDTC'] = np.nan
        RFXSTDTC_df = RFXSTDTC_rule(EX_input)
        if not RFXSTDTC_df.empty:
            DM_input = pd.merge(DM_input,RFXSTDTC_df, on = 'USUBJID', how = 'left')
        else:
            DM_input['RFXSTDTC'] = np.nan
        RFXENDTC_df = RFXENDTC_rule(EX_input)
        if not RFXENDTC_df.empty:
            DM_input = pd.merge(DM_input,RFXENDTC_df, on = 'USUBJID', how = 'left')
        else:
            DM_input['RFXENDTC'] = np.nan
        RFICDTC_df = RFICDTC_rule(DS_input)
        if not RFICDTC_df.empty:
            DM_input = pd.merge(DM_input,RFICDTC_df, on = 'USUBJID', how = 'left')
        else:
            DM_input['RFICDTC'] = np.nan
        DD_var_df = DD_rules(DD_input)
        if not DD_var_df.empty:
            DM_input = pd.merge(DM_input,DD_var_df, on = 'USUBJID', how = 'left')
        else:
            DM_input['DTHFL'] = np.nan
            DM_input['DTHDTC'] = np.nan
        RFPENDTC_df = RFPENDTC_rule(DD_input)
        if not RFPENDTC_df.empty:
            DM_input = pd.merge(DM_input,RFPENDTC_df, on = 'USUBJID', how = 'left')
        else:
            DM_input['RFPENDTC'] = np.nan
        return DM_input.drop_duplicates()
    except Exception as e:
        print(traceback.format_exc())
        print(e)

def write(df,domain):
    path = "Results/Target_Files/"+domain+".xpt"
    pyreadstat.write_xport(df, path)

def sdtmig_col_validation(sdtmig_df, source_df, domain_name):
    sdtmig_df_filter = sdtmig_df[sdtmig_df['Domain Prefix'] == domain_name]
    var_name = sdtmig_df_filter['Variable Name']
    data_type = sdtmig_df_filter['Type']
    core = sdtmig_df_filter['Core']
    source_df = source_df.reindex(columns=var_name)
    for var, cor, typ in zip(var_name, core, data_type):
        if (var in source_df.columns) and (cor != 'Perm') and (typ == 'Char'):
            source_df = source_df.astype({var: 'string'})
        elif (var in source_df.columns) and (cor != 'Perm') and (typ == 'Num'):
            source_df[var] = pd.to_numeric(source_df[var], downcast='integer')
        elif (var not in source_df.columns) and (cor == 'Exp' or cor == 'Req'):
            source_df[var] = np.nan
        elif (var in source_df.columns) and (source_df[var].isnull().values.all()) and (cor == 'Perm'):
            source_df = source_df.drop(var, axis=1)
        elif (var in source_df.columns) and (not source_df[var].isnull().values.all()) and (cor == 'Perm') and (
                typ == 'Num'):
            source_df[var] = pd.to_numeric(source_df[var], downcast='integer')
        elif (var in source_df.columns) and (not source_df[var].isnull().values.all()) and (cor == 'Perm') and (
                typ == 'Char'):
            source_df = source_df.astype({var: 'string'})
    return source_df.copy()


class ETL:
    def __init__(self, DB_Type, DNS, User, pwd) -> None:
        self.DB_Type = DB_Type
        self.DNS = DNS
        self.UserName = User
        self.Password = pwd
        self.Schema_Like = None
        self.SynName = None
        self.DSCount = 10
        self.Schema_Syn_DF = pd.DataFrame()
        self.ConnectMethod = None
        if DB_Type == 'OC':
            self.ConnectMethod = 'OC_Connect'
        elif DB_Type == 'Oracle_Connect':
            self.ConnectMethod = 'Oracle_Connect'
        oc_config.user = self.UserName
        oc_config.pw = self.Password
        oc_config.dsn = self.DNS

    def get_syn_sql_frame(self, Schema_Like, SynName, DSCount):
        self.Schema_Like = Schema_Like
        self.SynName = SynName
        self.DSCount = DSCount

        sql = "select OWNER, SYNONYM_NAME  from all_synonyms "
        sql_clause = None

        if self.Schema_Like is not None:
            if sql_clause is not None:
                sql_clause = sql_clause + "and owner like '%" + self.Schema_Like + "%' "
            else:
                sql_clause = sql + "where owner like '%" + self.Schema_Like + "%' "

        if self.SynName is not None:
            if sql_clause is not None:
                sql_clause = sql_clause + "and synonym_name like '%" + self.SynName + "%' "
            else:
                sql_clause = sql + "where synonym_name like '%" + self.SynName + "%' "

        if self.DSCount is not None:
            if sql_clause is not None:
                sql_clause = sql_clause + "and ROWNUM <= " + str(self.DSCount)
            else:
                sql_clause = sql + "where ROWNUM <= " + str(self.DSCount)
        return sql_clause

    def OC_fetch_synonym(self):
        try:
            con = cx_Oracle.connect(oc_config.user, oc_config.pw, oc_config.dsn, encoding="UTF-8")
            sql = self.get_syn_sql_frame(self.Schema_Like, self.SynName, self.DSCount)
            cur = con.cursor()
            cur.execute(sql)
            print(cur.arraysize)
            Schema_list = []
            Synonym_list = []
            for OWNER, SYNONYM_NAME in cur:
                Schema_list.append(OWNER)
                Synonym_list.append(SYNONYM_NAME)

            if len(Schema_list) >= 1:
                data = {"Schema": Schema_list, "Synonym": Synonym_list}
                self.Schema_Syn_DF = pd.DataFrame(data)

            print('Printing lits of Schema and Synonyms')  # need to be removed
            print(Schema_list)  # need to be removed
            print(Synonym_list)  # need to be removed

        except Exception as e:
            print("There was an issue connecting to Oracle: ", e)

    def OC_fetch_ba(self):
        try:
            con = cx_Oracle.connect(oc_config.user, oc_config.pw, oc_config.dsn, encoding="UTF-8")
            sql = self.get_syn_sql_frame(self.Schema_Like, '', self.DSCount)
            cur = con.cursor()
            cur.execute(sql)
            print(cur.arraysize)
            Schema_list = []
            Synonym_list = []
            for OWNER, SYNONYM_NAME in cur:
                Schema_list.append(OWNER)
                Synonym_list.append(SYNONYM_NAME)

            if len(Schema_list) >= 1:
                data = {"Schema": Schema_list, "Synonym": Synonym_list}
                self.Schema_Syn_DF = pd.DataFrame(data)

            print('Printing lits of Schema and Synonyms')  # need to be removed
            print(Schema_list)  # need to be removed
            print(Synonym_list)  # need to be removed
            return Schema_list

        except Exception as e:
            print("There was an issue connecting to Oracle: ", e)

    def OC_fetch_synonym_data(self, SchemaName, SynonymName, foldername, file):
        try:
            con = cx_Oracle.connect(oc_config.user, oc_config.pw, oc_config.dsn, encoding="UTF-8")
            cur = con.cursor()
            # initiate schema
            cur.callproc("PKG_BUSINESS_AREA.SP_Initialize_BA", (SchemaName, None, 'NA/Dummy'))

            # data Extract
            sql = "select * from " + SchemaName + "." + SynonymName
            cur.execute(sql)
            col_names = []
            for i in range(0, len(cur.description)):
                col_names.append(cur.description[i][0])
            # print(cur.description[0])
            try:
                df = pd.DataFrame(cur)
                df.columns = col_names
                df.reset_index(drop=True, level=0, inplace=True)
            except:
                row, col = df.shape
                if row == 0:
                    df = pd.DataFrame(columns=col_names)
                    # print('xxxxxx')# need to be removed
            # print(df.shape) # need to be removed
            # df.append(cur)
            # print(df.to_dict('records'))
            df.fillna("-", inplace=True)
            r, c = df.shape
            if r == 0:
                pass
            else:
                # foldername = "Training_Files"
                # file = "Training_Dataset"
                Training_path = "Results/" + foldername + "/" + file + ".xlsx"
                df1 = pd.read_excel(Training_path)
                # df1 = pd.read_excel("Results/Training_Files/Training_Dataset.xlsx")
                frames = [df, df1]
                df = pd.concat(frames)
                df = df.fillna('')
                df.to_excel(Training_path, index=False)
                # self.MongoDB_connect(df.to_dict('records'), SchemaName + "." + SynonymName)


        except Exception as e:
            print("There was an issue connecting to Oracle: ", e)

    def oracle_fetch_data(self, SchemaName, TableName):
        try:
            con = cx_Oracle.connect(oc_config.user, oc_config.pw, oc_config.dsn, encoding="UTF-8")
            cur = con.cursor()
            # data Extract
            sql = "select * from " + SchemaName + "." + TableName + " where ROWNUM <= 2"
            print(sql)
            print("Executing Query, this may take a while")
            cur.execute(sql)
            col_names = []
            for i in range(0, len(cur.description)):
                col_names.append(cur.description[i][0])
            df = pd.DataFrame(cur)
            df.columns = col_names
            df.reset_index(drop=True, level=0, inplace=True)
            # print('yyyyyy')# need to be removed
            # print(df.shape) # need to be removed
            # df.append(cur)
            # print(df.to_dict('records'))
            df1 = pd.read_excel("Results/Training_Files/Training_Dataset.xlsx")
            frames = [df, df1]
            df = pd.concat(frames)
            df = df.fillna('')
            df.to_excel("Results/Training_Files/Training_Dataset.xlsx", index=False)
            # self.MongoDB_connect(df.to_dict('records'), SchemaName + "." + TableName)

        except Exception as e:
            print("There was an issue connecting to Oracle: ", e)

class some:
    def __init__(self, DB_Type, DNS, User, pwd, domain) -> None:
        self.DB_Type = DB_Type
        self.DNS = DNS
        self.UserName = User
        self.Password = pwd
        self.domain = domain
        self.Schema_Like = None
        self.SynName = None
        self.DSCount = 4
        self.Schema_Syn_DF = pd.DataFrame()
        self.ConnectMethod = None
        if DB_Type == 'OC':
            self.ConnectMethod = 'OC_Connect'
        elif DB_Type == 'Oracle_Connect':
            self.ConnectMethod = 'Oracle_Connect'

    def test_func(self):
        if (self.UserName is not None) and (self.Password is not None) and (schemaLike is not None) and (tableLike is not None):
            etlObj = ETL(DB_type,DB_DNS,self.UserName,self.Password )
            if etlObj.DB_Type == 'OC':
                etlObj.get_syn_sql_frame(schemaLike,tableLike,self.DSCount)
                etlObj.OC_fetch_synonym()
                if etlObj.Schema_Syn_DF is not None:
                    for index, row in etlObj.Schema_Syn_DF.iterrows():
                        etlObj.OC_fetch_synonym_data(row['Schema'],row['Synonym'], 'Training_Files', 'Training_Dataset')
            elif etlObj.DB_Type == 'Oracle':
                etlObj.oracle_fetch_data(schemaLike,tableLike)
        else:
            print('Check the input fields is not missing')

    def get_ba(self):
        if (self.UserName is not None) and (self.Password is not None) and (study is not None):
            etlObj = ETL(DB_type,DB_DNS,self.UserName,self.Password )
            # study = study + '_SRDM'
            DSCount_live = None
            if etlObj.DB_Type == 'OC':
                etlObj.get_syn_sql_frame(study,'',DSCount_live)
                ba_list = etlObj.OC_fetch_ba()
                return ba_list

            elif etlObj.DB_Type == 'Oracle':
                etlObj.oracle_fetch_data(study,'')
        else:
            print('Check the input fields is not missing')

    def test_func_live(self):
        if (DB_type is not None) and (DB_DNS is not None) and (self.UserName is not None) and (self.Password is not None):
            etlObj = ETL(DB_type, DB_DNS, self.UserName, self.Password)
            if etlObj.DB_Type == 'OC':
                for i in pred_domains:
                    df1 = pd.DataFrame()
                    path = "Results/LiveData_Files/" + i + ".xlsx"
                    df1.to_excel(path, index=False)
                    etlObj.get_syn_sql_frame(schemaLike, i, numData)
                    etlObj.OC_fetch_synonym()
                    if etlObj.Schema_Syn_DF is not None:
                        for index, row in etlObj.Schema_Syn_DF.iterrows():
                            etlObj.OC_fetch_synonym_data(row['Schema'], row['Synonym'], 'LiveData_Files', i)
            elif etlObj.DB_Type == 'Oracle':
                etlObj.oracle_fetch_data(schemaLike, domain_name)
        else:
            print("You are missing input please execute them again ")


if __name__ == "__main__":
    DB_type = 'OC'
    DB_DNS = 'prddw'
    app.run(debug=True)


