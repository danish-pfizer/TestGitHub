import cx_Oracle
import OC_db_config as oc_config
import pandas as pd
from  pymongo import MongoClient
import pymongo

import pymongo
import numpy as np
import pandas as pd
import json
import spacy
from collections import defaultdict
import re
from dateutil.parser import parse
import Attribute_Values as Av
c1 = Av.NERModelLiveData()


class ETL:
    def __init__(self, DB_Type,DNS, User, pwd) -> None:
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

    def MongoDB_connect(self,dataframe, table_name):
        try:
            CONNECTION_STRING = "mongodb://localhost:27017/"
            client = MongoClient(CONNECTION_STRING)
            dbname = client['IDAT_DB']
            collection_name = dbname["SDTM_LIVE_DATA"]
            collection_name.insert_many([{"Table_Name": table_name,"DATA":dataframe}])
        except Exception as e:
            print("There was an issue connecting to MongoDB: ", e)

    def get_syn_sql_frame(self,Schema_Like, SynName, DSCount=10):
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
            con = cx_Oracle.connect(oc_config.user, oc_config.pw, oc_config.dsn)
            sql= self.get_syn_sql_frame(self.Schema_Like, self.SynName, self.DSCount)
            cur = con.cursor()
            cur.execute(sql)
            print(cur.arraysize)
            Schema_list=[]
            Synonym_list=[]
            for OWNER, SYNONYM_NAME in cur:
                Schema_list.append(OWNER)
                Synonym_list.append(SYNONYM_NAME)

            if len(Schema_list) >= 1:
                data = {"Schema":Schema_list,"Synonym":Synonym_list}
                self.Schema_Syn_DF = pd.DataFrame(data)
                
            print('Printing lits of Schema and Synonyms') # need to be removed
            print(Schema_list) # need to be removed
            print(Synonym_list) # need to be removed

        except Exception as e:
            print("There was an issue connecting to Oracle: ", e)

    def OC_fetch_synonym_data(self,SchemaName, SynonymName):
        try:
            con = cx_Oracle.connect(oc_config.user, oc_config.pw, oc_config.dsn)
            cur = con.cursor()
            #initiate schema
            cur.callproc("PKG_BUSINESS_AREA.SP_Initialize_BA",(SchemaName,None,'NA/Dummy'))

            #data Extract
            sql = "select * from "+ SchemaName + "." + SynonymName
            cur.execute(sql)
            col_names = []
            for i in range(0, len(cur.description)):
                col_names.append(cur.description[i][0])
            df = pd.DataFrame(cur)
            df.columns = col_names
            df.reset_index(drop=True,level=0, inplace=True)
            #print('xxxxxx')# need to be removed
            #print(df.shape) # need to be removed
            #df.append(cur)
            #print(df.to_dict('records'))
            self.MongoDB_connect(df.to_dict('records'),SchemaName+ "."+SynonymName)
            
        except Exception as e:
            print("There was an issue connecting to Oracle: ", e)

    def oracle_fetch_data(self, SchemaName,TableName):
        try:
            con = cx_Oracle.connect(oc_config.user, oc_config.pw, oc_config.dsn)
            cur = con.cursor()
            #data Extract
            sql = "select * from "+ SchemaName + "." + TableName +" where ROWNUM <= 2"
            print(sql)
            print("Executing Query, this may take a while")
            cur.execute(sql)
            col_names = []
            for i in range(0, len(cur.description)):
                col_names.append(cur.description[i][0])
            df = pd.DataFrame(cur)
            df.columns = col_names
            df.reset_index(drop=True,level=0, inplace=True)
            #print('yyyyyy')# need to be removed
            #print(df.shape) # need to be removed
            #df.append(cur)
            #print(df.to_dict('records'))
            self.MongoDB_connect(df.to_dict('records'),SchemaName+ "."+TableName)
            
        except Exception as e:
            print("There was an issue connecting to Oracle: ", e)

class NERModel_LiveData:
    nlp = spacy.load('en_core_web_sm', disable=['tagger', 'parser', 'lemmatizer'])
    client = pymongo.MongoClient(c1.mongodb_connection)
    mydb = client[c1.db_name]
    mycollection = mydb[c1.collection]
    mycollection_output = mydb[c1.collection_output]
    #schemaname = input("Enter Schema name: ")
    #table_name = input("Enter Table name: ")
    #domain_name = table_name.split('_')[-1][:2]
    #principles_file_name = c1.file_names[0] + '_' + domain_name
    #map_file_name = c1.file_names[1] + '_' + domain_name
    #mycollection_inp = mydb[domain_name]
    #tablename = Schema_like + "." + table_name
    counter1 = 0

    def __init__(self,Schema_like,table_like):
        self.schemaname = Schema_like
        self.table_name = table_like
        self.tablename = Schema_like+'.'+table_like
        self.domain_name = table_like.split('_')[-1][:2]
        self.principles_file_name = c1.file_names[0] + '_' + self.domain_name
        self.map_file_name = c1.file_names[1] + '_' + self.domain_name
        self.mycollection_inp = self.mydb[self.domain_name]
        self.client = pymongo.MongoClient(c1.mongodb_connection)
        self.mydb = self.client[c1.db_name]
        self.mycollection = self.mydb[c1.collection]
        self.mycollection_output = self.mydb[c1.collection_output]

    def getdata(self):
        all_records = self.mycollection.find()
        df = pd.DataFrame(all_records)
        df1 = pd.DataFrame()
        for i in range(len(df)):
            if df[c1.mongodb_cols[0]][i] == self.tablename:
                df1 = pd.DataFrame(df[c1.mongodb_cols[1]][i])
        df1 = df1.fillna('')
        return df1

    @staticmethod
    def predicted_output(study):
        nlp_ner = spacy.load(c1.spacy_model_path)
        data = []
        for i in range(len(study)):
            to_list = list(study.values[i].astype('str'))
            if to_list != "":
                data.append(' '.join(to_list))
            data[i] = re.sub(' +', ' ', data[i])
        doc = []
        for i in data:
            doc.append(nlp_ner(i))
        df = pd.DataFrame()
        df_supp = pd.DataFrame()
        for i in range(len(doc)):
            l1 = []
            res = defaultdict(list)
            for ent in doc[i].ents:
                l1.append(tuple((ent.label_, ent.text)))
            for k, j in l1:
                res[k].append(j)
            dict1 = dict(res)
            if c1.multicol in dict1:
                if len(dict1[c1.multicol]) > 1:
                    for cnt in range(1, len(dict1[c1.multicol])):
                        for k1, j1 in l1:
                            if k1 != c1.multicol:
                                dict1[k1].append(j1)
            df1 = pd.DataFrame.from_dict(dict1, orient='index')
            df1 = df1.transpose()
            df2 = pd.DataFrame.from_dict(dict1, orient='index')
            df2 = df2.transpose()
            if c1.multicol in dict1:
                if len(dict1[c1.multicol]) > 1:
                    df2 = df2[0:df1.shape[0]]
                    df1 = df1[0:1]
                    df1[c1.multicol] = c1.multicol_val
                else:
                    df2 = df2[1:df1.shape[0]]
            else:
                df2 = df2[1:df1.shape[0]]
            df = pd.concat([df, df1], ignore_index=True, sort=False)
            df_supp = pd.concat([df_supp, df2], ignore_index=True, sort=False)
        return df, df_supp

    def validate_datatype(self, data, meta, key):
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
            self.counter1 = count

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
                                print(c1.principles_keys[0] + ' :' + index, 'WARNING: Type mismatch in variable', i, 'Source type: ', c1.value_date,
                                      'Target type:', type(val))

    def validate_duplicated(self, data, meta, key):
        for i in data.columns:
            for j in meta.keys():
                if i == j:
                    if meta[i][key] == c1.value_unique:
                        if len(data) != len(data[i].value_counts()):
                            s = data[i].value_counts()
                            s = s[s > 1]
                            for d_ind in s.index:
                                print(c1.principles_keys[3] + ' : WARNING: ' + i + ' Found', s.loc[d_ind], 'observations for', d_ind)

    def validate_nullable(self, data, meta, key):
        for i in data.columns:
            for j in meta.keys():
                if i == j:
                    if meta[i][key] == c1.value_no:
                        for ind, val in data[i].iteritems():
                            if pd.notnull(val) == False:
                                print(c1.principles_keys[1] + ' : WARNING:', i, 'should not contain any null value. But record', ind, 'is null')

    def validate_len_range(self, data, meta, key):
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
                            print(c1.principles_keys[2] + ' :' + i, count, 'WARNING: Length is exceeding from the source')
                        count += 1


    def ner_output(self, map_dict):
        df1 = self.getdata()
        #map_dict = self.read_map_details()
        df2 = df1[map_dict.keys()]
        ner_pred_df, supp_df = self.predicted_output(df2)
        return ner_pred_df

    def evaluate_principles(self):
        all_records = self.mycollection_inp.find()
        df = pd.DataFrame(all_records)
        for i in range(len(df)):
            if df[c1.mongodb_cols[2]][i] == self.map_file_name:
                map_dict = df[c1.mongodb_cols[1]][i]
            elif df[c1.mongodb_cols[2]][i] == self.principles_file_name:
                eval_dict = df[c1.mongodb_cols[1]][i]
        ner_pred_df = self.ner_output(map_dict)

        # Updating Data to Get Warnings
        ner_pred_df['SUBJID'][12] = np.nan
        ner_pred_df['SUBJID'][11] = ner_pred_df['SUBJID'][10]
        ner_pred_df['AGEU'][12] = 'months'
        #ner_pred_df['AGE'][12] = 'Yr'

        dict1 = {}
        for i in list(eval_dict.keys()):
            if i != '_id':
                dict1[i] = eval_dict[i]
        eval_dict = dict1
        self.validate_datatype(ner_pred_df, eval_dict, c1.principles_keys[0])
        self.validate_duplicated(ner_pred_df, eval_dict, c1.principles_keys[3])
        self.validate_nullable(ner_pred_df, eval_dict, c1.principles_keys[1])
        self.validate_len_range(ner_pred_df, eval_dict, c1.principles_keys[2])
        print(ner_pred_df)
        dict1 = ner_pred_df.to_dict(orient='records')
        dict2 = {}
        dict2[c1.mongodb_cols[0]] = self.tablename
        dict2[c1.mongodb_cols[1]] = dict1
        self.mycollection_output.delete_one({c1.mongodb_cols[0]: self.tablename})
        self.mycollection_output.insert_one(dict2)

if __name__ == "__main__":

    #DB_type = input("Enter the Type of Database you want to connect ( OC for Oracle Clinic ): ")
    #DB_DNS = input("Enter %s DNS:" % DB_type)
    #DB_User = input("Enter %s User Name:" % DB_type)
    #DB_pwd = input("Enter %s Password:" % DB_User)

    #delete on prod
    DB_type ='OC'
    DB_DNS = 'prddw'
    DB_User='dr06'
    DB_pwd='Asdf1234'

    #DB_type ='Oracle'
    #DB_DNS = 'podsprod.pfizer.com'
    #DB_User='pods_cdars'
    #DB_pwd='Pfizer#8143'
    #table PODSDAL.PODS_ODS_CONTACT_INFO_V

    if (DB_type is not None) and (DB_DNS is not None) and (DB_User is not None) and (DB_pwd is not None):
        Schema_like = input("Enter the schema (Like ie BA_%):")
        Table_Like = input("Enter Table (Like ie SM_DM%):")
        Table_Count = 1
        #Table_Count = input("Enter number of table to extract:" )
        #delete on prod
        #Schema_like ='BA_C'
        #Table_Like = 'SM_DM'
        #Table_Count = 1
        
        etlObj = ETL(DB_type,DB_DNS,DB_User,DB_pwd )
        if etlObj.DB_Type == 'OC':
            etlObj.get_syn_sql_frame(Schema_like,Table_Like,Table_Count)
            etlObj.OC_fetch_synonym()
            if etlObj.Schema_Syn_DF is not None:
                for index, row in etlObj.Schema_Syn_DF.iterrows(): 
                    etlObj.OC_fetch_synonym_data(row['Schema'],row['Synonym'])
        elif etlObj.DB_Type == 'Oracle':
            etlObj.oracle_fetch_data(Schema_like,Table_Like)
            
    else:
        print("You are missing input please execute them again ")




    test1 = NERModel_LiveData(Schema_like,Table_Like)
    test1.evaluate_principles()
