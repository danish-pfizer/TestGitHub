import pymongo
import pandas as pd
import json
import spacy
import ast
import re
from spacy.tokens import DocBin
from tqdm import tqdm
import os
from collections import Counter
from dateutil.parser import parse
import Attribute_Values as Av
c1 = Av.NERModelTraining()

import cx_Oracle
import OC_db_config as oc_config
import pandas as pd
from  pymongo import MongoClient
import pymongo

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
            collection_name = dbname["SDTM_Auto"]
            collection_name.insert_many([{"Table_Name": table_name,"DATA":dataframe}])
            print('XxXxX')
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


class NERModel_Training:
    nlp = spacy.load('en_core_web_sm', disable=['tagger', 'parser', 'lemmatizer'])
    client = pymongo.MongoClient(c1.mongodb_connection)
    mydb = client[c1.db_name]
    mycollection = mydb[c1.collection]
    tablename = input("Enter Table name for NER Model Training: ")
    domain_name = tablename.split('_')[-1][:2]
    collection_output = mydb[domain_name]
    updated_map_file = ''

    def getdata(self):
        all_records = self.mycollection.find()
        df = pd.DataFrame(all_records)
        df1 = pd.DataFrame()
        for i in range(len(df)):
            if self.tablename in df[c1.mongodb_cols[0]][i]:
                df2 = pd.DataFrame(df[c1.mongodb_cols[1]][i])
                frames = [df2, df1]
                df1 = pd.concat(frames)
        df1 = df1.fillna('')
        return df1

    @staticmethod
    def generate_map_dict(df1):
        tds_dict = {}
        for i in df1.columns:
            tds_dict[i] = ""
        tds_dict = json.dumps(tds_dict, indent=0)
        with open(c1.map_file_location, 'w') as fp:
            fp.write(str(tds_dict))
        print("Map File is stored at location: " + c1.map_file_location)


    def read_map_details(self):
        if self.updated_map_file == '':
            updated_map_file = input("Enter Updated Map File Location: ")
            self.updated_map_file = updated_map_file
        # Reading Mapping Details
        with open(self.updated_map_file) as f:
            d1 = f.read()
        map_dict = ast.literal_eval(d1)
        map_dict1 = {}
        for key in map_dict.keys():
            if map_dict[key] != '':
                map_dict1[key] = map_dict[key]
        map_dict = map_dict1
        return map_dict

    # Convert RDS rows to string
    @staticmethod
    def convert_tostring(rds):
        data = []
        for i in range(len(rds)):
            to_list = list(rds.values[i].astype('str'))
            if to_list != "":
                data.append(' '.join(to_list))
            data[i] = re.sub(' +', ' ', data[i])
        return data

    # Generating Final Annotations
    @staticmethod
    def generate_annotations(rds, mapdict, data):
        finalannotation = "["
        for i in range(len(rds)):
            n = 0
            ctr = 0
            aa = '("' + data[i] + '",{"entities":['
            for j in rds.columns:
                col_len = len(str(rds[j].iloc[i]))
                ctr = ctr + 1
                if len(str(rds[j].iloc[i])) > 0:
                    aa = aa + '(' + str(n) + ',' + str(n + col_len) + ',"' + mapdict[j] + '")'
                    n = n + col_len + 1
                    if ctr == len(rds.columns):
                        aa = aa + ']}),'
                    else:
                        aa = aa + ','
            finalannotation = finalannotation + aa
        finalannotation = finalannotation + "]"
        return finalannotation

    # the DocBin will store the example documents
    def create_training(self, data):
        db = DocBin()
        for text, annot in tqdm(data):  # data in previous format
            doc = self.nlp.make_doc(text)  # create doc object from text
            ents = []
            for start, end, label in annot["entities"]:  # add character indexes
                span = doc.char_span(start, end, label=label, alignment_mode="contract")
                if span is None:
                    print("Skipping entity")
                else:
                    ents.append(span)
            doc.ents = ents  # label the text with the ents
            db.add(doc)
        return db

    def ner_training(self):
        df1 = self.getdata()
        self.generate_map_dict(df1)
        map_dict = self.read_map_details()
        df2 = df1[map_dict.keys()]
        data = self.convert_tostring(df2)
        final_annotation = self.generate_annotations(df2, map_dict, data)
        training_data = ast.literal_eval(final_annotation)
        train_data = self.create_training(training_data)
        train_data.to_disk("./train.spacy")
        os.system(c1.cmd1)
        cmd2 = c1.cmd2_part1 + self.domain_name + c1.cmd2_part2
        os.system(cmd2)
        # print("NER Model is stored at location: " + c1.model_output_loc)


    @staticmethod
    def get_datatype(rds, principles_dict, map_dict):
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
                                parse(value, fuzzy=False)
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

    @staticmethod
    def get_nullable(rds, principles_dict, map_dict):
        for i in principles_dict.keys():
            for j in rds.columns:
                if map_dict[j] == i:
                    if rds[j].isna().sum() == 0 and len(rds[rds[j] == '']) == 0:
                        principles_dict[i][c1.principles_keys[1]] = c1.value_no
                    else:
                        principles_dict[i][c1.principles_keys[1]] = c1.value_yes
        return principles_dict

    @staticmethod
    def get_len_range(rds, principles_dict, map_dict):
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

    @staticmethod
    def get_uniqueness(rds, principles_dict, map_dict):
        for i in principles_dict.keys():
            for j in rds.columns:
                if map_dict[j] == i:
                    if len(rds[j].value_counts()) == rds.shape[0]:
                        principles_dict[i][c1.principles_keys[3]] = c1.value_unique
                    else:
                        principles_dict[i][c1.principles_keys[3]] = c1.value_duplicated
        return principles_dict

    def generate_metadata(self):
        df1 = self.getdata()
        map_dict = self.read_map_details()
        df2 = df1[map_dict.keys()].reset_index(drop=True)
        #domain_name = ''
        #for i in list(map_dict.keys()):
        #    if map_dict[i] == c1.domain_col_name:
        #        domain_col = i
        #        domain_name = df2[domain_col].iloc[0][:2]
        principles_dict = {}
        for i in list(set(map_dict.values())):
            principles_dict[i] = {}
        principles_dict = self.get_datatype(df2, principles_dict, map_dict)
        principles_dict = self.get_nullable(df2, principles_dict, map_dict)
        principles_dict = self.get_len_range(df2, principles_dict, map_dict)
        principles_dict = self.get_uniqueness(df2, principles_dict, map_dict)
        principle_file_loc = c1.principle_file_location + '' + c1.file_names[0] + '_' + self.domain_name + '.json'
        with open(principle_file_loc, 'w') as fp:
            fp.write(str(principles_dict))
        with open(principle_file_loc) as f:
            d1 = f.read()
        eval_dict = ast.literal_eval(d1)
        dict1 = {}
        dict1[c1.mongodb_cols[2]] = c1.file_names[0] + '_' + self.domain_name
        dict1[c1.mongodb_cols[1]] = eval_dict
        #collection_output = self.mydb[self.domain_name]
        self.collection_output.delete_one({c1.mongodb_cols[2] : c1.file_names[0] + '_' + self.domain_name})
        self.collection_output.insert_one(dict1)
        dict1 = {}
        dict1[c1.mongodb_cols[2]] = c1.file_names[1] + '_' + self.domain_name
        dict1[c1.mongodb_cols[1]] = map_dict
        self.collection_output.delete_one({c1.mongodb_cols[2]: c1.file_names[1] + '_' + self.domain_name})
        self.collection_output.insert_one(dict1)
        print("Principle Metadata File is stored at location: " + principle_file_loc)
        print("Principle Metadata File and Map File are stored in MongoDB collection: " + self.domain_name)


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
        Table_Count = input("Enter number of table to extract:" )
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

    test1 = NERModel_Training()
    test1.ner_training()
    test1.generate_metadata()
