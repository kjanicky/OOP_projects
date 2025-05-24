from distutils.command.clean import clean

import requests
import csv
import json
import pandas as pd

class Extract():
    def __init__(self,url,resource):
        self.url = url
        self.resource = resource

    def fetch_data(self):
        final_url = self.url + f"/{self.resource}"
        response = requests.get(final_url)
        return response.json()


class Transform():
    def __init__(self,data):
        self.data = data

    def filter_by(self,field,value):
        if isinstance(value, list):
            return [item for item in self.data if item.get(field) in value]
        else:
            return [item for item in self.data if item.get(field) == value]

    def select_fields(self,fields):
        if isinstance(fields, list):
            return [{key: item[key] for key in fields if key in item} for item in self.data]
        else:
            return [{fields: item[fields]} for item in self.data if fields in item] #for every item that is in list fields there is created new dict with this key and the value corresponding

    def drop_nulls(self):
        return [item for item in self.data if None not in item.values()] ## if item != None always return true bad way to code it

    def rename_fields(self,old_field_name,new_field_name):
        for item in self.data:
            if old_field_name in item:
                item[new_field_name] = item.pop(old_field_name)
        return self.data

class Load():
    def __init__(self,data):
        self.data = data

    def save_to_file(self,filepath,filetype):
        if filetype == 'csv':
            csv_headers = list(self.data[0].keys())
            with open(filepath,'w',newline = '',encoding = 'utf-8') as csv_file:
                writer = csv.DictWriter(csv_file,fieldnames=csv_headers)
                writer.writeheader()
                writer.writerows(self.data)
        elif filetype == 'json':
            with open(filepath,'w',encoding='utf-8') as file:
                content = json.dumps(self.data)
                file.write(content)
        elif filetype == 'parquet':
            df = pd.DataFrame(self.data)
            df.to_parquet(filepath,'fastparquet')

class Pipeline():
    def __init__(self,url,resource,filepath=None,filetype=None,filter_value=None,filter_field=None,select_fields=None,old_names=None,new_names=None) :
        self.url = url
        self.resource = resource
        self.filepath = filepath
        self.filetype = filetype
        self.filter_value = filter_value
        self.filter_field = filter_field
        self.select_fields = select_fields
        self.old_names = old_names
        self.new_names = new_names

    def Run(self):
        data = Extract(self.url, self.resource).fetch_data()

        # Drop nulls
        clean_data = Transform(data).drop_nulls()

        if self.filter_value is not None and self.filter_field is not None:
            if any(self.filter_field in item for item in clean_data):
                clean_data = Transform(clean_data).filter_by(self.filter_field, self.filter_value)
            else:
                print(f"Field '{self.filter_field}' does not exist. Filtering was not done.")

        if self.select_fields is not None:
            existing_fields = set().union(*(item.keys() for item in clean_data))
            valid_fields = [f for f in self.select_fields if f in existing_fields]
            missing_fields = [f for f in self.select_fields if f not in existing_fields]

            if missing_fields:
                print(f"Some fields are missing: {missing_fields}")
            if valid_fields:
                clean_data = Transform(clean_data).select_fields(valid_fields)

        if self.old_names is not None and self.new_names is not None:
            if not isinstance(self.old_names, list):
                self.old_names = [self.old_names]
            if not isinstance(self.new_names, list):
                self.new_names = [self.new_names]

            if len(self.old_names) != len(self.new_names):
                print("Number of old_names and new_names is not the same please enter the same number")
            else:
                valid_pairs = [
                    (old, new) for old, new in zip(self.old_names, self.new_names)
                    if any(old in item for item in clean_data)
                ]
                missing = [old for old in self.old_names if not any(old in item for item in clean_data)]
                if missing:
                    print(f"Some fields for rename are not existing: {missing}")
                for old, new in valid_pairs:
                    clean_data = Transform(clean_data).rename_fields(old, new)


        if self.filetype is not None and self.filepath is not None:
            Load(clean_data).save_to_file(self.filepath, self.filetype)

Pipeline('https://jsonplaceholder.typicode.com','users','C:/CODING/Python/OOP Learning/OOP_projects/ETL_module/output.csv'
,'csv',1,'id',['id','username'],'id','ID').Run()
#check for methods
#data = Extract('https://jsonplaceholder.typicode.com','users').fetch_data()
#filter_data = Transform(data).filter_by('userId',[1,2,3])
#filter_data = Transform(data).rename_fields(['userId','id'],['User_ID','ID'])
#filter_data =  Transform(data).select_fields(['userId','id'])
#filter_data = Transform(data).drop_nulls()
#Load(data).save_to_file('C:\CODING\Python\OOP Learning\OOP_projects\ETL_module\output.parquet','parquet')
#print(filter_data)
