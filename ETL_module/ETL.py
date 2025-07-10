import requests
import csv
import json
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

class Extract:
    def __init__(self,url,resource):
        self.url = url
        self.resource = resource

    def fetch_data(self):
        response = requests.get(f"{self.url}/{self.resource}")
        response.raise_for_status()
        return response.json()


class Transform:
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

class Load:
    def save_to_file(self, data, filepath, filetype):
        if filetype == 'csv':
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
                writer.writeheader()
                writer.writerows(data)
        elif filetype == 'json':
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f)
        elif filetype == 'parquet':
            df = pd.DataFrame(data)
            df.to_parquet(filepath, engine='fastparquet')

class DBLoader:
    def __init__(self, dbname='Online_Posts', user='postgres', password = None, host='localhost', port=5432):
        self.conn = psycopg2.connect(dbname=dbname, user=user, password = password, host=host, port=port)
        self.cur = self.conn.cursor()

    def insert_row(self, table_name, item):
        keys = ', '.join(f'"{k}"' for k in item.keys())
        placeholders = ', '.join(['%s'] * len(item))
        values = list(item.values())
        sql = f'INSERT INTO "{table_name}" ({keys}) VALUES ({placeholders});'
        self.cur.execute(sql, values)

    def insert_all(self, table_name, data):
        for item in data:
            self.insert_row(table_name, item)
        self.conn.commit()
        self.cur.close()
        self.conn.close()
        print(f" {len(data)} records inserted into '{table_name}'")

class Pipeline:
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

    def Run(self,save_to_file=True, save_to_db=False, db_table=None, db_params=None):

        data = Extract(self.url, self.resource).fetch_data()
        transformer = Transform(data)

        if self.filter_value is not None and self.filter_field is not None:
            if any(self.filter_field in item for item in data):
                data = Transform(data).filter_by(self.filter_field, self.filter_value)
            else:
                print(f"Field '{self.filter_field}' does not exist. Filtering was not done.")

        if self.select_fields is not None:
            existing_fields = set().union(*(item.keys() for item in data))
            valid_fields = [f for f in self.select_fields if f in existing_fields]
            missing_fields = [f for f in self.select_fields if f not in existing_fields]

            if missing_fields:
                print(f"Some fields are missing: {missing_fields}")
            if valid_fields:
                data = Transform(data).select_fields(valid_fields)

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
                    if any(old in item for item in data)
                ]
                missing = [old for old in self.old_names if not any(old in item for item in data)]
                if missing:
                    print(f"Some fields for rename are not existing: {missing}")
                for old, new in valid_pairs:
                    data = Transform(data).rename_fields(old, new)

        # Save to file if requested
        if save_to_file and self.filetype is not None and self.filepath is not None:
            Load().save_to_file(data, self.filepath, self.filetype)

            # Save to DB if requested
        if save_to_db and db_table is not None:
            db_loader = DBLoader(**(db_params or {}))# db_params is expected to be dict with keys like dbname, user, host, port
            db_loader.insert_all(db_table, data)

# run to save in a file
Pipeline(
    url='https://jsonplaceholder.typicode.com',
    resource='posts',
    filepath='output.csv',
    filetype='csv',
    filter_field='userId',
    filter_value=1,
    select_fields=['id', 'title', 'body'],
    old_names='id',
    new_names='post_id'
).Run()
# run to send records to DB in postgreAdmin
load_dotenv()
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
}
Pipeline(
    url='https://jsonplaceholder.typicode.com',
    resource='posts',
    filter_field=None,
    filter_value=None,
    select_fields=None,
    old_names='userId',
    new_names='userid'
).Run(
    save_to_file=False,
    save_to_db=True,
    db_table='posts',
    db_params=db_params
)

