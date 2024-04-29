# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from datetime import datetime
import pandas as pd
import logging
import sqlite3
import os


# logger = logging.getLogger()
# logging.basicConfig(level=logging.INFO)


class Schemes:
    RAW = ["name", "price_ua", "link"]
    OUT = RAW + ["date", "price_us"]


class ClientDB:
    def __init__(self, db: str) -> None:
        self.db = db
        self.con = sqlite3.connect(self.db)
        self.cursor = self.con.cursor()
    
    def write_df_to_db(self, df: pd.DataFrame, table: str):
        df.to_sql(name=table, con=self.con, if_exists = 'append', chunksize = 1000)


    def request(self, req: str):
        response = self.cursor.execute(req)
        return response.fetchall()
    

class BootsPipeline:
    def process_item(self, item, spider):
        print('we are in 1 pipilne')
        adapter = ItemAdapter(item)

        if adapter['price_ua']:
            course = 35
            adapter['price_us'] = round(adapter['price_ua'] / 2, 2)

        adapter['date'] = datetime.now()
        
        self.df = pd.concat([self.df, pd.DataFrame([adapter.asdict()])])
        return item


    def open_spider(self, spider):
        self.df = pd.DataFrame(columns=Schemes.OUT)
 

    def close_spider(self, spider):
        print('Number of rows:', len(self.df))
        print(f'Number of rows with None: {self.df.isnull().any(axis=1).sum()}')
        
        name_db = 'krossy.db'
        path_db = os.path.join(os.getcwd(), '../..', name_db)
        try:
            db = ClientDB(db=path_db)
            db.write_df_to_db(df=self.df, table='krossy_table')
            print(f'It has been written {len(self.df)} items to db')
        except Exception:
            print('Error with writing items to db!')