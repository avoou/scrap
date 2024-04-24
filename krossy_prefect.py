import requests
import httpx
import sqlite3
import pandas as pd
from pprint import pprint
from bs4 import BeautifulSoup, Tag
from typing import Tuple
from abc import ABC, abstractmethod
from datetime import datetime
from prefect import task, flow, get_run_logger


class Schemes:
    RAW = ["name", "price_ua", "link"]
    OUT = RAW + ["date", "price_us"]


class NoItemsError(Exception):
    pass


class EmptyDfError(Exception):
    pass


class ExtractItems(ABC):
    @abstractmethod
    def get_items(self):
        raise NotImplementedError("Not implemented")
    

    @abstractmethod
    def get_pages_count(self):
        raise NotImplementedError("Not implemented")
    

    @abstractmethod
    def get_items(self):
        raise NotImplementedError("Not implemented")


    @abstractmethod
    def get_name(self):
        raise NotImplementedError("Not implemented")
    

    @abstractmethod
    def get_price_current(self):
        raise NotImplementedError("Not implemented")


    @abstractmethod
    def get_items_link(self):
        raise NotImplementedError("Not implemented")
    

    @abstractmethod
    def get_items_by_page(self):
        raise NotImplementedError("Not implemented")

@task
def get_bs_by_url(url: str) -> BeautifulSoup:
    logger = get_run_logger()
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:78.0)   Gecko/20100101 Firefox/78.0",
        "Accept": "*/*",
        "Referer": "https://megasport.ua",
    }
    try:
        response = requests.get(url, headers=headers)
        response.encoding = 'utf-8'
        txt = response.text
    except:
        logger.error("Error with response. Check internet connection or url")
        raise NoItemsError
    return BeautifulSoup(txt, 'html.parser')


class ExtractBootsMaleItems(ExtractItems):
    def get_items(self, sp: BeautifulSoup) -> Tag:
        items = sp.find('div', class_='Fkfp3V')
        return items if items else [None]
    

    def get_pages_count(self, sp: BeautifulSoup) -> int:
        try:
            return len(sp.find(id='select-page'))
        except Exception:
            return None
        

    def get_name(self, item: Tag) -> str:
        try:
            name = item.find('div', class_='ihuxuw').text
            name = name.replace("\u2009", " ").replace("\xa0", " ")
            return str(name)
        except Exception:
            return None


    def get_price_current(self, item: Tag) -> Tuple[int, str]:
        try:
            price = item.find('span', class_='MeSmTt').text
            price, current = price.split("\u2009")
            price = int(price.replace("\xa0", ""))

            return price, current
        except Exception:
            return None, None


    def get_items_link(self, item: Tag) -> str:
        try:
            tag = item.find('a', class_='it25hX')
            return tag.get('href')
        except Exception:
            return None
        

    def get_items_by_page(self, page_url: str):
        sp = get_bs_by_url(page_url)
        items = self.get_items(sp)
        return items


@task
def get_items_by_page(url: str, ServeCls):
    items = ServeCls.get_items_by_page(url)
    return items


@task
def to_df(items: list, host: str, ExtractObj: ExtractItems) -> pd.DataFrame:
    df = pd.DataFrame(columns=Schemes.RAW)

    for item in items:
        name = ExtractObj.get_name(item=item)
        price, _ = ExtractObj.get_price_current(item=item)
        link = ExtractObj.get_items_link(item)
        res = {
            "name": name,
            "price_ua": price,
            "link": host + link if link else None,
        }

        df = pd.concat([df, pd.DataFrame([res])])

    return df


@task
def extract(host: str, path: str, ExtractObj: ExtractItems) -> list:
    logger = get_run_logger()
    logger.info('Start extracting ...')
    url = host + path
    sp = get_bs_by_url(url)
    count = ExtractObj.get_pages_count(sp)
    if not count:
        count = 1
    #count = 2
    futures = []
    for i in range(1, count):
        path = path + f"page-{i}/"
        url = host + path
        futures.append(get_items_by_page.submit(url, ExtractObj))
    
    items = []
    for future in futures:
        for item in future.result():
            if item:
                items.append(item) 

    if not len(items):
        logger.error("Cant get items. Check the implementation of get_items method")
        raise NoItemsError

    df = to_df(items=items, host=host, ExtractObj=ExtractObj)

    logger.info(f'Count of extracting items is: {len(df)}')
    return df


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


@task
def drop_none(df: pd.DataFrame):
    logger = get_run_logger()
    logger.info(f'Count of missing items is: {df.isnull().any(axis=1).sum()}')
    res = df.dropna()
    
    if not len(res):
        logger.error('Empty extract df. Check internet connection or urls')
        raise EmptyDfError
    
    return res


@task
def add_another_current(df: pd.DataFrame):
    course = 35
    df.loc[:, 'price_us'] = df.loc[:, 'price_ua'].apply(lambda x: round(x / course, 2))


@task
def add_data_time(df: pd.DataFrame):
    df.loc[:, 'date'] = datetime.now()


@task
def transform(extract_df: pd.DataFrame):
    df = pd.DataFrame(columns=Schemes.OUT)
    extract_df = extract_df.copy()
    extract_df = drop_none(extract_df)
    add_another_current(extract_df)
    add_data_time(extract_df)
    return pd.concat([df, extract_df])
    #return df


@task
def load_to_db(df_to_load: pd.DataFrame, db_path: str, db_table: str):
    logger = get_run_logger()
    db = ClientDB(db=db_path)
    try:
        db.write_df_to_db(df=df_to_load, table=db_table)
        return len(df_to_load)
    except Exception:
        logger.error("Trouble to connect to db")
        return None


@flow
def etl_flow(host: str, path:str, db_path, db_table):
    logger = get_run_logger()
    try:
        ExtractObj = ExtractBootsMaleItems()
        extract_df = extract(host=host, path=path, ExtractObj=ExtractObj)
        transform_df = transform(extract_df=extract_df)
        count = load_to_db(df_to_load=transform_df, db_path=db_path, db_table=db_table)
        if count:
            logger.info(f'It has been written {count} items to {db_path} in {db_table} table')
    except Exception as e:
        logger.error(e, "Something went wrong")


if __name__ == '__main__':
        etl_flow(
            host='https://megasport.ua', 
            path='/ua/catalog/krossovki-i-snikersi/male/', 
            db_path='krossy.db', 
            db_table='krossy_table'
        )