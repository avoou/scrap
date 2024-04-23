import requests
import httpx
import sqlite3
import logging
import pandas as pd
from pprint import pprint
from bs4 import BeautifulSoup, Tag
from typing import Tuple
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from prefect import task, flow, get_run_logger


# logger = logging.getLogger()
# logging.basicConfig(level=logging.INFO)



MAX_THREADS = 100


class Schemes:
    RAW = ["name", "price_ua", "link"]
    OUT = RAW + ["date", "price_us"]


class NoItemsError(Exception):
    pass


class EmptyDfError(Exception):
    pass


class ExtractItems(ABC):
    @abstractmethod
    def _get_pages_count(self) -> int:
        raise NotImplementedError("Not implemented")
    

    @abstractmethod
    def _get_items(self) -> str:
        raise NotImplementedError("Not implemented")


    @abstractmethod
    def _get_name(self) -> str:
        raise NotImplementedError("Not implemented")
    

    @abstractmethod
    def _get_price_current(self) -> str:
        raise NotImplementedError("Not implemented")


    @abstractmethod
    def _get_items_link(self) -> str:
        raise NotImplementedError("Not implemented")
    

class ExtractBootsMaleItems(ExtractItems):
    @staticmethod
    def get_items(sp: BeautifulSoup) -> Tag:
        items = sp.find('div', class_='Fkfp3V')
        return items if items else [None]
    

    @staticmethod
    def get_pages_count(sp: BeautifulSoup) -> int:
        try:
            return len(sp.find(id='select-page'))
        except Exception:
            return None
        

    @staticmethod
    def get_name(item: Tag) -> str:
        try:
            name = item.find('div', class_='ihuxuw').text
            name = name.replace("\u2009", " ").replace("\xa0", " ")
            return str(name)
        except Exception:
            return None


    @staticmethod
    def get_price_current(item: Tag) -> Tuple[int, str]:
        try:
            price = item.find('span', class_='MeSmTt').text
            price, current = price.split("\u2009")
            price = int(price.replace("\xa0", ""))

            return price, current
        except Exception:
            return None, None


    @staticmethod
    def get_items_link(item: Tag) -> str:
        try:
            tag = item.find('a', class_='it25hX')
            return tag.get('href')
        except Exception:
            return None


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


@task
def get_items_by_page(url: str, ServeCls):
    sp = get_bs_by_url(url)
    items = ServeCls.get_items(sp)
    return items


@task
def to_df(items: list, host: str, ServeCls) -> pd.DataFrame:
    df = pd.DataFrame(columns=Schemes.RAW)

    for item in items:
        name = ServeCls.get_name(item=item)
        price, current = ServeCls.get_price_current(item=item)
        link = ServeCls.get_items_link(item)
        res = {
            "name": name,
            "price_ua": price,
            "link": host + link if link else None,
        }

        df = pd.concat([df, pd.DataFrame([res])])

    return df


@flow
def extract_boots(host: str, path: str, ServeCls) -> list:
    logger = get_run_logger()
    logger.info('Start extracting ...')
    url = host + path
    sp = get_bs_by_url(url)
    count = ServeCls.get_pages_count(sp)
    if not count:
        count = 1
    count = 2
    futures = []
    for i in range(1, count):
        path = path + f"page-{i}/"
        url = host + path
        futures.append(get_items_by_page.submit(url, ServeCls))
    
    items = []
    for future in futures:
        for item in future.result():
            if item:
                items.append(item) 

    if not len(items):
        logger.error("Cant get items. Check the implementation of get_items method")
        raise NoItemsError

    df = to_df(items=items, host=host, ServeCls=ServeCls)
    #print(df.info())
    return df




# class ClientDB:
#     def __init__(self, db: str) -> None:
#         self.db = db
#         self.con = sqlite3.connect(self.db)
#         self.cursor = self.con.cursor()
    
#     def write_df_to_db(self, df: pd.DataFrame):
#         df.to_sql(name='krossy_table', con=self.con, if_exists = 'append', chunksize = 1000)


#     def request(self, req: str):
#         response = self.cursor.execute(req)
#         return response.fetchall()


# class ExtractItems(ABC):
#     def __init__(self, client: ClientWeb, host: str, path: str):
#         self.host = host
#         self.path = path
#         self.url = host + path
#         self.client = client
#         self.df = pd.DataFrame(columns=Schemes.RAW)
    

#     @abstractmethod
#     def _get_pages_count(self) -> int:
#         raise NotImplementedError("Not implemented")
    

#     @abstractmethod
#     def _get_items(self) -> str:
#         raise NotImplementedError("Not implemented")
    

#     @abstractmethod
#     def _get_name(self) -> str:
#         raise NotImplementedError("Not implemented")
    

#     @abstractmethod
#     def _get_price_current(self) -> str:
#         raise NotImplementedError("Not implemented")


#     @abstractmethod
#     def _get_items_link(self) -> str:
#         raise NotImplementedError("Not implemented")
    
#     @task
#     def _get_items_by_all_pages(self):
#         logger = get_run_logger()
#         pages = self._get_pages_count(self.url)
#         if not pages:
#             pages = 1
#             logger.warning('Only one page is used for extracting')

#         urls = [self.url + f"page-{i}/" for i in range(1, pages)]
#         items = []
#         with ThreadPoolExecutor(max_workers=int(MAX_THREADS)) as executor:
#             bs_pages = list(executor.map(self.client.get_bs_by_url, urls))
#         for page in bs_pages:
#             for item in self._get_items(page):
#                 items.append(item)

#         if not len(items):
#             logger.error('No any items. Check internet connection or urls')
#             raise NoItemsError
        
#         return items


#     def _get_items_by_all_pages_consistently(self):
#         pages = self._get_pages_count(self.url)
#         if not pages:
#             pages = 1
#         pages = 2
#         items = []
#         for i in range(1, pages):
#             url = self.url + f"page-{i}/"
#             sp = self.client.get_bs_by_url(url)
#             for item in self._get_items(sp):
#                 items.append(item)
#         return items

#     @task
#     def extract(self):
#         items = self._get_items_by_all_pages()
#         for item in items:
#             name = self._get_name(item=item)
#             price, current = self._get_price_current(item=item)
#             link = self._get_items_link(item)
#             res = {
#                 "name": name,
#                 "price_ua": price,
#                 "link": self.host + link if link else None,
#             }

#             self.df = pd.concat([self.df, pd.DataFrame([res])])


#     @property
#     def dataframe(self):
#         return self.df
    

#     @property
#     def lasts_items(self):
#         return self.last_items_count


# class ExtractBootsMaleItems(ExtractItems):
#     def _get_items(self, sp: BeautifulSoup) -> Tag:
#         return sp.find('div', class_='Fkfp3V')


#     def _get_name(self, item: Tag) -> str:
#         try:
#             name = item.find('div', class_='ihuxuw').text
#             name = name.replace("\u2009", " ").replace("\xa0", " ")
#             return str(name)
#         except Exception:
#             return None


#     def _get_price_current(self, item: Tag) -> Tuple[int, str]:
#         try:
#             price = item.find('span', class_='MeSmTt').text
#             price, current = price.split("\u2009")
#             price = int(price.replace("\xa0", ""))

#             return price, current
#         except Exception:
#             return None, None


#     def _get_items_link(self, item: Tag) -> str:
#         try:
#             tag = item.find('a', class_='it25hX')
#             return tag.get('href')
#         except Exception:
#             return None


#     def _get_pages_count(self, url: str) -> int:
#         try:
#             sp = self.client.get_bs_by_url(url)
#             return len(sp.find(id='select-page'))
#         except Exception:
#             return None


# class Transform:
#     def __init__(self, ) -> None:
#         self.df = pd.DataFrame(columns=Schemes.OUT)

#     @task
#     def drop_none(self, df: pd.DataFrame):
#         logger = get_run_logger()
#         logger.info(f'Number of missing items: {df.isnull().any(axis=1).sum()}')
#         res = df.dropna()
        
#         if not len(res):
#             logger.error('Empty extract df. Check internet connection or urls')
#             raise EmptyDfError
        
#         return res

#     @task
#     def add_another_current(self, df: pd.DataFrame):
#         course = 35
#         df.loc[:, 'price_us'] = df.loc[:, 'price_ua'].apply(lambda x: round(x / course, 2))

#     @task
#     def add_data_time(self, df: pd.DataFrame):
#         df.loc[:, 'date'] = datetime.now()

#     @task
#     def transform(self, extract_df: pd.DataFrame):
#         extract_df = extract_df.copy()
#         extract_df = self.drop_none(extract_df)
#         self.add_another_current(extract_df)
#         self.add_data_time(extract_df)
#         self.df = pd.concat([self.df, extract_df])
        

#     @property
#     def dataframe(self):
#         return self.df

# @flow
# def main():
#     logger = get_run_logger()
#     try:
#         client = ClientWeb()
#         transform = Transform()
#         db = ClientDB(db='krossy.db')

#         boots_items_extractor = ExtractBootsMaleItems(client=client, host='https://megasport.ua', path='/ua/catalog/krossovki-i-snikersi/male/')
#         boots_items_extractor.extract()

#         transform.transform(boots_items_extractor.dataframe)  

#         db.write_df_to_db(transform.dataframe)

#         logger.info(f'It has written to db {len(transform.dataframe)} of items')

#     except Exception as e:
#         print(e)
#         logger.error(e, f'error: {str(e)}')


    


if __name__ == '__main__':
        extract_boots(host='https://megasport.ua', path='/ua/catalog/krossovki-i-snikersi/male/', ServeCls=ExtractBootsMaleItems)