import requests
import pandas as pd
from pprint import pprint
from bs4 import BeautifulSoup, Tag
from typing import Tuple
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor

MAX_THREADS = 100

class Client:
    def __init__(self,) -> None:
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:78.0)   Gecko/20100101 Firefox/78.0", 
        }


    def get_bs_by_url(self, url:str) -> BeautifulSoup:
        response = requests.get(url, headers=self.headers)
        response.encoding = 'utf-8'
        txt = response.text
        return BeautifulSoup(txt, 'html.parser')


class ExtractItems(ABC):
    def __init__(self, client: Client, host: str, path: str):
        self.host = host
        self.path = path
        self.url = host + path
        self.client = client
        self.df = pd.DataFrame(columns=["name", "price", "current", "link"])
    

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
    

    def _get_items_by_all_pages(self):
        pages = self._get_pages_count(self.url)
        if not pages:
            pages = 1
            #TODO add logging that count of pages not found
        urls = [self.url + f"page-{i}/" for i in range(1, pages)]
        items = []
        with ThreadPoolExecutor(max_workers=int(MAX_THREADS)) as executor:
            bs_pages = list(executor.map(self.client.get_bs_by_url, urls))
        for page in bs_pages:
            for item in self._get_items(page):
                items.append(item)
        return items


    def _get_items_by_all_pages_consistently(self):
        pages = self._get_pages_count(self.url)
        if not pages:
            pages = 1
        pages = 2
        items = []
        for i in range(1, pages):
            url = self.url + f"page-{i}/"
            sp = self.client.get_bs_by_url(url)
            for item in self._get_items(sp):
                items.append(item)
        return items


    def get_extract_df(self):
        items = self._get_items_by_all_pages()
        for item in items:
            name = self._get_name(item=item)
            price, current = self._get_price_current(item=item)
            link = self._get_items_link(item)
            res = {
                "name": name,
                "price": price,
                "current": current,
                "link": self.host + str(link),
            }

            self.df = pd.concat([self.df, pd.DataFrame([res])])


    @property
    def dataframe(self):
        return self.df


class ExtractBootsMaleItems(ExtractItems):
    def _get_items(self, sp: BeautifulSoup) -> Tag:
        return sp.find('div', class_='Fkfp3V')


    def _get_name(self, item: Tag) -> str:
        try:
            name = item.find('div', class_='ihuxuw').text
            name = name.replace("\u2009", " ").replace("\xa0", " ")
            return name
        except Exception:
            return None


    def _get_price_current(self, item: Tag) -> Tuple[int, str]:
        try:
            price = item.find('span', class_='MeSmTt').text
            price, current = price.split("\u2009")
            price = int(price.replace("\xa0", ""))

            return price, current
        except Exception:
            return None, None


    def _get_items_link(self, item: Tag) -> Tag:
        try:
            tag = item.find('a', class_='it25hX')
            return tag.get('href')
        except Exception:
            return ''


    def _get_pages_count(self, url: str) -> int:
        try:
            sp = self.client.get_bs_by_url(url)
            return len(sp.find(id='select-page'))
        except Exception:
            return 1

    
import time
start = time.time()


client = Client()
boots_items_extractor = ExtractBootsMaleItems(client=client, host='https://megasport.ua', path='/ua/catalog/krossovki-i-snikersi/male/')
boots_items_extractor.get_extract_df()


print(len(boots_items_extractor.dataframe))
#print(boots_items_extractor.dataframe.info())
print('max boots price', boots_items_extractor.dataframe['price'].max())
print('min boots price', boots_items_extractor.dataframe['price'].min())
end = time.time()
print('time: ', end - start) #for 7 pages and 450 items it takes about 3 sec with threads