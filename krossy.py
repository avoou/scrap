import requests
from pprint import pprint
from bs4 import BeautifulSoup, Tag
from typing import TypedDict, Optional, Tuple, List
from abc import ABC, abstractmethod


class ExtractItems(ABC):
    def __init__(self):
        return
    
    @abstractmethod
    def get_items(self) -> str:
        raise NotImplementedError("Not implemented")
    

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError("Not implemented")
    

    @abstractmethod
    def get_price_current(self) -> str:
        raise NotImplementedError("Not implemented")


    @abstractmethod
    def get_items_link(self) -> str:
        raise NotImplementedError("Not implemented")
    

    @abstractmethod
    def get_extract_df(self) -> dict:
        raise NotImplementedError("Not implemented")

class Client:
    def __init__(self,) -> None:
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:78.0)   Gecko/20100101 Firefox/78.0", 
            "Referer": "https://www.google.com"
        }


    def get_bs_by_url(self, url:str) -> BeautifulSoup:
        response = requests.get(url, headers=self.headers)
        response.encoding = 'utf-8'
        txt = response.text
        return BeautifulSoup(txt, 'html.parser')


class ExtractBootsMaleItems(ExtractItems):
    def __init__(self, client: Client, host: str, path: str) -> None:
        self.host = host
        self.path = path
        self.url = host + path
        self.client = client
        self.result = []


    def get_items(self, sp: BeautifulSoup) -> Tag:
        return sp.find('div', class_='Fkfp3V')


    def get_name(self, item: Tag) -> str:
        try:
            name = item.find('div', class_='ihuxuw').text
            name = name.replace("\u2009", " ").replace("\xa0", " ")
            return name
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


    def get_items_link(self, item: Tag) -> Tag:
        try:
            tag = item.find('a', class_='it25hX')
            return tag.get('href')
        except Exception:
            return None


    def get_pages_count(self, url: str) -> int:
        try:
            sp = self.client.get_bs_by_url(url)
            return len(sp.find(id='select-page'))
        except Exception:
            return 1


    def get_extract_df(self):
        pages = self.get_pages_count(self.url)
        pages = 2

        for i in range(1, pages):
            url = self.url + f"page-{i}/"
            sp = self.client.get_bs_by_url(url)
            items = self.get_items(sp)
            
            for item in items:
                name = self.get_name(item=item)
                price, current = self.get_price_current(item=item)
                link = self.get_items_link(item)
                res = {
                    "name": name,
                    "price": price,
                    "current": current,
                    "link": self.host + str(link),
                }
                self.result.append(res)
    

client = Client()
boots_items_extractor = ExtractBootsMaleItems(client=client, host='https://megasport.ua', path='/ua/catalog/krossovki-i-snikersi/male/')
boots_items_extractor.get_extract_df()

print(len(boots_items_extractor.result))