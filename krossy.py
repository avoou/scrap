import requests
from pprint import pprint
from bs4 import BeautifulSoup, Tag
from typing import TypedDict, Optional, Tuple


def get_bs_by_url(url: str) -> BeautifulSoup:
    response = requests.get(url)
    response.encoding = 'utf-8'
    txt = response.text
    return BeautifulSoup(txt, 'html.parser')


def get_name(item: Tag) -> str:
    try:
        name = item.find('div', class_='ihuxuw').text
        name = name.replace("\u2009", " ").replace("\xa0", " ")
        return name
    except Exception:
        return None


def get_price_current(item: Tag) -> Tuple[int, str]:
    try:
        price = item.find('span', class_='MeSmTt').text
        price, current = price.split("\u2009")
        price = int(price.replace("\xa0", ""))

        return price, current
    except Exception:
        return None, None


def get_items_link(item: Tag) -> Tag:
    try:
        tag = item.find('a', class_='it25hX')
        return tag.get('href')
    except Exception:
        return None


def get_pages_count(sp: Tag) -> int:
    try:
        return len(sp.find(id='select-page'))
    except Exception:
        return 1
    

host = 'https://megasport.ua'
boots_url = '/ua/catalog/krossovki-i-snikersi/male/'

sp = get_bs_by_url(host + boots_url)

pages = len(sp.find(id='select-page'))

# TODO: add DataFrame
collect = []

for i in range(1, 2):
    url = host + boots_url + f"page-{i}/"
    sp = get_bs_by_url(url)
    items = sp.find('div', class_='Fkfp3V')
    
    for item in items:
        name = get_name(item=item)
        price, current = get_price_current(item=item)
        link = get_items_link(item)
        res = {
            "name": name,
            "price": price,
            "current": current,
            "link": host + str(link),
        }
        collect.append(res)


#pprint(collect)
print(len(collect))