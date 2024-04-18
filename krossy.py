import requests
from bs4 import BeautifulSoup

results_ua_url = 'https://megasport.ua/ua/catalog/krossovki-i-snikersi/male/'
response = requests.get(results_ua_url)
print(response.encoding)
response.encoding = 'utf-8'
txt = response.text
sp = BeautifulSoup(txt, 'html.parser')

pages = len(sp.find(id='select-page'))

items = sp.find('div', class_='Fkfp3V')

items = [item for item in items]
item = items[1]
name = item.find('div', class_='ihuxuw').text
price = item.find('span', class_='MeSmTt').text
price = price.replace("\xa0", " ")
price = price.split(' грн')[0]
price = price.replace(" ", "")
price = int(price)
print(name, price)
print(len(sp.find(id='select-page')))