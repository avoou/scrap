# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BootsItem(scrapy.Item):
    # define the fields for your item here like:
    date = scrapy.Field()
    name = scrapy.Field()
    price_ua = scrapy.Field()
    price_us = scrapy.Field()
    link = scrapy.Field()
