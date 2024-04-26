import scrapy

#to run being in project dir: scrapy crawl boots

class BootSpider(scrapy.Spider):
    name = 'boots'

    def start_requests(self):
        urls = ['https://megasport.ua/ua/catalog/krossovki-i-snikersi/male/']
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def _get_name(self, item) -> str:
        name = item.css('div.ihuxuw::text').getall()
        name = "".join(name).replace("\u2009", " ").replace("\xa0", " ")
        return name if name else None
    

    def _get_price(self, item):
        price_current = "".join(item.css('span.MeSmTt::text').getall())
        if price_current:
            price, current = price_current.split("\u2009")
            price = int(price.replace("\xa0", ""))
        else:
            price = None

        return price
    

    def _get_link(self, item):
        return item.css('a.it25hX::attr(href)').get()


    def parse(self, response):
        items = response.css("div.Fkfp3V div.Z7K92d")
        for item in items:
            yield {
                "name": self._get_name(item),
                "price_ua": self._get_price(item),
                "link": response.urljoin(self._get_link(item)),
            }

        pagination = response.css('div.pfK9C7')
        next_page_path = pagination[0].css('[data-test-id="nextPage"]::attr(href)').get()
        if next_page_path:  
            url = response.urljoin(next_page_path)
            yield scrapy.Request(url=url, callback=self.parse)