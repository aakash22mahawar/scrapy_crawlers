import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import CloseSpider
import time

class ScrapyCrawler(CrawlSpider):
    name = "books"

    start_urls = ['https://books.toscrape.com/catalogue/page-1.html']

    book_details = LinkExtractor(restrict_css='h3>a')
    next_page = LinkExtractor(restrict_css='li.next>a')  # next page

    rule_book_details = Rule(book_details, callback='parse_item', follow=False)
    rule_next_page = Rule(next_page, follow=True)
    rules = (rule_book_details, rule_next_page)

    def __init__(self, *args, **kwargs):
        super(ScrapyCrawler, self).__init__(*args, **kwargs)
        self.item_count = 0
        self.item_limit = 1000  # Set your desired item limit here

    def parse_item(self, response):
        if response.status == 200:
            item = {
                'title': response.css('h1::text').get(),
                'upc': response.css('tr>td::text').get()
            }
            print(response.url, response.status)
            print(item)
            self.item_count += 1

            if self.item_count >= self.item_limit:
                raise CloseSpider('Item limit reached')

            yield item
        else:
            print(f'{response.url}, {response.status}, request has been failed')

if __name__ == "__main__":
    start = time.time()
    process = CrawlerProcess(settings={
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOAD_TIMEOUT': 5,
        'DOWNLOAD_DELAY': 0,
        'LOG_ENABLED': True,
        'LOG_LEVEL': 'INFO',
        'RETRY_ENABLED': False,  # Disable Scrapy's built-in retry mechanism
        'HTTPERROR_ALLOW_ALL': True,
    })

    process.crawl(ScrapyCrawler)
    process.start()
    end = time.time()
    print(round(end - start))
