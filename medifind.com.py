import json
import secrets
import socket

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.http import JsonRequest
from user_agent import generate_user_agent
from w3lib.http import basic_auth_header



class WEbCrawlerInS(scrapy.Spider):
    name = 'example'

    headers = {
        'content-type': 'application/json',
        'User-Agent': generate_user_agent(),

    }

    def start_requests(self):

        yield scrapy.Request(
            url='https://www.medifind.com/api/search/conditions?page=0&size=10000',
            headers=self.headers, callback=self.parse_conditions

        )

    def parse_conditions(self, response):
        jsn = response.json()
        for i in jsn['results']:
            json_data = {
                'specialty': [],
                'projectId': i['projectId'],
                'size': 10000,
                'page': 0,
                'type': 'conditionSearch',
            }

            yield JsonRequest(
                url='https://www.medifind.com/api/search/doctors',
                headers=self.headers, callback=self.parse, data=json_data

            )

    def parse(self, response):

        jsn = response.json()
        for i in jsn['results']:
            personId = i.get('personId', None)

            yield scrapy.Request(
                url=f'https://www.medifind.com/api/entity/doctor/{personId}',
                headers=self.headers, callback=self.parse_doc

            )

    def parse_doc(self, response):

        yield response.json()


if __name__ == "__main__":
    settings = {
        # 'FEED_EXPORT_ENCODING': 'utf-8-sig',
        # 'FEED_EXPORT_BATCH_ITEM_COUNT': 100000,
        'FEED_FORMAT': 'json',  # csv, json, xml
        'FEED_URI': "medifind.com.json",  #
        # 'FEED_EXPORT_FIELDS': ['name', 'address', 'Website', 'Fax', 'Toll-free phone', 'Phone', 'Comp Name 2nd line',
        #                        'See Reference'],
        'ROBOTSTXT_OBEY': False,
        # Configure maximum concurrent requests performed by Scrapy (default: 16)
        'CONCURRENT_REQUESTS': 100,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 10000,
        'RETRY_ENABLED': False,
        'COOKIES_ENABLED': False,
        'LOG_LEVEL': 'INFO',
        'DOWNLOAD_TIMEOUT': 600,
        # 'DOWNLOAD_DELAY': 25,
        'RETRY_TIMES': 10,
        'HTTPCACHE_ENABLED': True,
        'TELNETCONSOLE_ENABLED': False,
        'HTTPCACHE_EXPIRATION_SECS': 0,
        'HTTPCACHE_DIR': 'httpcache_new',
        'HTTPCACHE_IGNORE_HTTP_CODES': [int(x) for x in range(399, 600)],
        # 'HTTPCACHE_STORAGE': 'scrapy.extensions.httpcache.DbmCacheStorage'

    }

    c = CrawlerProcess(settings)
    c.crawl(WEbCrawlerInS)
    c.start()


