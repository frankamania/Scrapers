import json

import scrapy
from scrapy.crawler import CrawlerProcess





headers = {
    'Host': 'ufcstats.com',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-IN,en;q=0.9',
}

uniqe_clssses = []


class WEbCrawlerInS(scrapy.Spider):
    name = 'example'

    player_map = {x["url"]: x for x in json.loads(open('player_data.json', 'r', encoding='utf-8-sig').read())}

    def start_requests(self):
        yield scrapy.Request(
            url='http://ufcstats.com/statistics/events/completed?page=all',
            headers=headers, callback=self.parse,
        )

    def parse(self, response):
        for i in response.css('table.b-statistics__table-events tr.b-statistics__table-row'):

            try:
                event_url = i.css('td.b-statistics__table-col:nth-child(1) a::attr(href)').get(default="").strip()

                yield scrapy.Request(
                    url=event_url,
                    headers=headers, callback=self.parse_events
                )

            except:
                pass

    def parse_events(self, response):

        for i in response.css('tbody.b-fight-details__table-body tr.b-fight-details__table-row::attr(data-link)').getall():
            yield scrapy.Request(
                    url=i,
                    headers=headers, callback=self.parse_matches_stats_full,cb_kwargs=dict(event_id=response.url.replace('http://ufcstats.com/event-details/',''))
                )

    # def parse_fighter_data(self, response):
    #
    #     def getClaenList(lst):
    #         return "".join([x.strip() for x in lst if x.strip() != ""])
    #
    #     record = response.css('body > section > div > h2 > span.b-content__title-record::text').get(default="").replace('Record:', '').strip()
    #     wins,losses,draws = record.split('-')
    #
    #     dataset = {'name': response.css('body > section > div > h2 > span.b-content__title-highlight::text').get(default="").strip(),
    #                'Record': record,
    #                "url": response.url,
    #                "wins":wins,
    #                "losses":losses,
    #                "draws":draws
    #                }
    #
    #     for i in response.css('ul li.b-list__box-list-item.b-list__box-list-item_type_block'):
    #         key = i.css('i::text').get(default="").replace(':', '').strip()
    #         val = getClaenList(i.css('::text').getall()).replace(key, '').replace(':', '').strip()
    #         dataset[key] = val
    #
    #     for i in response.css('tbody.b-fight-details__table-body tr.b-fight-details__table-row'):
    #         for k in i.css('td:nth-child(2) > p > a::attr(href)').getall():
    #             yield scrapy.Request(
    #                 url=k,
    #                 headers=headers, callback=self.parse_fighter_data
    #             )
    #
    #     yield dataset

    def parse_matches_stats_full(self, response,event_id):

        fight_id = response.url.replace('http://ufcstats.com/fight-details/','')
        tr = response.css('body > section > div > div > section:nth-child(4) > table > tbody > tr')
        for i in [1,2]:
            fighter_name = tr.css(f'td:nth-child(1) p:nth-child({i}) a::text').get(default="").strip()
            fighter_id = tr.css(f'td:nth-child(1) p:nth-child({i}) a::attr(href)').get().replace('http://ufcstats.com/fighter'
                                                                                    '-details/','').strip()

            kd = tr.css(f'td:nth-child(2) p:nth-child({i})::text').get(default="").strip()
            sig_str = tr.css(f'td:nth-child(3) p:nth-child({i})::text').get(default="").strip()
            sig_str_persentage = tr.css(f'td:nth-child(4) p:nth-child({i})::text').get(default="").strip()
            total_str = tr.css(f'td:nth-child(5) p:nth-child({i})::text').get(default="").strip()
            td = tr.css(f'td:nth-child(6) p:nth-child({i})::text').get(default="").strip()
            td_persentage = tr.css(f'td:nth-child(7) p:nth-child({i})::text').get(default="").strip()
            sub_att = tr.css(f'td:nth-child(8) p:nth-child({i})::text').get(default="").strip()
            rev = tr.css(f'td:nth-child(9) p:nth-child({i})::text').get(default="").strip()
            ctrl = tr.css(f'td:nth-child(10) p:nth-child({i})::text').get(default="").strip()

            yield dict(
                event_id=event_id,
                fight_id=fight_id,
                fighter_name=fighter_name,
                fighter_id=fighter_id,
                kd=kd,
                sig_str=sig_str,
                sig_str_persentage=sig_str_persentage,
                total_str=total_str,
                td=td,
                td_persentage=td_persentage,
                sub_att=sub_att,
                rev=rev,
                ctrl=ctrl
            )





if __name__ == "__main__":
    settings = {
        # 'FEED_EXPORT_ENCODING': 'utf-8-sig',
        # 'FEED_EXPORT_BATCH_ITEM_COUNT': 100000,
        'FEED_FORMAT': 'json',  # csv, json, xml
        'FEED_URI': "match_ststs_more.json",  #

        'ROBOTSTXT_OBEY': False,
        # Configure maximum concurrent requests performed by Scrapy (default: 16)
        'CONCURRENT_REQUESTS': 5,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2500,
        'RETRY_ENABLED': False,
        'COOKIES_ENABLED': True,
        'LOG_LEVEL': 'INFO',
        'DOWNLOAD_TIMEOUT': 700,
        # 'DOWNLOAD_DELAY': 0.15,
        'RETRY_TIMES': 10,
        'HTTPCACHE_ENABLED': True,
        'HTTPCACHE_EXPIRATION_SECS': 0,
        'HTTPCACHE_DIR': 'httpcache_new',
        'HTTPCACHE_IGNORE_HTTP_CODES': [int(x) for x in range(399, 600)],
        'HTTPCACHE_STORAGE': 'scrapy.extensions.httpcache.FilesystemCacheStorage'

    }

    c = CrawlerProcess(settings)
    c.crawl(WEbCrawlerInS)
    c.start()
