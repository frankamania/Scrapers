import json

import scrapy
from scrapy.crawler import CrawlerProcess

import dateparser

def decodeEmail(e):
    de = ""
    k = int(e[:2], 16)

    for i in range(2, len(e) - 1, 2):
        de += chr(int(e[i:i + 2], 16) ^ k)

    return de


headers = {
    'Host': 'ufcstats.com',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-IN,en;q=0.9',
}

final_results = []


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

                dataset = {'name': i.css('td.b-statistics__table-col:nth-child(1) a::text').get(default="").strip(),
                           'location': i.css('td.b-statistics__table-col:nth-child(2)::text').get(default="").strip(),
                           'event_id': event_url.replace('http://ufcstats.com/event-details/',''),
                           'Event Date': dateparser.parse(i.css('td.b-statistics__table-col:nth-child(1) span.b-statistics__date::text').get(default="").strip())}

                yield scrapy.Request(
                    url=event_url,
                    headers=headers, callback=self.parse_events, cb_kwargs=dict(dataset=dataset)
                )
            except:
                pass

    def parse_events(self, response, dataset):
        def getClaenList(lst):
            return "\n".join([x.strip() for x in lst if x.strip() != ""])

        for i in response.css('tbody.b-fight-details__table-body tr.b-fight-details__table-row'):
            fighter_1 = i.css('td:nth-child(2) > p:nth-child(1) > a::attr(href)').get(default="").strip()
            fighter_2 = i.css('td:nth-child(2) > p:nth-child(2) > a::attr(href)').get(default="").strip()

            # fighter_1 = self.player_map[fighter_1]
            # fighter_2 = self.player_map[fighter_2]
            # methd = getClaenList(i.css('td:nth-child(8) > p ::text').getall())
            # win_or_loss = i.css('td:nth-child(1) > p > a > i > i::text').get(default="").strip()
            # weight_class= i.css('td:nth-child(7) > p::text').get(default="").strip()
            # round = i.css('td:nth-child(9) > p::text').get(default="").strip()
            # dataset.update(dict(fighter_1=fighter_1,fighter_2=fighter_2,methd=methd,win_or_loss=win_or_loss,round=round,weight_class=weight_class))
            # yield dataset






            # if 'win' in win_or_loss:
            #     if 'ufc_wins' in fighter_1:
            #         fighter_1['ufc_wins'] = fighter_1['ufc_wins'] + 1
            #     else:
            #         fighter_1['ufc_wins'] = 1
            #
            #     if 'ufc_losses' in fighter_2:
            #         fighter_2['ufc_losses'] = fighter_2['ufc_losses'] + 1
            #     else:
            #         fighter_2['ufc_losses'] = 1
            #
            #     if 'SUB' in methd:
            #
            #         if 'ufc_submission_wins' in fighter_1:
            #             fighter_1['ufc_submission_wins'] = fighter_1['ufc_submission_wins'] + 1
            #         else:
            #             fighter_1['ufc_submission_wins'] = 1
            #
            #         if 'ufc_submission_loss' in fighter_2:
            #             fighter_2['ufc_submission_loss'] = fighter_2['ufc_submission_loss'] + 1
            #         else:
            #             fighter_2['ufc_submission_loss'] = 1
            #
            #     if 'KO/TKO' in methd:
            #
            #         if 'ufc_KO_wins' in fighter_1:
            #             fighter_1['ufc_KO_wins'] = fighter_1['ufc_KO_wins'] + 1
            #         else:
            #             fighter_1['ufc_KO_wins'] = 1
            #
            #         if 'ufc_KO_loss' in fighter_2:
            #             fighter_2['ufc_KO_loss'] = fighter_2['ufc_KO_loss'] + 1
            #         else:
            #             fighter_2['ufc_KO_loss'] = 1
            #
            #     if 'DEC' in methd:
            #
            #         if 'ufc_Decision_wins' in fighter_1:
            #             fighter_1['ufc_Decision_wins'] = fighter_1['ufc_Decision_wins'] + 1
            #         else:
            #             fighter_1['ufc_Decision_wins'] = 1
            #
            #         if 'ufc_Decision_loss' in fighter_2:
            #             fighter_2['ufc_Decision_loss'] = fighter_2['ufc_Decision_loss'] + 1
            #         else:
            #             fighter_2['ufc_Decision_loss'] = 1
            #
            # else:
            #     if 'ufc_draws' in fighter_1:
            #         fighter_1['ufc_draws'] = fighter_1['ufc_draws'] + 1
            #     else:
            #         fighter_1['ufc_draws'] = 1
            #
            #     if 'ufc_draws' in fighter_2:
            #         fighter_2['ufc_draws'] = fighter_2['ufc_draws'] + 1
            #     else:
            #         fighter_2['ufc_draws'] = 1


            # figher_id = 1
            # dataset[f'Fighter {figher_id}'] = fighter_1.get("name","")
            # dataset[f'Wins (Fighter {figher_id})'] = fighter_1.get("wins","")
            # dataset[f'Losses (Fighter {figher_id})'] = fighter_1.get("losses","")
            # dataset[f'Draws (Fighter {figher_id})'] = fighter_1.get("draws","")
            #
            # dataset[f'Wins In UFC (Fighter {figher_id})'] = fighter_1.get("draws","")
            # dataset[f'Losses In UFC (Fighter {figher_id})'] = fighter_1.get("draws","")
            # dataset[f'Draws In UFC (Fighter {figher_id})'] = fighter_1.get("draws","")
            #
            # dataset[f'KO Wins (Fighter {figher_id})'] = fighter_1.get("draws","")
            # dataset[f'Sub Wins (Fighter {figher_id})'] = fighter_1.get("draws","")
            # dataset[f'Decision Wins (Fighter {figher_id})'] = fighter_1.get("draws","")
            #
            # dataset[f'KO Losses (Fighter {figher_id})'] = fighter_1.get("draws","")
            # dataset[f'Sub Losses (Fighter {figher_id})'] = fighter_1.get("draws","")
            # dataset[f'Decision Losses (Fighter {figher_id})'] = fighter_1.get("draws","")
            #
            #
            # dataset[f'Height (Fighter {figher_id})'] = fighter_1.get("Height","")
            # dataset[f'Weight (Fighter {figher_id})'] = fighter_1.get("Weight","")
            # dataset[f'Reach (Fighter {figher_id})'] = fighter_1.get("Reach","")
            # dataset[f'Stance (Fighter {figher_id})'] = fighter_1.get("STANCE","")
            #
            # dataset[f'Age (Fighter {figher_id})'] = fighter_1.get("STANCE","")
            #
            # dataset[f'SLpM (Fighter {figher_id})'] = fighter_1.get("SLpM","")
            # dataset[f'Str. Acc. (Fighter {figher_id})'] = fighter_1.get("Str. Acc.","")
            # dataset[f'SApM (Fighter {figher_id})'] = fighter_1.get("SApM","")
            # dataset[f'Str. Def (Fighter {figher_id})'] = fighter_1.get("Str. Def","")
            # dataset[f'TD Avg. (Fighter {figher_id})'] = fighter_1.get("TD Avg.","")
            # dataset[f'TD Acc. (Fighter {figher_id})'] = fighter_1.get("TD Acc.","")
            # dataset[f'TD Def. (Fighter {figher_id})'] = fighter_1.get("TD Def.","")
            # dataset[f'Sub. Avg. (Fighter {figher_id})'] = fighter_1.get("Sub. Avg.","")
            # dataset[f'Odds (Fighter {figher_id})'] = ""
            #
            #
            # dataset[f'Weight Class'] = i.css('td:nth-child(7) > p::text').get(default="").strip()
            # dataset[f'Winner'] = fighter_1.get("draws","")
            # dataset[f'Method'] = fighter_1.get("draws","")
            # dataset[f'Round'] = fighter_1.get("draws","")

            dataset['fight_id'] = i.css('::attr(data-link)').get(default="").replace('http://ufcstats.com/fight-details/','').strip()
            dataset['W/L'] = i.css('td:nth-child(1) > p > a > i > i::text').get(default="").strip()
            dataset['FIGHTER 1'] = fighter_1
            dataset['FIGHTER 2'] = fighter_2
            dataset['KD (fighter 1)'] = i.css('td:nth-child(3) > p:nth-child(1)::text').get(default="").strip()
            dataset['KD (fighter 2)'] = i.css('td:nth-child(3) > p:nth-child(2)::text').get(default="").strip()
            dataset['STR (fighter 1)'] = i.css('td:nth-child(4) > p:nth-child(1)::text').get(default="").strip()
            dataset['STR (fighter 2)'] = i.css('td:nth-child(4) > p:nth-child(2)::text').get(default="").strip()
            dataset['TD (fighter 1)'] = i.css('td:nth-child(5) > p:nth-child(1)::text').get(default="").strip()
            dataset['TD (fighter 2)'] = i.css('td:nth-child(5) > p:nth-child(2)::text').get(default="").strip()
            dataset['SUB (fighter 1)'] = i.css('td:nth-child(6) > p:nth-child(1)::text').get(default="").strip()
            dataset['SUB (fighter 2)'] = i.css('td:nth-child(6) > p:nth-child(2)::text').get(default="").strip()
            dataset['WEIGHT CLASS'] = i.css('td:nth-child(7) > p::text').get(default="").strip()
            dataset['METHOD'] = getClaenList(i.css('td:nth-child(8) > p ::text').getall())
            dataset['ROUND'] = i.css('td:nth-child(9) > p::text').get(default="").strip()
            dataset['TIME'] = i.css('td:nth-child(10) > p::text').get(default="").strip()

            yield dataset


if __name__ == "__main__":
    settings = {
        # 'FEED_EXPORT_ENCODING': 'utf-8-sig',
        # 'FEED_EXPORT_BATCH_ITEM_COUNT': 100000,
        'FEED_FORMAT': 'json',  # csv, json, xml
        'FEED_URI': "data_event_match_stats_first_page.json",  #
        ''
        ' ROBOTSTXT_OBEY': False,
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
