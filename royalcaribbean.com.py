import scrapy
from scrapy.crawler import CrawlerProcess
from datetime import datetime


cookies = {

}


headers = {
    'Host': 'www.royalcaribbean.com',
    # Requests sorts cookies= alphabetically
    # 'Cookie': f"bgv=b2; check=true; rwd_id=d3d9949d-5ae6-42ad-a8bd-b5c875d0842e; country=USA; AMCVS_981337045329610C0A490D44%40AdobeOrg=1; gp=0; wuc=USA; language=en; wul=en; mt.v=2.2063590524.1648605705862; s_ecid=MCMID%7C64101824525015176144392487991098750663; AMCV_981337045329610C0A490D44%40AdobeOrg=1585540135%7CMCIDTS%7C19082%7CMCMID%7C64101824525015176144392487991098750663%7CMCAAMLH-1649210505%7C12%7CMCAAMB-1649210505%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1648612906s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C4.4.0; jsess=45808F5AE51DE74325620134D7AA6C9B; currency=USD; SSOExchange=0000000000; akacd_PRC-RCI-GDP-PRD=3826058477~rv=35~id=d582e20892e69bd4c7c5548e6ec1828c; visitorType=New to Cruise; MCMID=64101824525015176144392487991098750663; mboxEdgeCluster=31; s_dl=1; s_evar66cvp=%5B%5B%27Direct%27%2C%271648605707350%27%5D%5D; s_evar68cvp=%5B%5B%27Direct%27%2C%271648605707351%27%5D%5D; s_cc=true; _cs_c=0; AAM_SEGMENTS=aamseg%3D5101766%2CAAMSEG%3D22377820%2CAAMSEG%3D22402801; _scid=1fa7235b-a800-4424-9d6f-b442a5c13d01; __pdst=06f8ecc019dd451c989d91ea80f55d29; _fbp=fb.1.1648605707888.2013063091; _ga=GA1.2.1788372896.1648605708; _gid=GA1.2.1591552260.1648605708; _gcl_au=1.1.1755133787.1648605708; mt.mbsh=%7B%22fs%22:1648605708589%7D; _pin_unauth=dWlkPVptRm1ObU0zTmpZdE16STRaaTAwTmprMkxXRTFZVGt0TURJeE9EbGlNMlE0TjJWbA; __qca=P0-1847237163-1648605708730; _sctr=1|1648578600000; _clck=1gkotji|1|f07|0; closedConflict=true; AKA_A2=A; emailCaptureClosed=true; s_cp_url=%5B%5BB%5D%5D; s_v21=%5B%5B%27retargeting%2520not%2520active%27%2C%271648605758897%27%5D%5D; bgv=b2; pageNamePersistant=aem:cruises:itinerary; s_sq=%5B%5BB%5D%5D; aam_sc=aamsc%3D4490925%7C4501996%7C5101766%7C13426292%7C14941333%7C22377820%7C22402801; ADRUM=s=1648607487416&r=https%3A%2F%2Fwww.royalcaribbean.com%2Fcruises%2Fitinerary%2F4-night-bahamas-perfect-day-from-orlando-port-canaveral-on-independence%2FID04PCN-4069341060%3Fhash%3D-1509202941; mbox=session#844328c0f01647c596d37909cfbbce8f#1648609350|PC#844328c0f01647c596d37909cfbbce8f.31_0#1711850508; OptanonConsent=isIABGlobal=false&datestamp=Wed+Mar+30+2022+08%3A01%3A29+GMT%2B0530+(India+Standard+Time)&version=6.30.0&hosts=&consentId=adbcf418-310e-4c36-8bc9-8777edf3094c&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0007%3A1%2CC0004%3A1&AwaitingReconsent=false&geolocation=IN%3BKL; OptanonAlertBoxClosed=2022-03-30T02:31:29.135Z; AWSALB=W+A6y8D+eB2m0qezEAnNVakr+u2O94Ht629YWfzNeoDLEMiIRM6vIgHGK13Cxi0L22CHsvrIdYur8vrr0Afv/KgE9F5jCnLtu4kHlyMkOc21Sajjw7Q3hvxKpE3p; AWSALBCORS=W+A6y8D+eB2m0qezEAnNVakr+u2O94Ht629YWfzNeoDLEMiIRM6vIgHGK13Cxi0L22CHsvrIdYur8vrr0Afv/KgE9F5jCnLtu4kHlyMkOc21Sajjw7Q3hvxKpE3p; rcclGuestCookie=%7B%22dateCreated%22%3Anull%2C%22minDate%22%3Anull%2C%22maxDate%22%3Anull%2C%22searchUrl%22%3A%22https%3A%2F%2Fwww.royalcaribbean.com%2Fcruises%3Ffeatured%3Dtrue%26currentPage%3D1%22%2C%22numberOfNights%22%3A%5B%5D%2C%22sailingTo%22%3A%5B%5D%2C%22leavingFrom%22%3A%5B%5D%2C%22ships%22%3A%5B%5D%2C%22vacationTypes%22%3A%5B%5D%2C%22accessibility%22%3Anull%2C%22topSearchResults%22%3A%7B%7D%2C%22packageCode%22%3Anull%2C%22shipCode%22%3Anull%2C%22sailDate%22%3Anull%2C%22startDate%22%3Anull%2C%22departureCode%22%3Anull%2C%22itineraryName%22%3Anull%2C%22nights%22%3A0%2C%22stateroomPricing%22%3A%22LowestAvailable%22%2C%22itineraryUrl%22%3Anull%2C%22numberOfAdults%22%3A0%2C%22numberOfChildren%22%3A0%2C%22numberOfRooms%22%3A0%2C%22itineraryNumber%22%3Anull%2C%22bookingStatus%22%3Anull%2C%22groupId%22%3Anull%7D; _cs_id=a4763eeb-66d5-af87-9dd9-14f7bce0c291.1648605707.1.1648607491.1648605707.1.1682769707612; _cs_s=5.0.0.1648609291459; gpv_pn=no%20value; _uetsid=5b740d80afcd11ec83266b40dc5cbddc; _uetvid=5b747090afcd11ecb6f1ff1770f4d014; utag_main=v_id:017fd88eb6b40009f86ae5608e2b05072001f06a00b96{_sn:1$_se:21$_ss:0$_st:1648609294091$ses_id:1648605705913%3Bexp-session$_pn:5%3Bexp-session$vapi_domain:royalcaribbean.com$count_dc:3%3Bexp-1711678997930;} s_nr=1648607494129-New; _clsk=wt6dg9|1648607494832|19|0|www.clarity.ms/eus2-d/collect",
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'none',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-IN,en;q=0.9,zh-TW;q=0.8,zh;q=0.7,ml;q=0.6,hi;q=0.5',
}

uniqe_clssses = []

class WEbCrawlerInS(scrapy.Spider):
    name = 'example'


    def start_requests(self):

        yield scrapy.Request(
            url=f'https://www.royalcaribbean.com/ajax/cruises/service/lookup?initialLoad=false&featured=true&loadLinks=true&market=usa&country=USA&language=en&browser=chrome&browserVersion=99.0.4844&screenWidth=1883&browserWidth=1883',cookies =cookies ,
            headers=headers, callback=self.parse,cb_kwargs=dict(parse_next=True)

        )

    def parse(self, response,parse_next=False):
        jsn = response.json()

        if parse_next:

            for i in range(1,jsn['listResultsModule']['totalPages']+1):

                yield scrapy.Request(
                    url=f'https://www.royalcaribbean.com/ajax/cruises/service/lookup?initialLoad=false&featured=true&loadLinks=true&market=usa&country=USA&language=en&currentPage={i}&browser=chrome&browserVersion=99.0.4844&screenWidth=1883&browserWidth=1883',
                    cookies=cookies,
                    headers=headers, callback=self.parse,

                )

        for result in jsn['listResultsModule']['resultData']['pageResults']:

            cruise_name =result['displayName']
            ship_name =result['ship']['name']
            portOfDeparture = result['portOfDeparture']['name']
            sailingNights = result['sailingNights']
            #cruisePorts = result['sailingNights']
            #leaveing_from = [ x for x in result['cruisePorts']]

            for sail in result['sailings']:

                start_date = sail['startDate']
                cruisePorts = "|".join([ x['name'] for x in result['cruisePorts']])

                dataset = dict(cruise_name=cruise_name,ship_name=ship_name,sailingNights=sailingNights,portOfDeparture=portOfDeparture,cruisePorts=cruisePorts,start_date=start_date)

                dataset['price_INTERIOR'] = ""
                dataset['price_OUTSIDE'] = ""
                dataset['price_DELUXE'] = ""
                dataset['price_BALCONY'] = ""

                for pr in sail['priceRanges']:
                    dataset[f"price_{pr['category']}"] = pr['amount']


                for k,v in sail['cabinLaf'].items():
                    dataset[k] = v


                yield dataset






if __name__ == "__main__":

    now = datetime.now()
    date_time = now.strftime("%m_%d_%Y_%H_%M_%S")
    file_name = f'scraped_{date_time}.csv'

    settings = {
        #'FEED_EXPORT_ENCODING': 'utf-8-sig',
        #'FEED_EXPORT_BATCH_ITEM_COUNT': 100000,
        'FEED_FORMAT': 'csv',  # csv, json, xml
        'FEED_URI': file_name,  #
        ' ROBOTSTXT_OBEY': False,
        # Configure maximum concurrent requests performed by Scrapy (default: 16)
        'CONCURRENT_REQUESTS': 5,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2500,
        'RETRY_ENABLED': False,
        'COOKIES_ENABLED': True,
        'LOG_LEVEL': 'INFO',
        'DOWNLOAD_TIMEOUT': 700,
        #'DOWNLOAD_DELAY': 0.15,
        'RETRY_TIMES': 10,
        'HTTPCACHE_ENABLED': False,


    }

    c = CrawlerProcess(settings)
    c.crawl(WEbCrawlerInS)
    c.start()

print(uniqe_clssses)