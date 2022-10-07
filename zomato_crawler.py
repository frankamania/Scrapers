import json

import scrapy
from scrapy.crawler import CrawlerProcess

from scrapy.http import JsonRequest


def flatten(t):
    return [item for sublist in t for item in sublist]

cookies = {
    'fre': '0',
    'rd': '1380000',
    'zl': 'en',
    'fbtrack': '4fc81d99151b4dab6c509274994f2197',
    '_gcl_au': '1.1.971778517.1648016809',
    '_ga': 'GA1.2.1752204844.1648016809',
    '_fbp': 'fb.1.1648016809331.1128255738',
    'G_ENABLED_IDPS': 'google',
    'fbcity': '1',
    'ltv': '3',
    'lty': '3',
    'locus': '%7B%22addressId%22%3A0%2C%22lat%22%3A28.554254%2C%22lng%22%3A77.194418%2C%22cityId%22%3A1%2C%22ltv%22%3A3%2C%22lty%22%3A%22zone%22%2C%22fetchFromGoogle%22%3Afalse%2C%22dszId%22%3A193%2C%22fen%22%3A%22Delhi+NCR%22%7D',
    'PHPSESSID': '5b7c6f0868a4b60bbb4ea32024b37dfb',
    'csrf': 'f07e5d794d4441336ba1dccc7980c5e6',
    '_gid': 'GA1.2.433303400.1648450632',
    'g_state': '{"i_p":1649055442478,"i_l":3}',
    '_gat_global': '1',
    '_gat_city': '1',
    '_gat_country': '1',
    'AWSALBTG': 'b+WsMw46nCMJ0rso17XtS2EnxrzICz//xJcwUt50jsyJOkVeVKNw3mFdE3B2qwrcnlx6e/sd2XtBTZvqRnH8iV+xC7ldS6NtnBAkns5/yKpL7rFe/wp2OPsx9kQ64P4si7b6SnnbExgLxHEIVjALzEUWoJOFfSbLUmfCvK2Ttsyp',
    'AWSALBTGCORS': 'b+WsMw46nCMJ0rso17XtS2EnxrzICz//xJcwUt50jsyJOkVeVKNw3mFdE3B2qwrcnlx6e/sd2XtBTZvqRnH8iV+xC7ldS6NtnBAkns5/yKpL7rFe/wp2OPsx9kQ64P4si7b6SnnbExgLxHEIVjALzEUWoJOFfSbLUmfCvK2Ttsyp',
}

headers = {
    'Host': 'www.zomato.com',
    # Requests sorts cookies= alphabetically
    # 'Cookie': 'fre=0; rd=1380000; zl=en; fbtrack=4fc81d99151b4dab6c509274994f2197; _gcl_au=1.1.971778517.1648016809; _ga=GA1.2.1752204844.1648016809; _fbp=fb.1.1648016809331.1128255738; G_ENABLED_IDPS=google; fbcity=1; ltv=3; lty=3; locus=%7B%22addressId%22%3A0%2C%22lat%22%3A28.554254%2C%22lng%22%3A77.194418%2C%22cityId%22%3A1%2C%22ltv%22%3A3%2C%22lty%22%3A%22zone%22%2C%22fetchFromGoogle%22%3Afalse%2C%22dszId%22%3A193%2C%22fen%22%3A%22Delhi+NCR%22%7D; PHPSESSID=5b7c6f0868a4b60bbb4ea32024b37dfb; csrf=f07e5d794d4441336ba1dccc7980c5e6; _gid=GA1.2.433303400.1648450632; g_state={"i_p":1649055442478,"i_l":3}; _gat_global=1; _gat_city=1; _gat_country=1; AWSALBTG=b+WsMw46nCMJ0rso17XtS2EnxrzICz//xJcwUt50jsyJOkVeVKNw3mFdE3B2qwrcnlx6e/sd2XtBTZvqRnH8iV+xC7ldS6NtnBAkns5/yKpL7rFe/wp2OPsx9kQ64P4si7b6SnnbExgLxHEIVjALzEUWoJOFfSbLUmfCvK2Ttsyp; AWSALBTGCORS=b+WsMw46nCMJ0rso17XtS2EnxrzICz//xJcwUt50jsyJOkVeVKNw3mFdE3B2qwrcnlx6e/sd2XtBTZvqRnH8iV+xC7ldS6NtnBAkns5/yKpL7rFe/wp2OPsx9kQ64P4si7b6SnnbExgLxHEIVjALzEUWoJOFfSbLUmfCvK2Ttsyp',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
    'content-type': 'application/json',
    'x-zomato-csrft': 'f07e5d794d4441336ba1dccc7980c5e6',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36',
    'sec-ch-ua-platform': '"Windows"',
    'accept': '*/*',
    'origin': 'https://www.zomato.com',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://www.zomato.com/ncr/restaurants/starbucks-coffee',
    'accept-language': 'en-IN,en;q=0.9,zh-TW;q=0.8,zh;q=0.7,ml;q=0.6,hi;q=0.5',
}


uniqe_clssses = []

datasets = {}

class WEbCrawlerInS(scrapy.Spider):
    name = 'example'

    def start_requests(self):
        data = json.loads(open('zomato.json', 'r', encoding='utf-8-sig').read())

        yield JsonRequest(
            url='https://www.zomato.com/webroutes/search/applyFilter', data=data, cookies=cookies,
            headers=headers, callback=self.parse, cb_kwargs=dict(parse_next=True)

        )

    def parse(self, response, parse_next=False):
        jsn = response.json()
        data = {'dineoutAdsMetaData': {}, 'appliedFilter': [
            {'filterType': 'category_sheet', 'filterValue': 'delivery_home', 'isHidden': True, 'isApplied': True,
             'postKey': '{"category_context":"delivery_home"}'},
            {'filterType': 'sort', 'filterValue': 'rating_desc', 'postKey': '{"sort":"rating_desc"}',
             'isApplied': True},
            {'filterType': 'rating', 'filterValue': '5.0', 'isApplied': True, 'postKey': '{"rating":[5]}'},
            {'filterType': 'rating', 'filterValue': '4.0', 'isApplied': True, 'postKey': '{"rating":[4]}'},
            {'filterType': 'cfo', 'filterValue': '300', 'isApplied': True, 'postKey': '{"cfo":[300]}'},
            {'filterType': 'cfo', 'filterValue': '20000', 'isApplied': True, 'postKey': '{"cfo":[20000]}'}],
                'urlParamsForAds': {}}





        if parse_next:
            for i in jsn['pageData']['sections']['SECTION_SEARCH_RESULT']:
                #yield i

                yield scrapy.Request(
                    url=f'https://www.zomato.com/webroutes/getPage?page_url={i["order"]["actionInfo"]["clickUrl"]}&location=&isMobile=0', cookies=cookies,
                    headers=headers, callback=self.parse_resturent,cb_kwargs=dict(

                        avgcost=i["info"].get("costText",{}).get("text","")
                    )

                )

            if jsn['pageData']['sections']['SECTION_SEARCH_META_INFO']['searchMetaData']['hasMore']:
                data["searchMetadata"] = jsn['pageData']['sections']['SECTION_SEARCH_META_INFO']['searchMetaData']

                jsn_data = {
                    "context": "delivery",
                    "filters": json.dumps(data),
                    "addressId": 0,
                    "entityId": 3,
                    "entityType": "zone",
                    "locationType": "",
                    "isOrderLocation": 1,
                    "cityId": 1,
                    "latitude": "28.5542540000",
                    "longitude": "77.1944180000",
                    "userDefinedLatitude": 28.554254,
                    "userDefinedLongitude": 77.194418,
                    "entityName": "South Delhi",
                    "orderLocationName": "South Delhi",
                    "cityName": "Delhi NCR",
                    "countryId": 1,
                    "countryName": "India",
                    "displayTitle": "South Delhi",
                    "o2Serviceable": True,
                    "placeId": "193",
                    "cellId": "4110974516623048704",
                    "deliverySubzoneId": 193,
                    "placeType": "DSZ",
                    "placeName": "South Delhi",
                    "isO2City": True,
                    "fetchFromGoogle": False,
                    "fetchedFromCookie": False,
                    "isO2OnlyCity": False,
                    "address_template": [],
                    "otherRestaurantsUrl": ""
                }

                yield JsonRequest(
                    url='https://www.zomato.com/webroutes/search/home', data=jsn_data, cookies=cookies,
                    headers=headers, callback=self.parse

                )

        else:

            for i in jsn['sections']['SECTION_SEARCH_RESULT']:
                yield scrapy.Request(
                    url=f'https://www.zomato.com/webroutes/getPage?page_url={i["order"]["actionInfo"]["clickUrl"]}&location=&isMobile=0',
                    cookies=cookies,
                    headers=headers, callback=self.parse_resturent,cb_kwargs=dict(

                        avgcost=i["info"].get("costText",{}).get("text","")
                    )

                )

            if jsn['sections']['SECTION_SEARCH_META_INFO']['searchMetaData']['hasMore']:
                data["searchMetadata"] = jsn['sections']['SECTION_SEARCH_META_INFO']['searchMetaData']
                jsn_data = {
                    "context": "delivery",
                    "filters": json.dumps(data),
                    "addressId": 0,
                    "entityId": 3,
                    "entityType": "zone",
                    "locationType": "",
                    "isOrderLocation": 1,
                    "cityId": 1,
                    "latitude": "28.5542540000",
                    "longitude": "77.1944180000",
                    "userDefinedLatitude": 28.554254,
                    "userDefinedLongitude": 77.194418,
                    "entityName": "South Delhi",
                    "orderLocationName": "South Delhi",
                    "cityName": "Delhi NCR",
                    "countryId": 1,
                    "countryName": "India",
                    "displayTitle": "South Delhi",
                    "o2Serviceable": True,
                    "placeId": "193",
                    "cellId": "4110974516623048704",
                    "deliverySubzoneId": 193,
                    "placeType": "DSZ",
                    "placeName": "South Delhi",
                    "isO2City": True,
                    "fetchFromGoogle": False,
                    "fetchedFromCookie": False,
                    "isO2OnlyCity": False,
                    "address_template": [],
                    "otherRestaurantsUrl": ""
                }

                yield JsonRequest(
                    url='https://www.zomato.com/webroutes/search/home', data=jsn_data, cookies=cookies,
                    headers=headers, callback=self.parse

                )

    def parse_resturent(self, response,avgcost):

        jsn = response.json()
        dataset = {}

        dataset['Resturent Name'] = jsn['page_data']['sections']['SECTION_BASIC_INFO']["name"]
        dataset['Number of Recommended Items'] = [x["title"].replace('Recommended (','').replace(')','').strip() for x in flatten([y["children"] for y in jsn["page_data"]["navbarSection"] if y['title'] == 'Order Online']) if 'Recommended (' in x["title"]]
        dataset['Avg cost'] = ""
        dataset['Cuisine'] = jsn['page_data']['sections']['SECTION_BASIC_INFO']['cuisine_string']
        dataset['Number of Outlets'] = jsn['page_data']['sections']['SECTION_RES_CONTACT']['res_chain_text'].replace('See all ','').split(' ')[0]

        dataset['Location of Outlets'] = []
        #dataset['Location of Outlets'] = [y['displayName'] for y in flatten([ x['magicLinks'] for x in jsn['page_data']['sections']['SECTION_MAGIC_LINKS'] if x['title'] == "Top Stores"])]
        dataset['Delivery Rating'] = jsn['page_data']['sections']['SECTION_BASIC_INFO']['rating_new']['ratings']['DELIVERY']['rating']
        dataset['Number of Delivery Reviews'] = jsn['page_data']['sections']['SECTION_BASIC_INFO']['rating_new']['ratings']['DELIVERY']['reviewCount']
        dataset['Top Store'] = [y['displayName'] for y in flatten([ x['magicLinks'] for x in jsn['page_data']['sections']['SECTION_MAGIC_LINKS'] if x['title'] == "Top Stores"])]
        dataset['Seating Available'] = ""
        dataset['Popular Dishes'] = ""
        dataset['Review Highlights'] = ""
        dataset['Frequent Searches Leading to the Page'] = [y['displayName'] for y in flatten([ x['magicLinks'] for x in jsn['page_data']['sections']['SECTION_MAGIC_LINKS'] if x['title'] == "Frequent searches leading to this page"])]




        yield scrapy.Request(
            url=response.url.replace('/order&location=','&location='),
            cookies=cookies,
            headers=headers, callback=self.parse_resturent_review, cb_kwargs=dict(dataset=dataset)

        )



    def parse_resturent_review(self, response, dataset):
        jsn = response.json()

        if 'No Seating Available' in response.text:
            is_seating="No"
        else:
            is_seating="YES"

        dataset['Seating Available'] = is_seating

        dataset['Review Highlights'] = [x["title"] for x in jsn['page_data']['sections'].get("SECTION_TOP_TAGS",[])]
        try:
            dataset['Popular Dishes'] = jsn['page_data']['sections']["SECTION_RES_DETAILS"]["TOP_DISHES"].get(
                "description", "")
        except:
            dataset['Popular Dishes'] = ""
        dataset['Avg cost'] =  [x["title"] for x in jsn['page_data']['sections']["SECTION_RES_DETAILS"]["CFT_DETAILS"]["cfts"]]
        dataset['url'] = jsn['page_info']['pageUrl']

        if dataset['url'] not in datasets:
            datasets[dataset['url']] = dataset

        chain_url = jsn['page_data']['sections']['SECTION_RES_CONTACT']['res_chain_url'].replace('https://www.zomato.com','')
        if chain_url !="":
            yield scrapy.Request(
                url=f'https://www.zomato.com/webroutes/getPage?page_url={chain_url}&location=&isMobile=0',
                cookies=cookies,
                headers=headers, callback=self.parse_resturent_chain_id, cb_kwargs=dict(dataset=dataset)

            )





    def parse_resturent_chain_id(self, response, dataset):
        jsn = response.json()

        chain_id = None
        for cd in jsn['page_data']['sections']['SECTION_SEARCH_META_INFO']['searchMetaData']['filterInfo']['railFilters']:
            if cd['filterType'] == 'chain_id':
                chain_id = cd['filterValue']

        if chain_id is not None:
            filters = {'dineoutAdsMetaData': {},"appliedFilter":[{"filterType":"category_sheet","filterValue":"go_out","postKey":"{\"category_context\":\"go_out\"}"},{"filterType":"chain_id","filterValue":chain_id,"isApplied":True,"postKey":"{\"chain_id\":"+chain_id+"}"},{"filterType":"sort","filterValue":"popularity_desc","postKey":"{\"sort\":\"popularity_desc\"}","isApplied":True}],"urlParamsForAds":{}}
            jsn_data = {
                "context": "all",
                "filters": json.dumps(filters),
                "addressId": 0,
                "entityId": 3,
                "entityType": "zone",
                "locationType": "",
                "isOrderLocation": 1,
                "cityId": 1,
                "latitude": "28.5542540000",
                "longitude": "77.1944180000",
                "userDefinedLatitude": 28.554254,
                "userDefinedLongitude": 77.194418,
                "entityName": "Delhi NCR",
                "orderLocationName": "Delhi NCR",
                "cityName": "Delhi NCR",
                "countryId": 1,
                "countryName": "India",
                "displayTitle": "Delhi NCR",
                "o2Serviceable": True,
                "placeId": "193",
                "cellId": "4110974516623048704",
                "deliverySubzoneId": 193,
                "placeType": "DSZ",
                "placeName": "Delhi NCR",
                "isO2City": True,
                "fetchFromGoogle": False,
                "fetchedFromCookie": True,
                "isO2OnlyCity": False,
                "address_template": [],
                "otherRestaurantsUrl": ""
            }
            yield scrapy.Request(
                url='https://www.zomato.com/webroutes/search/applyFilter',method='POST',body=json.dumps(jsn_data),
                cookies=cookies,
                headers=headers, callback=self.parse_resturent_locations, cb_kwargs=dict(dataset=dataset,parse_next=True,filters=filters)

            )

    def parse_resturent_locations(self, response, dataset,filters,parse_next=False):
        jsn = response.json()

        if parse_next:
            for i in jsn['pageData']['sections']['SECTION_SEARCH_RESULT']:

                datasets[dataset['url']]['Location of Outlets'].append(i['info']['locality']['name'])




            if jsn['pageData']['sections']['SECTION_SEARCH_META_INFO']['searchMetaData']['hasMore']:
                data = filters.copy()
                data["searchMetadata"] = jsn['pageData']['sections']['SECTION_SEARCH_META_INFO']['searchMetaData']

                jsn_data = {
            "context": "all",
            "filters": json.dumps(data),
            "addressId": 0,
            "entityId": 3,
            "entityType": "zone",
            "locationType": "",
            "isOrderLocation": 1,
            "cityId": 1,
            "latitude": "28.5542540000",
            "longitude": "77.1944180000",
            "userDefinedLatitude": 28.554254,
            "userDefinedLongitude": 77.194418,
            "entityName": "Delhi NCR",
            "orderLocationName": "Delhi NCR",
            "cityName": "Delhi NCR",
            "countryId": 1,
            "countryName": "India",
            "displayTitle": "Delhi NCR",
            "o2Serviceable": True,
            "placeId": "193",
            "cellId": "4110974516623048704",
            "deliverySubzoneId": 193,
            "placeType": "DSZ",
            "placeName": "Delhi NCR",
            "isO2City": True,
            "fetchFromGoogle": False,
            "fetchedFromCookie": True,
            "isO2OnlyCity": False,
            "address_template": [],
            "otherRestaurantsUrl": ""
        }

                yield JsonRequest(
                    url='https://www.zomato.com/webroutes/search/home', data=jsn_data, cookies=cookies,
                    headers=headers, callback=self.parse_resturent_locations, cb_kwargs=dict(dataset=dataset,filters=filters)

                )

        else:

            for i in jsn['sections']['SECTION_SEARCH_RESULT']:

                datasets[dataset['url']]['Location of Outlets'].append(i['info']['locality']['name'])

            if jsn['sections']['SECTION_SEARCH_META_INFO']['searchMetaData']['hasMore']:
                data = filters.copy()
                data["searchMetadata"] = jsn['sections']['SECTION_SEARCH_META_INFO']['searchMetaData']

                jsn_data = {
                    "context": "all",
                    "filters": json.dumps(data),
                    "addressId": 0,
                    "entityId": 3,
                    "entityType": "zone",
                    "locationType": "",
                    "isOrderLocation": 1,
                    "cityId": 1,
                    "latitude": "28.5542540000",
                    "longitude": "77.1944180000",
                    "userDefinedLatitude": 28.554254,
                    "userDefinedLongitude": 77.194418,
                    "entityName": "Delhi NCR",
                    "orderLocationName": "Delhi NCR",
                    "cityName": "Delhi NCR",
                    "countryId": 1,
                    "countryName": "India",
                    "displayTitle": "Delhi NCR",
                    "o2Serviceable": True,
                    "placeId": "193",
                    "cellId": "4110974516623048704",
                    "deliverySubzoneId": 193,
                    "placeType": "DSZ",
                    "placeName": "Delhi NCR",
                    "isO2City": True,
                    "fetchFromGoogle": False,
                    "fetchedFromCookie": True,
                    "isO2OnlyCity": False,
                    "address_template": [],
                    "otherRestaurantsUrl": ""
                }

                yield JsonRequest(
                    url='https://www.zomato.com/webroutes/search/home', data=jsn_data, cookies=cookies,
                    headers=headers, callback=self.parse_resturent_locations, cb_kwargs=dict(dataset=dataset,filters=filters)

                )



if __name__ == "__main__":
    settings = {
        # 'FEED_EXPORT_ENCODING': 'utf-8-sig',
        # 'FEED_EXPORT_BATCH_ITEM_COUNT': 100000,
        'FEED_FORMAT': 'csv',  # csv, json, xml
        'FEED_URI': "zomato_full3.csv",  #

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

with open('zomato_new_scrape.json','w',encoding='utf-8-sig') as mj:
    json.dump(datasets,mj)