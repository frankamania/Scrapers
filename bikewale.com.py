import scrapy
import slugify.slugify
from scrapy.crawler import CrawlerProcess

cookies = {
    '_bwutmz': 'utmcsr%3D%28direct%29%7Cutmgclid%3D%7Cutmccn%3D%28direct%29%7Cutmcmd%3D%28none%29',
    'BWC': 'kDkiy2GqKmq2m4BErIUtSzfG1',
    '_bwtest': '18',
    'newUserSession': '0',
    '_cwv': 'kDkiy2GqKmq2m4BErIUtSzfG1.reQJEOT92U.1652617496.1652617781.1652617861.1',
}

headers = {
    'Host': 'www.bikewale.com',
    # Requests sorts cookies= alphabetically
    # 'Cookie': '_bwutmz=utmcsr%3D%28direct%29%7Cutmgclid%3D%7Cutmccn%3D%28direct%29%7Cutmcmd%3D%28none%29; BWC=kDkiy2GqKmq2m4BErIUtSzfG1; _bwtest=18; newUserSession=0; _cwv=kDkiy2GqKmq2m4BErIUtSzfG1.reQJEOT92U.1652617496.1652617781.1652617861.1',
    'device_model': 'ASUS_Z01QD',
    'device_brand': 'Asus',
    'device': 'Asus ASUS_Z01QD Asus',
    'os_api': '25',
    'os_version': '4.0.9',
    'version_name': '3.3.0',
    'version_code': '76',
    'platformid': '3',
    'user-agent': 'android.bikewale.com/3.3.0/9',
    'if-modified-since': 'Sun, 15 May 2022 06:55:08 GMT',
}

all_keys = []


class WEbCrawlerInS(scrapy.Spider):
    name = 'example'
    custom_settings = {

        'REFERER_ENABLED': True,
        'COOKIES_ENABLED': True,

    }

    def start_requests(self):

        yield scrapy.Request(
            url=f'https://www.bikewale.com/api/makelist/?requestType=19',
            headers=headers, cookies=cookies, callback=self.parse_brands)

    def parse_brands(self, response):
        jsn = response.json()
        for make in jsn['makes']:
            makeId = make['makeId']

            yield scrapy.Request(
                url=f'https://www.bikewale.com/api/v2/MakePage/?makeId={makeId}',
                headers=headers, cookies=cookies, callback=self.list_models)

    def list_models(self, response):
        jsn = response.json()
        for model in jsn['popularBikes']:
            modelId = model['objModel']['modelId']

            yield scrapy.Request(
                url=f'https://www.bikewale.com/api/v9/Model/{modelId}/details/?deviceId=cud7FMc2ROetU734zHmRhT',
                headers=headers, cookies=cookies, callback=self.parse_model,
                cb_kwargs=dict(VersionPrice=model['VersionPrice']))

    def parse_model(self, response, VersionPrice=0):
        jsn = response.json()

        for version in jsn['versionList']:
            versionId = version['versionId']
            modelId = jsn['modelId']
            makeId = jsn['makeId']

            key = f"{makeId}_{modelId}_{versionId}"

            dataset = {'data_item_key': key, 'Source URL': jsn['shareUrl'], 'Make': jsn['makeName'],
                       'Model': jsn['modelName'],
                       'isDiscontinued': jsn['isDiscontinued'], 'isUpcoming': jsn['isUpcoming'],
                       'Version': version['versionName'], 'price': version['price'], 'Ex-showroom': VersionPrice,
                       'Description': jsn['smallDescription']}

            for keySpec in jsn['keySpecs']:
                dataset[f'key {keySpec.get("displayText")}'] = keySpec.get("displayValue", None)

            yield scrapy.Request(
                url=f'https://www.bikewale.com/api/model/bikespecs/?modelId={modelId}&deviceId=1&versionId={versionId}',
                headers=headers, cookies=cookies, callback=self.parse_specs,
                cb_kwargs=dict(dataset=dataset, modelId=modelId, versionId=versionId))

    def parse_specs(self, response, dataset, versionId, modelId):

        jsn = response.json()

        for specification in jsn['specsCategory']:
            specc_type = specification['displayName']
            for itm in specification['specs']:
                dataset[f'{specc_type} {itm.get("displayText")}'] = itm.get("displayValue", None)

        for feature in jsn['featuresList']:
            dataset[f'{feature.get("displayText")}'] = feature.get("displayValue", None)

        dataset['modelColors'] = [x['colorName'] for x in jsn['modelColors']]

        yield scrapy.Request(
            url=f'https://www.bikewale.com/api/v2/PQCityList/?modelId={modelId}',
            headers=headers, cookies=cookies, callback=self.parse_citys,
            cb_kwargs=dict(dataset=dataset, modelId=modelId, versionId=versionId), dont_filter=True)

    def parse_citys(self, response, dataset, versionId, modelId):

        try:
            jsn = response.json()
            for city in jsn['cities'][:10]:
                Cityname = city['name']
                Cityid = city['id']
                if city['hasAreas']:

                    yield scrapy.Request(
                        url=f'https://www.bikewale.com/api/v2/PQAreaList/?modelId={modelId}&cityId={Cityid}',
                        headers=headers, cookies=cookies, callback=self.parse_areas,
                        cb_kwargs=dict(dataset=dataset, Cityname=Cityname, versionId=versionId, Cityid=Cityid,modelId=modelId),
                        dont_filter=True)
                else:

                    yield scrapy.Request(
                        url=f'https://www.bikewale.com/api/v6/model/versionlistprice/?modelId={modelId}&cityid={Cityid}&areaid=null&deviceId=cud7FMc2ROetU734zHmRhT',
                        headers=headers, cookies=cookies, callback=self.parse_final_price,
                        cb_kwargs=dict(dataset=dataset, Cityname=Cityname, versionId=versionId), dont_filter=True)
        except:
            yield dataset

            for ky in dataset.keys():
                if ky not in all_keys:
                    all_keys.append(ky)

    def parse_areas(self, response, dataset, versionId, modelId, Cityname, Cityid):
        try:
            jsn = response.json()
            areasid = jsn['areas'][0]['id']
            yield scrapy.Request(
                url=f'https://www.bikewale.com/api/v6/model/versionlistprice/?modelId={modelId}&cityid={Cityid}&areaid={areasid}&deviceId=cud7FMc2ROetU734zHmRhT',
                headers=headers, cookies=cookies, callback=self.parse_final_price,
                cb_kwargs=dict(dataset=dataset, Cityname=Cityname, versionId=versionId), dont_filter=True)
        except:
            yield dataset

            for ky in dataset.keys():
                if ky not in all_keys:
                    all_keys.append(ky)

    def parse_final_price(self, response, dataset, Cityname, versionId):

        dataset['Price City'] = Cityname
        dataset['Price City emikey'] = None

        try:
            jsn = response.json()
            version_map = {str(x['versionId']): x['price'] for x in jsn['versionList']}
            dataset['Price City emikey'] = version_map[str(versionId)]

            # for emikey, val in jsn.items():
            #     dataset[f'EMI Price {emikey}'] = val
        except Exception as e:
            print(e)
            pass

        yield dataset

        for ky in dataset.keys():
            if ky not in all_keys:
                all_keys.append(ky)


#
if __name__ == "__main__":
    fieldnames = ['Source URL', 'Make', 'Model', 'isDiscontinued', 'isUpcoming', 'Version', 'price', 'Description',
                  'key Displacement', 'key Max Power', 'key Kerb Weight', 'key Top Speed', 'Summary Displacement',
                  'Summary Max Power', 'Summary Max Torque', 'Summary Transmission', 'Summary Mileage - Owner Reported',
                  'Summary Front Brake Type', 'Summary Rear Brake Type', 'Summary Wheel Type', 'Summary Kerb Weight',
                  'Summary Chassis Type', 'Summary Top Speed', 'Summary Tyre Type', 'Summary Fuel Tank Capacity',
                  'Power & Performance Fuel Type', 'Power & Performance Max Power', 'Power & Performance Max Torque',
                  'Power & Performance Emission Standard', 'Power & Performance Displacement',
                  'Power & Performance Cylinders', 'Power & Performance Bore', 'Power & Performance Stroke',
                  'Power & Performance Valves Per Cylinder', 'Power & Performance Compression Ratio',
                  'Power & Performance Ignition', 'Power & Performance Spark Plugs',
                  'Power & Performance Cooling System', 'Power & Performance Transmission',
                  'Power & Performance Transmission Type', 'Power & Performance Gear Shifting Pattern',
                  'Power & Performance Clutch', 'Power & Performance Fuel Delivery System',
                  'Power & Performance Fuel Tank Capacity', 'Power & Performance Reserve Fuel Capacity',
                  'Power & Performance Riding Range', 'Power & Performance Mileage - ARAI',
                  'Power & Performance Mileage - Owner Reported', 'Power & Performance Top Speed',
                  'Brakes, Wheels & Suspension Braking System', 'Brakes, Wheels & Suspension Front Brake Type',
                  'Brakes, Wheels & Suspension Front Brake Size', 'Brakes, Wheels & Suspension Rear Brake Type',
                  'Brakes, Wheels & Suspension Rear Brake Size', 'Brakes, Wheels & Suspension Calliper Type',
                  'Brakes, Wheels & Suspension Wheel Type', 'Brakes, Wheels & Suspension Front Wheel Size',
                  'Brakes, Wheels & Suspension Rear Wheel Size', 'Brakes, Wheels & Suspension Front Tyre Size',
                  'Brakes, Wheels & Suspension Rear Tyre Size', 'Brakes, Wheels & Suspension Tyre Type',
                  'Brakes, Wheels & Suspension Radial Tyres', 'Brakes, Wheels & Suspension Front Tyre Pressure (Rider)',
                  'Brakes, Wheels & Suspension Rear Tyre Pressure (Rider)',
                  'Brakes, Wheels & Suspension Front Tyre Pressure (Rider & Pillion)',
                  'Brakes, Wheels & Suspension Rear Tyre Pressure (Rider & Pillion)',
                  'Brakes, Wheels & Suspension Front Suspension', 'Brakes, Wheels & Suspension Rear Suspension',
                  'Dimensions & Chassis Kerb Weight', 'Dimensions & Chassis Overall Length',
                  'Dimensions & Chassis Overall Width', 'Dimensions & Chassis Overall Height',
                  'Dimensions & Chassis Wheelbase', 'Dimensions & Chassis Ground Clearance',
                  'Dimensions & Chassis Seat Height', 'Dimensions & Chassis Chassis Type',
                  'Manufacturer Warranty Standard Warranty (Year)',
                  'Manufacturer Warranty Standard Warranty (Kilometers)', 'Odometer', 'DRLs (Daytime running lights)',
                  'Mobile App Connectivity', 'GPS & Navigation', 'USB charging port', 'Front storage box',
                  'Under seat storage', 'AHO (Automatic Headlight On)', 'Speedometer', 'Fuel Guage', 'Tachometer',
                  'Stand Alarm', 'Stepped Seat', 'No. of Tripmeters', 'Tripmeter Type', 'Low Fuel Indicator',
                  'Low Oil Indicator', 'Low Battery Indicator', 'Pillion Backrest', 'Pillion Grabrail', 'Pillion Seat',
                  'Pillion Footrest', 'Digital Fuel Guage', 'Start Type', 'Shift Light', 'Killswitch', 'Clock',
                  'Battery', 'Headlight Type', 'Brake/Tail Light', 'Turn Signal', 'Pass Light', 'Additional features',
                  'modelColors', 'Price City', 'EMI Price minTenure', 'EMI Price maxTenure', 'EMI Price defaultTenure',
                  'EMI Price minDownPayment', 'EMI Price maxDownPayment', 'EMI Price defaultDownPayment',
                  'EMI Price minLoanToValue', 'EMI Price maxLoanToValue', 'EMI Price defaultLoanToValue',
                  'EMI Price minRateOfInterest', 'EMI Price maxRateOfInterest', 'EMI Price defaultRateOfInterest',
                  'EMI Price processingFee', 'EMI Price emiAmount', 'EMI Price totalAmount',
                  'EMI Price onRoadBikePrice', 'EMI Price isReducingInterest', 'key Range', 'key Charging Time',
                  'key Motor Power (Rated)', 'Summary Riding Range', 'Summary Battery charging time',
                  'Summary Rated Power', 'Summary Battery capacity', 'Summary Battery warranty',
                  'Summary Carrying capacity', 'Power & Performance Rated Power', 'Power & Performance Gradeability',
                  'Power & Performance Battery charging time', 'Power & Performance Fast charging time',
                  'Power & Performance Carrying capacity', 'Power & Performance Battery capacity',
                  'Power & Performance Battery type', 'Power & Performance Motor type',
                  'Power & Performance Charger output', 'Manufacturer Warranty Battery warranty',
                  'Manufacturer Warranty Motor warranty', 'Regenerative breaking', 'Reverse mode',
                  'Touch screen display', 'Central locking system', 'Artificial sound', 'Parking assist', 'Hill Assist',
                  'Anti theft system', 'Geo fencing', 'Start/stop button']

    settings = {

        # 'FEED_EXPORT_ENCODING': 'utf-8-sig',
        # 'FEED_EXPORT_BATCH_ITEM_COUNT':20000,
        'FEED_FORMAT': 'json',  # csv, json, xml
        'FEED_URI': "bikewale.com_data3.json",  #
        ' ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 200,
        # 'FEED_EXPORT_FIELDS': fieldnames,
        # Configure maximum concurrent requests performed by Scrapy (default: 16)
        # 'CONCURRENT_REQUESTS': 100000,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 100000,
        'RETRY_ENABLED': True,
        'LOG_LEVEL': 'INFO',
        'download_timeout': 360,
        'RETRY_TIMES': 10,
        'HTTPCACHE_ENABLED': True,
        'HTTPCACHE_EXPIRATION_SECS': 0,
        'HTTPCACHE_DIR': 'httpcache_new2',
        'HTTPCACHE_IGNORE_HTTP_CODES': [int(x) for x in range(299, 600)],
        'HTTPCACHE_STORAGE': 'scrapy.extensions.httpcache.FilesystemCacheStorage',

    }

    c = CrawlerProcess(settings)
    c.crawl(WEbCrawlerInS)
    c.start()

    print(all_keys)
