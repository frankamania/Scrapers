import scrapy
import slugify.slugify
from scrapy.crawler import CrawlerProcess

cookies = {

}

headers = {
    'Host': 'www.carwale.com',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
    'serverdomain': 'CarWale',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'sec-ch-ua-platform': '"Windows"',
    'accept': '*/*',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://www.carwale.com/dealer-showrooms/audi/bangalore/',
    'accept-language': 'en-IN,en;q=0.9,zh-TW;q=0.8,zh;q=0.7,ml;q=0.6,hi;q=0.5',
}

all_keys = []


class WEbCrawlerInS(scrapy.Spider):
    name = 'example'
    custom_settings = {

        'REFERER_ENABLED': True,
        'COOKIES_ENABLED': True,

    }

    # brands = ['Aston Martin']
    brands = ['Aston Martin', 'Audi', 'Bentley', 'BMW', 'Citroen', 'Datsun', 'Ferrari', 'Force Motors', 'Ford', 'Honda',
              'Hyundai', 'Isuzu', 'Jaguar', 'Jeep', 'Kia', 'Lamborghini', 'Land Rover', 'Lexus', 'Mahindra',
              'Maruti Suzuki',
              'Maserati', 'McLaren', 'Mercedes-Benz', 'MG', 'MINI', 'Nissan', 'Porsche', 'Renault', 'Rolls-Royce',
              'Skoda',
              'Tata', 'Toyota', 'Volkswagen', 'Volvo']

    #brands = ['Mahindra']

    def start_requests(self):
        for brand in self.brands:
            yield scrapy.Request(
                url=f'https://www.carwale.com/api/makepagedata/?maskingName={slugify.slugify(text=brand)}&platformId=1',
                headers=headers, cookies=cookies, callback=self.parse_models)

    def parse_models(self, response):


        jsn = response.json()
        for model in jsn['models']:

            modelMaskingName = model['modelMaskingName']
            makeMaskingName = model['makeMaskingName']

            yield scrapy.Request(
                url=f'https://www.carwale.com/api/modelpagedata/?makeMaskingName={makeMaskingName}&modelMaskingName={modelMaskingName}&showOfferUpfront=false&platformId=1',
                headers=headers, cookies=cookies, callback=self.parse_model)

    def parse_model(self, response):


        jsn = response.json()

        modelMaskingName = jsn['modelDetails']['modelMaskingName']
        makeMaskingName = jsn['modelDetails']['makeMaskingName']


        if len(jsn['nearByCities']) == 0:
            for version in jsn['versions']:
                versionMaskingName = version['versionMaskingName']

                price = version['priceOverview']['formattedPrice']
                exShowRoomPrice = version['priceOverview']['exShowRoomPrice']
                yield scrapy.Request(
                    url=f'https://www.carwale.com/api/versionpagedata/?makeMaskingName={makeMaskingName}&modelMaskingName={modelMaskingName}&versionMaskingName={versionMaskingName}&vid=-1',
                    headers=headers, cookies=cookies, callback=self.parse_varient, cb_kwargs=dict(city=None,price=price,exShowRoomPrice=exShowRoomPrice))
        else:
            for City in jsn['nearByCities']:

                city = City.get("name", "")
                city_id = City.get('id', "")

                for version in jsn['versions']:
                    versionMaskingName = version['versionMaskingName']
                    price = version['priceOverview']['formattedPrice']
                    exShowRoomPrice = version['priceOverview']['exShowRoomPrice']

                    yield scrapy.Request(
                        url=f'https://www.carwale.com/api/versionpagedata/?makeMaskingName={makeMaskingName}&modelMaskingName={modelMaskingName}&versionMaskingName={versionMaskingName}&vid=-1&cityId={city_id}&platformId=1&pageId=',
                        headers=headers, cookies=cookies, callback=self.parse_varient, cb_kwargs=dict(city=city,price=price,exShowRoomPrice=exShowRoomPrice))

    def parse_varient(self, response, city=None,price=None,exShowRoomPrice=None):
        jsn = response.json()

        if 'metaTags' in jsn:
            dataset = {}
            dataset['Source URL'] = jsn['metaTags']['canonical']
            dataset['Make'] = jsn['versionDetail']['makeName']
            dataset['Model'] = jsn['versionDetail']['modelName']
            dataset['Version'] = jsn['versionDetail']['versionName']
            dataset['status'] = jsn['versionDetail']['status']

            dataset['Notes'] = ""
            dataset['Class'] = ""
            dataset['Price'] = price
            dataset['exShowRoomPrice'] = jsn['versionDetail']['priceOverview']['exShowRoomPrice']
            dataset['Image URL'] = jsn['versionDetail']['imagePath']

            #
            # for keySpec in i['specsSummary']:
            #     dataset[
            #         f'specsSummary_{keySpec.get("itemName")}'] = f'{keySpec.get("value")} {keySpec.get("unitType")}'.strip()

            for keySpec in jsn['keySpecs']:
                dataset[f'key {keySpec.get("title")}'] = [x["text"] for x in keySpec.get("keySpecsValue")]

            for specification in jsn['specifications']:
                specc_type = specification['name']
                for itm in specification['items']:
                    dataset[f'{specc_type} {itm.get("name")}'] = [f"{x} {itm.get('unitType')}" for x in
                                                                  itm.get("values") if f"{x}".strip() != ""]

            for feature in jsn['features']:
                feature_type = feature['name']
                for itm in feature['items']:
                    dataset[f'{feature_type} {itm.get("name")}'] = [f"{x} {itm.get('unitType')}" for x in
                                                                    itm.get("values") if f"{x}".strip() != ""]

            dataset[f'Nearby City name'] = city
            dataset[f'Nearby City price'] = jsn['versionDetail']['priceOverview']['formattedPrice']

            yield dataset

        # for ky in dataset.keys():
        #     if ky not in all_keys:
        #         all_keys.append(ky)


if __name__ == "__main__":
    fieldnames = ['Source URL', 'Make', 'Model', 'Version', 'status', 'Notes', 'Class', 'Price', 'Image URL',
                  'key Price', 'key Mileage (ARAI)', 'key Engine', 'key Transmission', 'key Fuel Type ',
                  'key Seating Capacity', 'Engine & Transmission Top Speed',
                  'Engine & Transmission Acceleration (0-100 kmph)', 'Engine & Transmission Engine',
                  'Engine & Transmission Engine Type', 'Engine & Transmission Fuel Type ',
                  'Engine & Transmission Max Power (bhp@rpm)', 'Engine & Transmission Max Torque (Nm@rpm)',
                  'Engine & Transmission Performance on Alternate Fuel', 'Engine & Transmission Max Engine Performance',
                  'Engine & Transmission Max Motor Performance', 'Engine & Transmission Mileage (ARAI)',
                  'Engine & Transmission Driving Range', 'Engine & Transmission Drivetrain',
                  'Engine & Transmission Transmission', 'Engine & Transmission Emission Standard',
                  'Engine & Transmission Turbocharger/Supercharger', 'Engine & Transmission Battery',
                  'Engine & Transmission Battery Charging', 'Engine & Transmission Electric Motor',
                  'Engine & Transmission Others', 'Engine & Transmission Alternate Fuel', 'Dimensions & Weight Length',
                  'Dimensions & Weight Width', 'Dimensions & Weight Height', 'Dimensions & Weight Wheelbase',
                  'Dimensions & Weight Ground Clearance', 'Dimensions & Weight Kerb Weight', 'Capacity Doors',
                  'Capacity Seating Capacity', 'Capacity No of Seating Rows', 'Capacity Bootspace',
                  'Capacity Fuel Tank Capacity', 'Suspensions, Brakes, Steering & Tyres Four Wheel Steering',
                  'Suspensions, Brakes, Steering & Tyres Braking Performance',
                  'Suspensions, Brakes, Steering & Tyres Front Suspension',
                  'Suspensions, Brakes, Steering & Tyres Rear Suspension',
                  'Suspensions, Brakes, Steering & Tyres Front Brake Type',
                  'Suspensions, Brakes, Steering & Tyres Rear Brake Type',
                  'Suspensions, Brakes, Steering & Tyres Minimum Turning Radius',
                  'Suspensions, Brakes, Steering & Tyres Steering Type', 'Suspensions, Brakes, Steering & Tyres Wheels',
                  'Suspensions, Brakes, Steering & Tyres Spare Wheel',
                  'Suspensions, Brakes, Steering & Tyres Front Tyres',
                  'Suspensions, Brakes, Steering & Tyres Rear Tyres', 'Safety Overspeed Warning',
                  'Safety Lane Departure Warning', 'Safety Emergency Brake Light Flashing ',
                  'Safety Puncture Repair Kit', 'Safety Forward Collision Warning (FCW)',
                  'Safety Automatic Emergency Braking (AEB)', 'Safety High-beam Assist', 'Safety NCAP Rating',
                  'Safety Blind Spot Detection', 'Safety Lane Departure Prevention', 'Safety Rear Cross-Traffic Assist',
                  'Safety Airbags', 'Safety Tyre Pressure Monitoring System (TPMS)', 'Safety Child Seat Anchor Points',
                  'Safety Seat Belt Warning', 'Braking & Traction Anti-Lock Braking System (ABS)',
                  'Braking & Traction Electronic Brake-force Distribution (EBD)',
                  'Braking & Traction Brake Assist (BA)', 'Braking & Traction Electronic Stability Program (ESP)',
                  'Braking & Traction Four-Wheel-Drive', 'Braking & Traction Hill Hold Control',
                  'Braking & Traction Traction Control System (TC/TCS)', 'Braking & Traction Ride Height Adjustment',
                  'Braking & Traction Limited Slip Differential (LSD)', 'Locks & Security Engine immobilizer',
                  'Locks & Security Central Locking', 'Locks & Security Speed Sensing Door Lock',
                  'Locks & Security Child Safety Lock', 'Comfort & Convenience Air Conditioner',
                  'Comfort & Convenience Front AC', 'Comfort & Convenience Rear AC', 'Comfort & Convenience Heater',
                  'Comfort & Convenience Vanity Mirrors on Sun Visors', 'Comfort & Convenience Anti-glare Mirrors',
                  'Comfort & Convenience Parking Assist', 'Comfort & Convenience Parking Sensors',
                  'Comfort & Convenience Cruise Control', 'Comfort & Convenience Headlight & Ignition On Reminder',
                  'Comfort & Convenience Keyless Start/ Button Start', 'Comfort & Convenience Steering Adjustment',
                  'Comfort & Convenience 12V Power Outlets', 'Seats & Upholstery Driver Seat Adjustment',
                  'Seats & Upholstery Front Passenger Seat Adjustment', 'Seats & Upholstery Rear Row Seat Adjustment',
                  'Seats & Upholstery Seat Upholstery', 'Seats & Upholstery Leather-wrapped Steering Wheel',
                  'Seats & Upholstery Leather-wrapped Gear Knob', 'Seats & Upholstery Driver Armrest',
                  'Seats & Upholstery Rear Passenger Seats Type', 'Seats & Upholstery Ventilated Seats',
                  'Seats & Upholstery Ventilated Seat Type', 'Seats & Upholstery Interiors',
                  'Seats & Upholstery Interior Colours', 'Seats & Upholstery Rear Armrest',
                  'Seats & Upholstery Front Seatback Pockets', 'Seats & Upholstery Head-rests', 'Storage Cup Holders',
                  'Storage Driver Armrest Storage', 'Storage Cooled Glove Box ', 'Storage Sunglass Holder',
                  'Doors, Windows, Mirrors & Wipers Scuff Plates', 'Doors, Windows, Mirrors & Wipers Power Windows',
                  'Doors, Windows, Mirrors & Wipers One Touch -Down', 'Doors, Windows, Mirrors & Wipers One Touch - Up',
                  'Doors, Windows, Mirrors & Wipers Adjustable ORVM',
                  'Doors, Windows, Mirrors & Wipers Turn Indicators on ORVM',
                  'Doors, Windows, Mirrors & Wipers Exterior Door Handles',
                  'Doors, Windows, Mirrors & Wipers Rain-sensing Wipers',
                  'Doors, Windows, Mirrors & Wipers Interior Door Handles',
                  'Doors, Windows, Mirrors & Wipers Door Pockets',
                  'Doors, Windows, Mirrors & Wipers Side Window Blinds',
                  'Doors, Windows, Mirrors & Wipers Boot-lid Opener',
                  'Doors, Windows, Mirrors & Wipers Outside Rear View Mirrors (ORVMs)', 'Exterior Sunroof / Moonroof',
                  'Exterior Roof Mounted Antenna', 'Exterior Body-Coloured  Bumpers ',
                  'Exterior Chrome Finish Exhaust pipe', 'Lighting Cornering Headlights', 'Lighting Puddle Lamps',
                  'Lighting Ambient Interior Lighting', 'Lighting Daytime Running Lights', 'Lighting Fog Lights',
                  'Lighting Headlights', 'Lighting Automatic Head Lamps', 'Lighting Follow me home headlamps',
                  'Lighting Tail Lights', 'Lighting Cabin Lamps', 'Lighting Headlight Height Adjuster',
                  'Lighting Glove Box Lamp', 'Lighting Lights on Vanity Mirrors',
                  'Instrumentation Instantaneous Consumption', 'Instrumentation Instrument Cluster',
                  'Instrumentation Trip Meter', 'Instrumentation Average Fuel Consumption',
                  'Instrumentation Average Speed', 'Instrumentation Distance to Empty', 'Instrumentation Clock',
                  'Instrumentation Low Fuel Level Warning', 'Instrumentation Door Ajar Warning',
                  'Instrumentation Adjustable Cluster Brightness', 'Instrumentation Gear Indicator',
                  'Instrumentation Shift Indicator', 'Instrumentation Tachometer',
                  'Entertainment, Information & Communication Smart Connectivity',
                  'Entertainment, Information & Communication Display',
                  'Entertainment, Information & Communication Integrated (in-dash) Music System',
                  'Entertainment, Information & Communication Speakers',
                  'Entertainment, Information & Communication Steering mounted controls',
                  'Entertainment, Information & Communication GPS Navigation System',
                  'Entertainment, Information & Communication Bluetooth Compatibility',
                  'Entertainment, Information & Communication USB Compatibility',
                  'Entertainment, Information & Communication Aux Compatibility',
                  'Entertainment, Information & Communication AM/FM Radio',
                  'Entertainment, Information & Communication Wireless Charger',
                  'Entertainment, Information & Communication Head Unit Size',
                  'Entertainment, Information & Communication CD Player',
                  'Entertainment, Information & Communication DVD Playback',
                  'Entertainment, Information & Communication iPod Compatibility',
                  'Entertainment, Information & Communication Voice Command',
                  'Manufacturer Warranty Battery Warranty (Kilometres)', 'Manufacturer Warranty Warranty (Years)',
                  'Manufacturer Warranty Warranty (Kilometres)', 'Rear row Seat Adjustment', 'Nearby Citys Delhi',
                  'Braking & Traction Hill Descent Control', 'Braking & Traction Differential Lock',
                  'Comfort & Convenience Cabin-Boot Access', 'Telematics Find My Car',
                  'Telematics Check Vehicle Status Via App', 'Telematics Geo-Fence', 'Telematics Emergency Call',
                  'Telematics Over The Air (OTA) Updates', 'Telematics Remote AC On/Off Via app',
                  'Telematics Remote Car Lock/Unlock Via app', 'Telematics Remote Car Light Flashing & Honking Via app',
                  'Telematics Alexa Compatibility', 'Doors, Windows, Mirrors & Wipers Rear Defogger',
                  'Instrumentation Heads Up Display (HUD)',
                  'Entertainment, Information & Communication Internal Hard-drive',
                  'Seats & Upholstery 3rd Row Seats Type', 'Seats & Upholstery Folding Rear Seat',
                  'Seats & Upholstery Split Rear Seat', 'Exterior Body Kit', 'Nearby Citys Mumbai',
                  'Nearby Citys Bangalore', 'Nearby Citys Pune', 'Nearby Citys Hyderabad', 'Nearby Citys Ahmedabad',
                  'Nearby Citys Chennai', 'Nearby Citys Kolkata', 'Nearby Citys Chandigarh',
                  'Safety Middle rear three-point seatbelt', 'Safety Middle Rear Head Rest',
                  'Doors, Windows, Mirrors & Wipers Rear Wiper', 'Exterior Rub - Strips', 'Lighting Rear Reading Lamp',
                  'Entertainment, Information & Communication Display Screen for Rear Passengers',
                  'Manufacturer Warranty Battery Warranty (Years)', 'Telematics Remote Sunroof Open/Close Via app',
                  'Seats & Upholstery Third Row Seat Adjustment', 'Seats & Upholstery Split Third Row Seat',
                  'Storage Third Row Cup Holders', 'Doors, Windows, Mirrors & Wipers Rear Windshield Blind',
                  'Nearby Citys Navi Mumbai', 'Entertainment, Information & Communication Gesture Control',
                  'Rear row Seat Base Sliding', 'Comfort & Convenience Third Row AC']

    settings = {

        # 'FEED_EXPORT_ENCODING': 'utf-8-sig',
        # 'FEED_EXPORT_BATCH_ITEM_COUNT':20000,
        'FEED_FORMAT': 'json',  # csv, json, xml
        'FEED_URI': "carwale_car_data_for_process2.json",  #
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
        'HTTPCACHE_DIR': 'httpcache_new5',
        'HTTPCACHE_IGNORE_HTTP_CODES': [int(x) for x in range(299, 600)],
        'HTTPCACHE_STORAGE': 'scrapy.extensions.httpcache.FilesystemCacheStorage',

    }

    c = CrawlerProcess(settings)
    c.crawl(WEbCrawlerInS)
    c.start()

    print(all_keys)
