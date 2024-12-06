from base_task import DataIngestionTask
from datetime import datetime, timedelta

class CustomDataIngestionTask(DataIngestionTask):
    def __init__(self, web_service_url, file_path):
        super().__init__(web_service_url, file_path)
        self.meta_data = []

    def extract_metadata(self, params):
        file_meta_data = {
                "language": "en",
                "characterSet": "UTF-8",
                "hierarchyLevel": "dataset",
                "contact": {
                    "organisationName": "Hong Kong Observatory",
                    "contactInfo": {
                        "phone": {
                            "voice": "+852 2926 1133"
                        },
                        "address": {
                            "deliveryPoint": "134A Nathan Road, Kowloon",
                            "city": "Hong Kong",
                            "administrativeArea": "Kowloon",
                            "postalCode": "852",
                            "country": "Hong Kong",
                            "electronicMailAddress": "enquiry@hko.gov.hk"
                        },
                        "onlineResource": {
                            "linkage": "https://data.weather.gov.hk/weatherAPI/opendata/",
                            "protocol": "HTTP",
                            "name": "Hong Kong Observatory Open Data API"
                        }
                    },
                    "role": "pointOfContact"
                }
            }
        if params['dataType'] == 'HHOT':
            self.meta_data.append(file_meta_data | {
                "fileIdentifier": "hko_api_hhot",
                "title": "Hourly Heights of Astronomical Tides",
                "abstract": "Provides hourly heights of astronomical tides at specified stations.",
                "citation": {
                    "title": "HHOT - Hourly Heights of Astronomical Tides",
                    "date": {
                        "date": "2024-11",
                        "dateType": "revision"
                    }
                },
                "purpose": "Support tidal research and maritime navigation.",
                "onlineResource": {
                    "linkage": params['url'],
                    "protocol": "HTTP",
                    "name": "HHOT API"
                },
                "keywords": ["Tides", "Astronomical", "Marine"],
                "parameters": {
                    "dataType": "HHOT",
                    "station": params['station'],
                    "year": params['year']
                },
                "topicCategory": "oceans",
                "filePath": params['path'],
                "ingestionTime": ""
            })
        elif params['dataType'] == 'HLT':
            self.meta_data.append(file_meta_data | {
                "fileIdentifier": "hko_api_hlt",
                "title": "Times and Heights of Astronomical High and Low Tides",
                "abstract": "Provides times and heights of astronomical high and low tides.",
                "citation": {
                    "title": "HLT - Times and Heights of Astronomical Tides",
                    "date": {
                    "date": "2024-11",
                    "dateType": "revision"
                    }
                },
                "purpose": "Support maritime navigation and tidal prediction.",
                "onlineResource": {
                    "linkage": params['url'],
                    "protocol": "HTTP",
                    "name": "HLT API"
                },
                "keywords": ["High Tide", "Low Tide", "Astronomical"],
                "parameters": {
                    "dataType": "HLT",
                    "station": params['station'],
                    "year": params['year']
                },
                "topicCategory": "oceans",
                "filePath": params['path'],
                "ingestionTime": ""
            })
        elif params['dataType'] == 'MRS':
            self.meta_data.append(file_meta_data | {
                "fileIdentifier": "hko_api_mrs",
                "title": "Times of Moonrise, Moon Transit, and Moonset",
                "abstract": "Provides times of moonrise, moon transit, and moonset for specified dates.",
                "citation": {
                    "title": "MRS - Moonrise, Transit, and Moonset Times",
                    "date": {
                    "date": "2024-11",
                    "dateType": "revision"
                    }
                },
                "purpose": "Support astronomical studies and public knowledge.",
                "onlineResource": {
                    "linkage": params['url'],
                    "protocol": "HTTP",
                    "name": "MRS API"
                },
                "keywords": ["Astronomy", "Moonrise", "Moonset"],
                "parameters": {
                    "dataType": "MRS",
                    "year": params['year']
                },
                "topicCategory": "climatologyMeteorologyAtmosphere",
                "filePath": params['path'],
                "ingestionTime": ""
            })
        elif params['dataType'] == 'CLMTEMP':
            self.meta_data.append(file_meta_data | {
                "fileIdentifier": "hko_api_clmtemp",
                "title": "Daily Mean Temperature",
                "abstract": "Provides daily mean temperature data for specified stations.",
                "citation": {
                    "title": "CLMTEMP - Daily Mean Temperature",
                    "date": {
                    "date": "2024-11",
                    "dateType": "revision"
                    }
                },
                "purpose": "Support climate studies and public information.",
                "onlineResource": {
                    "linkage": params['url'],
                    "protocol": "HTTP",
                    "name": "CLMTEMP API"
                },
                "keywords": ["Temperature", "Climate", "Daily"],
                "parameters": {
                    "dataType": "CLMTEMP",
                    "station": params['station']
                },
                "topicCategory": "climatologyMeteorologyAtmosphere",
                "filePath": params['path'],
                "ingestionTime": ""
            })
        elif params['dataType'] == 'CLMMAXT':
            self.meta_data.append(file_meta_data | {
                "fileIdentifier": "hko_api_clmmaxt",
                "title": "Daily Maximum Temperature",
                "abstract": "Provides daily maximum temperature data for specified stations.",
                "citation": {
                    "title": "CLMMAXT - Daily Maximum Temperature",
                    "date": {
                    "date": "2024-11",
                    "dateType": "revision"
                    }
                },
                "purpose": "Support climate studies and public information.",
                "onlineResource": {
                    "linkage": params['url'],
                    "protocol": "HTTP",
                    "name": "CLMMAXT API"
                },
                "keywords": ["Temperature", "Maximum", "Climate"],
                "parameters": {
                    "dataType": "CLMMAXT",
                    "station": params['station']
                },
                "topicCategory": "climatologyMeteorologyAtmosphere",
                "filePath": params['path'],
                "ingestionTime": ""
            })
        elif params['dataType'] == 'CLMMINT':
            self.meta_data.append(file_meta_data | {
                "fileIdentifier": "hko_api_clmmint",
                "title": "Daily Minimum Temperature",
                "abstract": "Provides daily minimum temperature data for specified stations.",
                "citation": {
                    "title": "CLMMINT - Daily Minimum Temperature",
                    "date": {
                    "date": "2024-11",
                    "dateType": "revision"
                    }
                },
                "purpose": "Support climate studies and public information.",
                "onlineResource": {
                    "linkage": params['url'],
                    "protocol": "HTTP",
                    "name": "CLMMINT API"
                },
                "keywords": ["Temperature", "Minimum", "Climate"],
                "parameters": {
                    "dataType": "CLMMINT",
                    "station": params['station']
                },
                "topicCategory": "climatologyMeteorologyAtmosphere",
                "filePath": params['path'],
                "ingestionTime": ""
            })

    def create_data_info(self):
        data = []

        # Hourly heights of astronomical tides (by year)
        # https://data.weather.gov.hk/weatherAPI/opendata/opendata.php?dataType=HHOT&rformat=json&station=CCH&year=2024
        # Times and heights of astronomical high and low tides (by year)
        # https://data.weather.gov.hk/weatherAPI/opendata/opendata.php?dataType=HLT&rformat=json&station=CCH&year=2024
        years = [2022, 2023, 2024]
        stations = ['CCH', 'CLK', 'CMW', 'KCT', 'KLW', 'LOP', 'MWC', 'QUB', 'SPW', 'TAO', 'TBT', 'TMW', 'TPK', 'WAG']
        for dataType in ['HHOT', 'HLT']:
            for year in years:
                for station in stations:
                    url = f"https://data.weather.gov.hk/weatherAPI/opendata/opendata.php?dataType={dataType}&rformat=json&station={station}&year={year}"
                    path = f"{dataType}/{year}_{station}.json"
                    params = {
                        'dataType': dataType,
                        'year': year,
                        'station': station,
                        'url': url,
                        'path': path,
                    }
                    data.append(params)
                    self.extract_metadata(params)

        # Times of moonrise, moon transit and moonset (by year)
        # https://data.weather.gov.hk/weatherAPI/opendata/opendata.php?dataType=MRS&rformat=json&year=2024
        years = [2018, 2019, 2020, 2021, 2022, 2023, 2024]
        for dataType in ['MRS']:
            for year in years:
                url = f"https://data.weather.gov.hk/weatherAPI/opendata/opendata.php?dataType={dataType}&rformat=json&year={year}"
                path = f"{dataType}/{year}.json"
                params = {
                    'dataType': dataType,
                    'year': year,
                    'url': url,
                    'path': path,
                }
                data.append(params)
                self.extract_metadata(params)

        # Daily Mean Temperature (by station)
        # https://data.weather.gov.hk/weatherAPI/opendata/opendata.php?dataType=CLMTEMP&rformat=json&station=CCH
        # Daily Maximum Temperature (by station)
        # https://data.weather.gov.hk/weatherAPI/opendata/opendata.php?dataType=CLMMAXT&rformat=json&station=CCH
        # Daily Minimum Temperature (by station)
        # https://data.weather.gov.hk/weatherAPI/opendata/opendata.php?dataType=CLMMINT&rformat=json&station=CCH
        # stations = [CCH,CWB,HKA,HKO,HKP,HKS,HPV,JKB,KLT,KP,KSC,KTG,LFS,NGP,PEN,PLC,SE1,SEK,SHA,SKG,SKW,SSH,SSP,STY,TC,TKL,TMS,TPO,TU1,TW,TWN,TY1,TYW,VP1,WGL,WLP,WTS,YCT,YLP]
        stations = ['CCH', 'CWB', 'HKA', 'HKO', 'HKP', 'HKS', 'HPV', 'JKB', 'KLT', 'KP'
                    , 'KSC', 'KTG', 'LFS', 'NGP', 'PEN', 'PLC', 'SE1', 'SEK', 'SHA', 'SKG'
                    , 'SKW', 'SSH', 'SSP', 'STY', 'TC', 'TKL', 'TMS', 'TPO', 'TU1', 'TW'
                    , 'TWN', 'TY1', 'TYW', 'VP1', 'WGL', 'WLP', 'WTS', 'YCT', 'YLP']
        for dataType in ['CLMTEMP', 'CLMMAXT', 'CLMMINT']:
            for station in stations:
                url = f"https://data.weather.gov.hk/weatherAPI/opendata/opendata.php?dataType={dataType}&rformat=json&station={station}"
                path = f"TEMP/{dataType}_{station}.json"
                params = {
                    'dataType': dataType,
                    'station': station,
                    'url': url,
                    'path': path,
                }
                data.append(params)
                self.extract_metadata(params)

        # Weather and Radiation Level Report (by lang, station)
        # https://data.weather.gov.hk/weatherAPI/opendata/opendata.php?dataType=RYES&date=20190910&lang=en&station=CLK
        stations = ['CLK', 'CCH', 'HKO', 'HPV', 'HKP', 'SE1', 'KAT', 'KP', 'KLT', 'KTG'
                    , 'LFS', 'EPC', 'SKG', 'SWH', 'STK', 'SHA', 'SSP', 'SKW', 'SEK', 'STY'
                    , 'TKL', 'PLC', 'TAP', 'JKB', 'TBT', 'TY1', 'TWN', 'TW', 'TUN', 'HKS'
                    , 'WTS', 'YCT', 'YLP', 'YNF']
        start_date = datetime.strptime("20190910", "%Y%m%d")
        end_date = datetime.now() - timedelta(days=1)
        for dataType in ['RYES']:
            for lang in ['en', 'tc', 'sc']:
                for station in stations:
                    current_date = start_date
                    while current_date <= end_date:
                        formatted_date = current_date.strftime("%Y%m%d")
                        url = f"https://data.weather.gov.hk/weatherAPI/opendata/opendata.php?dataType={dataType}&date={formatted_date}&lang={lang}&station={station}"
                        path = f"{dataType}/{lang}_{station}_{formatted_date}.json"
                        data.append({
                            'dataType': dataType,
                            'lang': lang,
                            'station': station,
                            'date': formatted_date,
                            'url': url,
                            'path': path,
                        })
                        current_date += timedelta(days=1)
        return data
    
    def __call__(self):
        super().__call__()
