from utilspy import Connector
from tqdm import tqdm
from tqdm.asyncio import tqdm as async_tqdm
import requests
import re
import asyncio
import aiohttp

import pandas as pd

from typing import Any, List, Tuple, Callable

from data_models import RuianCodeApiResponse, CoordinatesAPIResponse
from address_formatter import AddressFormatter, RemoveElementsFromLeftStrategy, RemoveElementsFromRightStrategy



class RuianFetcher(Connector):
    """
    Class for handling API calls to RUIAN web services
    """

    def __init__(self) -> None:
        self.__data = None

        self.address_formatter = AddressFormatter(RemoveElementsFromLeftStrategy())


    @staticmethod
    def code_api_details(address: str) -> Tuple:
        """Provide api details for ruian code API

        Returns:
            Tuple: (url: str, params: dict, headers: dict)
        """

        return ("https://vdp.cuzk.cz/vdp/ruian/adresnimista/fulltext", \
                
                {'adresa': address}, \

                {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br, zstd",
                    "Accept-Language": "en-GB,en;q=0.6",
                    "Cache-Control": "max-age=0",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                    "Sec-Gpc": "1",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                }
        )
    
    @staticmethod
    def coor_api_details(address: str) -> Tuple:
        """Provide api details for coordinates API

        Returns:
            Tuple: (url: str, params: dict, headers: dict)
        """

        return ("https://ags.cuzk.cz/arcgis/rest/services/RUIAN/Vyhledavaci_sluzba_nad_daty_RUIAN/MapServer/exts/GeocodeSOE/findAddressCandidates", \
                
                {
            'SingleLine': address,
            'f': 'pjson'
        }, \

                {
                    
                }
        )

    def ensure_length_limit(self, address: str) -> str:
        """
        Makes sure address is shorther than 40 chars (required by code API)
        """
        if len(address) > 40:
            address = self.address_formatter.cleanse(address)
        while len(address) > 40:
            address = self.address_formatter.format_address(address)
        return address

    def fetch_ruian_code(self, address: str) -> RuianCodeApiResponse:
        """
        Fetch RUIAN code data from RUIAN API for given address
        """

        # TODO prepare for 3 situations, error, success and success without found match

        address = self.ensure_length_limit(address)  # TODO not sure if here is good place for applying this method

        url, params, headers = self.code_api_details(address)

        response = requests.get(url, headers=headers, params=params)

        try:
            if response.status_code == 200:
                api_response = RuianCodeApiResponse(**response.json())
                if not api_response.polozky:
                    api_response.polozky = [None]
                return api_response
            else:
                return RuianCodeApiResponse(**{"polozky":[None],"existujiDalsiPolozky": False, "error_msg": f"HTTP Error {response.status_code}"})
            
        except Exception as e:
            return RuianCodeApiResponse({"polozky":[None],"existujiDalsiPolozky": False, "error_msg": f"{str(e)}"})
            
    def fetch_coordinates(self, address: str) -> CoordinatesAPIResponse:
        """
        Fetch data about coordinates from RUIAN API for given address
        """

        url, params, _ = self.coor_api_details(address)

        response = requests.get(url, params=params)

        try:
            api_response = CoordinatesAPIResponse(**response.json())
            if not api_response.candidates:
                api_response.candidates = [None]
            return api_response
            
        except Exception as e:
            return CoordinatesAPIResponse(**{"spatialReference": {"wkid": 102067, "latestWkid": 5514},
                                           "candidates": [None],
                                           "error_msg": f"{str(e)}"
                                           })
        
    def bulk_fetch_ruian_codes(self, column_name: str = 'undefined', file_path: str = '', server: str = '', db: str = '', in_table: str = '') -> List[RuianCodeApiResponse]:
        """
        Batch process multiple addresses
        """

        # TODo add export option as well

        data = self.load('auto', file_path, server, db, in_table)

        if column_name not in data:
            raise Exception(f'Column {column_name} is not present in DataFrame')
        
        data['ruian_code'] = None
        data['code_match_address'] = None

        responses = []
        
        for i, row in tqdm(data.iterrows(), total=data.shape[0], desc='Fetching ruian codes'):
            responses.append(self.fetch_ruian_code(row[column_name]))  

        ruians = [res.polozky[0].kod if res.polozky[0] is not None else None for res in responses]
        matches = [res.nazev if res is not None else None for res in responses]
        e = [res.error_msg for res in responses]

        data.loc[:, 'ruian_code'] = ruians
        data.loc[:, 'code_match_address'] = matches

        """
        for i, row in tqdm(data.iterrows(), total=data.shape[0], desc='Fetching coordinates'):
            responses.append(self.fetch_coordinates(row[column_name]))  

        coor_x = [res.location.x for res in responses]
        coor_y = [res.location.x for res in responses]
        matches2 = [res.address for res in responses]
        """

        self.export(data, 'excel', 'ruian_processed')

    def bulk_fetch_coordinates(self, column_name: str = 'undefined', file_path: str = '', server: str = '', db: str = '', in_table: str = '') -> List[CoordinatesAPIResponse]:
        """
        Batch process multiple addresses
        """

        # TODo add export option as well

        data = self.load('auto', file_path, server, db, in_table)

        pass
        
    async def afetch_ruian_code(self, address: str) -> RuianCodeApiResponse:
        """
        Asynchronous implementation of `fetch_ruian_code` method
        """
        address = self.ensure_length_limit(address)  # TODO not sure if here is good place for applying this method

        url, params, headers = self.code_api_details(address)

        async with asyncio.Semaphore(5):  # max 5 concurrent requests
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url=url, headers=headers, params=params) as response:
                        if response.status != 200:
                            json_data = await response.json()
                            
                            api_response = RuianCodeApiResponse(**json_data)
                            if not api_response.polozky:
                                api_response.polozky = [None]
                            return api_response
                        else:
                            return RuianCodeApiResponse(**{"polozky":[None],"existujiDalsiPolozky": False, "error_msg": f"HTTP Error {response.status}"})
                except Exception as e:
                    return RuianCodeApiResponse({"polozky":[None],"existujiDalsiPolozky": False, "error_msg": f"{str(e)}"})

    async def afetch_coordinates(self, address: str) -> CoordinatesAPIResponse:
        """
        Asynchronous implementation of `fetch_coordinates` method
        """
        url, params, _ = self.coor_api_details(address)

        async with asyncio.Semaphore(5):  # max 5 concurrent requests
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url=url, params=params) as response:
                        if response.status != 200:
                            json_data = await response.json()
                            api_response = RuianCodeApiResponse(**json_data)

                            if not api_response.polozky:
                                api_response.polozky = [None]
                            return api_response
                        else:
                            return RuianCodeApiResponse(**{"polozky":[None],"existujiDalsiPolozky": False, "error_msg": f"HTTP Error {response.status}"})
                except Exception as e:
                    return RuianCodeApiResponse({"polozky":[None],"existujiDalsiPolozky": False, "error_msg": f"{str(e)}"})

        

    async def abulk_fetch_ruian_codes(self, column_name: str = 'undefined', file_path: str = '', server: str = '', db: str = '', in_table: str = '') -> List[RuianCodeApiResponse]:
        """
        Asynchronously batch process multiple addresses
        """
        data = self.load('auto', file_path, server, db, in_table)
        
        if column_name not in data:
            raise Exception(f'Column {column_name} is not present in DataFrame')
        
        data['ruian_code'] = None
        data['code_match_address'] = None

        tasks = []

        # Prepare tasks for each row in the DataFrame
        for i, row in data.iterrows():
            if row['ruian_code'] is None:
                address = row[column_name]
                coro = self.afetch_ruian_code(address)
                tasks.append(coro)

        # Using tqdm for async progress tracking  ... not in order
        #responses = []
        #async for result in async_tqdm(asyncio.as_completed(tasks), total=len(tasks), desc='Fetching ruian codes'):
        #    response = await result
        #    responses.append(response)

        #responses = [await f for f in async_tqdm(asyncio.as_completed(tasks), total=len(tasks), desc='Fetching ruian codes')]

        responses = await async_tqdm.gather(*tasks)  # keeps order of DF

        ruians = [res.polozky[0].kod if res.polozky[0] is not None else None for res in responses]
        matches = [res.nazev if res is not None else None for res in responses]

    async def abulk_fetch_coordinates(self, column_name: str = 'undefined', file_path: str = '', server: str = '', db: str = '', in_table: str = '') -> List[CoordinatesAPIResponse]:
        """
        Asynchronously batch process multiple addresses
        """
        data = self.load('auto', file_path, server, db, in_table)


if __name__ == "__main__":
    
    r = RuianFetcher()

    asyncio.run(r.arun(column_name="address"))
    r.run(column_name="address")
