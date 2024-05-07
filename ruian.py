from utilspy import Connector
from tqdm import tqdm
from tqdm.asyncio import tqdm as async_tqdm
import requests
import re
import asyncio
import aiohttp
import json

import pandas as pd

from typing import Any, List, Tuple, Callable, Optional, Union, Type

from data_models import RuianCodeApiResponse, CoordinatesAPIResponse, ApiResponse
from address_formatter import AddressFormatter, RemoveElementsFromLeftStrategy, RemoveElementsFromRightStrategy
from utils import ensure_length_limit, ensure_clean_address, retry_api_call, retry_adjust_api_call, aensure_length_limit, aensure_clean_address, aretry_adjust_api_call



class RuianFetcher(Connector):
    """
    Class for handling API calls to RUIAN web services
    """

    def __init__(self) -> None:

        self.address_formatter = AddressFormatter(RemoveElementsFromLeftStrategy())

    def adjust_address(self, address: str, *args, **kwargs) -> Tuple:
        """Adjust the address using the formatter

        Args:
            address (str): address string

        Returns:
            Tuple: Containing the new arguments list and 
            kwargs dictionary
        """
        
        new_address = self.address_formatter.format_address(address)
        
        return ([new_address] + list(args), kwargs)
    
    def __load_check_data(self, addresses: Optional[Tuple[str]] = None, in_file: str = '', server: str = '', db: str = '', in_table: str = '', column_name: str = 'undefined') -> pd.DataFrame:
        """helper method to load data and do basic checks

        Args:
            addresses (Optional[Tuple[str]], optional): Tuple of address strings to be processed. Defaults to None.
            in_file (str, optional): Path to input file. Defaults to ''.
            server (str, optional): Name of server in local network. Defaults to ''.
            db (str, optional): Name of MS SQL database. Defaults to ''.
            in_table (str, optional): Name of input table. Defaults to ''.
            
        Raises:
            Exception: If `column_name` not present in input dataframe 
            Exception: No data provided

        Returns:
            Tuple (pd.DataFrame, str): dataframe containing input data with addresses and column name
        """
        if addresses is not None:
            column_name = 'address'
            data = pd.DataFrame({column_name: addresses})
        else:
            data = self.load('auto', in_file, server, db, in_table)
        
        if data.empty:
            raise Exception("No data provided")

        if column_name not in data:
            raise Exception(f'Column {column_name} is not present in DataFrame')

        return data, column_name

    @staticmethod
    def code_api_details(address: str) -> Tuple:
        """Provide api details for ruian code API
        Args:
            address (str): address string

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

        Args:
            address (str): address string
        Returns:
            Tuple: (url: str, params: dict, headers: dict)
        """

        return ("https://ags.cuzk.cz/arcgis/rest/services/RUIAN/Vyhledavaci_sluzba_nad_daty_RUIAN/MapServer/exts/GeocodeSOE/findAddressCandidates", \
                
                {
            'SingleLine': address,
            'f': 'pjson'
        }, \

                {}
        )
    
    def __perform_api_call(self, address: str, test_if_empty: Callable[[Union[CoordinatesAPIResponse, RuianCodeApiResponse]], bool],
                    api_response_object: Type[Union[CoordinatesAPIResponse, RuianCodeApiResponse]], api_details: Callable[[str], Tuple],
                    session: Optional[requests.Session] = None) -> ApiResponse:
        """generic api call

        Args:
            address (str): address string
            test_if_empty (Callable[[Union[CoordinatesAPIResponse, RuianCodeApiResponse]], bool]): function to test if response is empty
            api_response_object (Type[Union[CoordinatesAPIResponse, RuianCodeApiResponse]]): object in which data will be encapsulated
            api_details (Callable[[str], Tuple]): static method/function to provide api call details like url, params and headers

        Returns:
            ApiResponse: Response of API
        """
        requires_local_session = session is None
        if requires_local_session:
            session = requests.Session()
        try:
            url, params, headers = api_details(address)

            with session.get(url, headers=headers, params=params) as response:

                try:
                    if response.status_code == 200:
                        api_response = api_response_object(**response.json())
                        if test_if_empty(api_response):
                            return ApiResponse()
                        return ApiResponse(response=api_response)
                    else:
                        return ApiResponse(response=None, error_msg=f"HTTP Error {response.status_code}")
                    
                except Exception as e:
                    return ApiResponse(response=None, error_msg=f"{str(e)}")
        finally:
            if requires_local_session:
                session.close()

    @ensure_clean_address()
    @ensure_length_limit(limit=40)
    @retry_adjust_api_call(
        retry_count=3, 
        retry_condition=lambda x: x.response is None,
        param_adjuster=adjust_address
    )
    def fetch_ruian_code(self, address: str, session: Optional[requests.Session] = None) -> ApiResponse:
        """Fetch RUIAN code data from RUIAN API for given address

        Args:
            address (str): address string
            session (requests.Session, optional): Session object for connection pooling. Defaults to None.

        Returns:
            ApiResponse: Response of API
        """

        return self.__perform_api_call(address=address, test_if_empty=lambda x: not x.polozky, api_response_object=RuianCodeApiResponse, api_details=RuianFetcher.code_api_details, session=session)
    
    @ensure_clean_address()
    @retry_adjust_api_call(
        retry_count=3, 
        retry_condition=lambda x: x.response is None,
        param_adjuster=adjust_address
    )
    def fetch_coordinates(self, address: str, session: Optional[requests.Session] = None) -> ApiResponse:
        """Fetch data about coordinates from RUIAN API for given address

        Args:
            address (str): address string
            session (requests.Session, optional): Session object for connection pooling. Defaults to None.

        Returns:
            ApiResponse: Response of API
        """

        return self.__perform_api_call(address=address, test_if_empty=lambda x: not x.candidates, api_response_object=CoordinatesAPIResponse, api_details=RuianFetcher.coor_api_details, session=session)
        
    def bulk_fetch_ruian_codes(self, addresses: Optional[Tuple[str]] = None, in_file: str = '', server: str = '', db: str = '', in_table: str = '', column_name: str = 'undefined',
                               out_file: str = '', out_table: str = '', export: bool = False) -> List[ApiResponse]:
        """Batch process multiple addresses (Request RUIAN code). 
           Either from Tuple of address strings, from excel/csv by providing paths and column name or from db
           Processed data can be exported back to 

        Args:
            addresses (Optional[Tuple[str]], optional): Tuple of address strings to be processed. Defaults to None.
            in_file (str, optional): Path to input file. Defaults to ''.
            server (str, optional): Name of server in local network. Defaults to ''.
            db (str, optional): Name of MS SQL database. Defaults to ''.
            in_table (str, optional): Name of input table. Defaults to ''.
            column_name (str, optional): Name of column where are addresses. Defaults to 'undefined'.
            out_file (str, optional): Path to output excel/csv file. Defaults to ''.
            out_table (str, optional): Name of output table. Defaults to ''.
            export (bool, optional): Whether export data into db/excel/csv. Type of export is derived from file extension. Defaults to False.

        Raises:
            Exception: If `column_name` not present in input dataframe or No data provided

        Returns:
            List[ApiResponse]: List of `ApiResponse` objects representing responses from API
        """

        data, column_name = self.__load_check_data(addresses, in_file, server, db, in_table)

        data['ruian_code'] = None
        data['code_matched_address'] = None
        data['error_msg'] = None

        responses = []
        with requests.Session() as se:
            for _, row in tqdm(data.iterrows(), total=data.shape[0], desc='Fetching ruian codes...'):
                responses.append(self.fetch_ruian_code(row[column_name], se))  

        ruians = [[k.kod for k in res.response.polozky] if res.response is not None else None for res in responses]
        matches = [[n.nazev for n in res.response.polozky] if res.response is not None else None for res in responses]

        e = [res.error_msg for res in responses]

        data['ruian_code'] = ruians
        data['code_matched_address'] = matches
        data['error_msg'] = e

        data = data.explode(["ruian_code", "code_matched_address"]).reset_index(drop=True)
        
        if export:
            self.export(data, 'auto', out_file, server, db, out_table)

        return responses

    def bulk_fetch_coordinates(self, addresses: Optional[Tuple[str]] = None, in_file: str = '', server: str = '', db: str = '', in_table: str = '', column_name: str = 'undefined',
                               out_file: str = '', out_table: str = '', export: bool = False) -> List[ApiResponse]:

        """Batch process multiple addresses (Request coordinates).
           Either from Tuple of address strings, from excel/csv by providing paths and column name or from db
           Processed data can be exported back to 

        Args:
            addresses (Optional[Tuple[str]], optional): Tuple of address strings to be processed. Defaults to None.
            in_file (str, optional): Path to input file. Defaults to ''.
            server (str, optional): Name of server in local network. Defaults to ''.
            db (str, optional): Name of MS SQL database. Defaults to ''.
            in_table (str, optional): Name of input table. Defaults to ''.
            column_name (str, optional): Name of column where are addresses. Defaults to 'undefined'.
            out_file (str, optional): Path to output excel/csv file. Defaults to ''.
            out_table (str, optional): Name of output table. Defaults to ''.
            export (bool, optional): Whether export data into db/excel/csv. Type of export is derived from file extension. Defaults to False.
            

        Returns:
            List[ApiResponse]: List of `ApiResponse` objects representing responses from API
        """

        data, column_name = self.__load_check_data(addresses, in_file, server, db, in_table)

        data['x'] = None
        data['y'] = None
        data['coor_matched_address'] = None
        data['wkid'] = None
        data['error_msg'] = None

        responses = []

        with requests.Session() as se:
            for _, row in tqdm(data.iterrows(), total=data.shape[0], desc='Fetching coordinates...'):
                responses.append(self.fetch_coordinates(row[column_name], se))  

        coor_x = [[n.location.x for n in res.response.candidates] if res.response is not None else None for res in responses]
        coor_y = [[n.location.y for n in res.response.candidates] if res.response is not None else None for res in responses]
        matches = [[n.address for n in res.response.candidates] if res.response is not None else None for res in responses]
        wkid = [[n.location.spatialReference.latestWkid for n in res.response.candidates] if res.response is not None else None for res in responses]

        e = [res.error_msg for res in responses]

        data['x'] = coor_x
        data['y'] = coor_y
        data['coor_matched_address'] = matches
        data['wkid'] = wkid
        data['error_msg'] = e

        data = data.explode(["x", "y", "coor_matched_address", "wkid"]).reset_index(drop=True)

        if export:
            self.export(data, 'auto', out_file, server, db, out_table)

        return responses
    
    
    async def __aperform_api_call(self, address: str, test_if_empty: Callable[[Union[CoordinatesAPIResponse, RuianCodeApiResponse]], bool],
                    api_response_object: Union[CoordinatesAPIResponse, RuianCodeApiResponse], api_details: Callable[[str], Tuple]) -> ApiResponse:
        """Async version of generic api call

        Args:
            address (str): address string
            test_if_empty (Callable[[Union[CoordinatesAPIResponse, RuianCodeApiResponse]], bool]): function to test if response is empty
            api_response_object (Union[CoordinatesAPIResponse, RuianCodeApiResponse]): object in which data will be encapsulated
            api_details (Callable[[str], Tuple]): static method/function to provide api call details like url, params and headers

        Returns:
            ApiResponse: Response of API
        """
        
        url, params, headers = api_details(address)

        # TODO Semaphore is useless here, should be used in bulk method
        async with asyncio.Semaphore(5):  # max 5 concurrent requests
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url=url, headers=headers, params=params) as response:
                        if response.status == 200:
                            try:
                                json_data = await response.json()
                            except aiohttp.ContentTypeError:
                                data = await response.read()
                                json_data = json.loads(data)

                            api_response = api_response_object(**json_data)

                            if test_if_empty(api_response):
                                return ApiResponse()
                            return ApiResponse(response=api_response)
                        else:
                            return ApiResponse(response=None, error_msg=f"HTTP Error {response.status}")
                except Exception as e:
                    return ApiResponse(response=None, error_msg=f"{str(e)}")
    

    @aensure_clean_address()
    @aensure_length_limit(limit=40)
    @aretry_adjust_api_call(
        retry_count=3, 
        retry_condition=lambda x: x.response is None,
        param_adjuster=adjust_address
    )
    async def afetch_ruian_code(self, address: str) -> ApiResponse:
        """Asynchronous implementation of `fetch_ruian_code` method

        Args:
            address (str): address string

        Returns:
            ApiResponse: Response of API
        """

        # TODO Semaphore is useless here, should be used in bulk method
        return await self.__aperform_api_call(address=address, test_if_empty=lambda x: not x.polozky, api_response_object=RuianCodeApiResponse, api_details=RuianFetcher.code_api_details)

    @aensure_clean_address()         
    @aretry_adjust_api_call(
        retry_count=3, 
        retry_condition=lambda x: x.response is None,
        param_adjuster=adjust_address
    )
    async def afetch_coordinates(self, address: str) -> ApiResponse:
        """Asynchronous implementation of `fetch_coordinates` method

        Args:
            address (str): address string

        Returns:
            ApiResponse: Response of API
        """

        # TODO Semaphore is useless here, should be used in bulk method
        return await self.__aperform_api_call(address=address, test_if_empty=lambda x: not x.candidates, api_response_object=CoordinatesAPIResponse, api_details=RuianFetcher.coor_api_details)


    async def abulk_fetch_ruian_codes(self, addresses: Optional[Tuple[str]] = None, in_file: str = '', server: str = '', db: str = '', in_table: str = '', column_name: str = 'undefined',
                                     out_file: str = '', out_table: str = '', export: bool = False) -> List[ApiResponse]:
        """Asynchronously batch process multiple addresses

        Args:
            addresses (Optional[Tuple[str]], optional): Tuple of address strings to be processed. Defaults to None.
            in_file (str, optional): Path to input file. Defaults to ''.
            server (str, optional): Name of server in local network. Defaults to ''.
            db (str, optional): Name of MS SQL database. Defaults to ''.
            in_table (str, optional): Name of input table. Defaults to ''.
            column_name (str, optional): Name of column where are addresses. Defaults to 'undefined'.
            out_file (str, optional): Path to output excel/csv file. Defaults to ''.
            out_table (str, optional): Name of output table. Defaults to ''.
            export (bool, optional): Whether export data into db/excel/csv. Type of export is derived from file extension. Defaults to False.

        Returns:
            List[ApiResponse]: List of `ApiResponse` objects representing responses from API
        """
        data, column_name = self.__load_check_data(addresses, in_file, server, db, in_table)
        
        
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

        if export:
            self.export(data, 'auto', out_file, server, db, out_table)

        return responses

    async def abulk_fetch_coordinates(self, addresses: Optional[Tuple[str]] = None, in_file: str = '', server: str = '', db: str = '', in_table: str = '', column_name: str = 'undefined',
                                     out_file: str = '', out_table: str = '', export: bool = False) -> List[ApiResponse]:
        """Asynchronously batch process multiple addresses

        Args:
            addresses (Optional[Tuple[str]], optional): Tuple of address strings to be processed. Defaults to None.
            in_file (str, optional): Path to input file. Defaults to ''.
            server (str, optional): Name of server in local network. Defaults to ''.
            db (str, optional): Name of MS SQL database. Defaults to ''.
            in_table (str, optional): Name of input table. Defaults to ''.
            column_name (str, optional): Name of column where are addresses. Defaults to 'undefined'.
            out_file (str, optional): Path to output excel/csv file. Defaults to ''.
            out_table (str, optional): Name of output table. Defaults to ''.
            export (bool, optional): Whether export data into db/excel/csv. Type of export is derived from file extension. Defaults to False.

        Returns:
            List[ApiResponse]: List of `ApiResponse` objects representing responses from API
        """
        data, column_name = self.__load_check_data(addresses, in_file, server, db, in_table)

        responses = []

        if export:
            self.export(data, 'auto', out_file, server, db, out_table)

        return responses


if __name__ == "__main__":
    
    r = RuianFetcher()

    # TODO implement async support properly (with Semaphore on right place)
    # TODO generate exe / dockerize it
    
    ad = (
    "Hradec Králové, 50008, Hradec Králové, Partyzánská, 11, Česká republika",
    "Letovice, Rekreační č.p. 191, PSČ 67961, Česká republika",
    "Doktora Edvarda Beneše 644/5, Slaný, 27401, Česká republika",
    "Sadová 208, Tábor - Horky, 39001, Česká republika",
    "Mechovka 1121, Praha - Klánovice, 19014, Česká republika",
    "169, Horákov, 66404, Česká republika",
    "Jungmanova 869/4, Rýmařov, 79501, Česká republika",
    "Třída Tomáše Bati 941, Otrokovice, 76502, Česká republika",
    "Vojkovská 44, Říčany, 25101, Česká republika",
    "92, Kobeřice, 79807, Česká republika"
            )
    
    r.bulk_fetch_ruian_codes(ad, out_file="out.csv", export=True)
    r.bulk_fetch_coordinates(ad, out_file="out_cc.csv", export=True)
    with requests.Session() as s:
        for a in ad:
            print(r.fetch_ruian_code(a, s))
            print(r.fetch_coordinates(a, s))
            """
            print(asyncio.run(r.afetch_coordinates("Sadová 208, Tábor - Horky, 39001, Česká republika")))
            print(asyncio.run(r.afetch_ruian_code("Sadová 208, Tábor - Horky, 39001, Česká republika")))
            print(asyncio.run(r.afetch_coordinates("Jungmanova 869/4, Rýmařov, 79501, Česká republika")))
            print(asyncio.run(r.afetch_ruian_code("Jungmanova 869/4, Rýmařov, 79501, Česká republika")))
            """
    asyncio.run(r.afetch_ruian_code("Sadová 208, Tábor - Horky, 39001, Česká republika"))
    asyncio.run(r.afetch_ruian_code("Sadová 208, Tábor - Horky, 39001, Česká republika sdadadas"))
    asyncio.run(r.afetch_ruian_code("Sadová 208, Tábor - Horky, 39001, Česká republika"))
    asyncio.run(r.afetch_coordinates("Sadová 208, Tábor - Horky, 39001, Česká republika sdadadas"))
    asyncio.run(r.afetch_ruian_code("Doktora Edvarda Beneše 644/5, Slaný, 27401, Česká republika"))
    asyncio.run(r.afetch_coordinates("SDoktora Edvarda Beneše 644/5, Slaný, 27401, Česká republika"))

    r.fetch_ruian_code("Sadová 208, Tábor - Horky, 39001, Česká republika sdadadas")
    r.fetch_ruian_code("Sadová 208, Tábor - Horky, 39001, Česká republika")
    r.fetch_coordinates("Sadová 208, Tábor - Horky, 39001, Česká republika")
    r.fetch_ruian_code("Doktora Edvarda Beneše 644/5, Slaný, 27401, Česká republika")
    r.fetch_coordinates("Doktora Edvarda Beneše 644/5, Slaný, 27401, Česká republika")
    
