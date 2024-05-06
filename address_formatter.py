from abc import ABC, abstractmethod
import asyncio
import random
import re

from typing import List, Tuple, Callable


class FallbackStrategy(ABC):
    """Strategy interface"""
    @abstractmethod
    def apply_fallback(self, address: str) -> str:
        pass

class RemoveElementsRandomStrategy(FallbackStrategy):
    """
    Remove random elements of address. Elements are parts given by `split(' ')`
    """
    def apply_fallback(self, address: str) -> str:
        splitted = address.split(' ')
        if len(splitted) <= 1:
            return address
        splitted.pop(random.randint(0, len(splitted) - 1))
        return ' '.join(splitted)
    
class RemoveElementsFromLeftStrategy(FallbackStrategy):
    """
    Remove elements of address from left. Elements are parts given by `split(' ')`
    """
    def apply_fallback(self, address: str) -> str:
        splitted = address.split(' ')
        return ' '.join(splitted[1:]) if len(splitted) > 1 else address
    
class RemoveElementsFromRightStrategy(FallbackStrategy):
    """
    Remove elements of address from right. Elements are parts given by `split(' ')`
    """
    def apply_fallback(self, address: str) -> str:
        splitted = address.split(' ')
        return ' '.join(splitted[:-1]) if len(splitted) > 1 else address


class AddressFormatter:
    """
    Format address applying given FallBackStrategy
    """
    def __init__(self, fallback_strategy: FallbackStrategy):
        self.__fallback_strategy = fallback_strategy

    @property
    def fallback_strategy(self) -> FallbackStrategy:
        return self.__fallback_strategy
    
    @fallback_strategy.setter
    def fallback_strategy(self, fallback_strategy: FallbackStrategy):
        self.__fallback_strategy = FallbackStrategy


    def format_address(self, address: str) -> str:
       
        return self.cleanse(self.fallback_strategy.apply_fallback(address))
    
    @staticmethod
    def remove(address: str) -> str:
        """Removes patterns from address string

        Args:
            address (str): address string

        Returns:
            str: cleansed string
        """
        pattern_country = r",\s*(Česká republika|Česko|Czechia|Czech Republic)"
        pattern_remove = r"\s*č\.p\.|\bPSČ\b|\bpsč\b"

        return re.sub(pattern_remove, '', re.sub(pattern_country, '', address))
    
    @staticmethod
    def cleanse(address: str) -> str:
        """Cleanse address string from commas

        Args:
            address (str): address string

        Returns:
            str: cleansed string
        """
        pattern_trim = r"^\s*,|,\s*$"

        return re.sub(pattern_trim, '', address).strip()


