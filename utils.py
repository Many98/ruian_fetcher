from typing import Any, List, Tuple, Callable, Optional

def retry_api_call(func: Callable) -> Callable:
    """utility decorator for multiple api call retries

    Args:
        func (Callable): Method of RuianFetcher which returns ApiResponse 

    Returns:
        Callable: decorated method
    """
    def wrapper(self, address: str, *args, **kwargs):

        for _ in range(3):
            
            api_response = func(self, address, *args, **kwargs)
            
            if api_response.response is not None:
                break 
            address = self.address_formatter.format_address(address)
            
        return api_response

    return wrapper

def ensure_length_limit(func: Callable) -> Callable:
    """utility decorator to ensure that lenght of address string is less than 40 chars 

    Args:
        func (Callable): Method of RuianFetcher which returns ApiResponse 

    Returns:
        Callable: decorated method
    """
    def wrapper(self, address: str, *args, **kwargs):
        
        if len(address) > 40:
            address = self.address_formatter.cleanse(address)
        while len(address) > 40:
            address = self.address_formatter.format_address(address)
        
        return func(self, address, *args, **kwargs)
    return wrapper