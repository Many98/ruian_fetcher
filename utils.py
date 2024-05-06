from typing import Any, List, Tuple, Callable, Optional, Dict

# TODO consider tenacity module for more complex retry logic

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

def retry_adjust_api_call(retry_count: int = 3,
                    retry_condition: Optional[Callable[[Any], bool]] = None,
                    param_adjuster: Optional[Callable[..., Tuple[List, Dict]]] = None
                    ):
    """utility decorator for multiple api call retries

    Args:
        retry_count (int, optional): Retry count. Defaults to 3.
        retry_condition (Callable[[Any], bool], optional): Function that takes the result of the function call and 
            returns a boolean indicating whether to retry the call. Defaults to None.
        param_adjuster (Callable[..., Tuple[List, Dict]], optional): Function that adjusts the arguments
            for the next retry attempt. It should return a tuple containing the new arguments list and 
            kwargs dictionary. Defaults to None.
    """
    def decorator(func: Callable):
        def wrapper(self, *args, **kwargs):
            mutable_args = list(args)
            response = None
            last_exception = None
            for attempt in range(retry_count):
                try:
                    response = func(self, *mutable_args, **kwargs)
                    if retry_condition is not None and retry_condition(response):
                        if param_adjuster is not None:
                            mutable_args, kwargs = param_adjuster(self, *mutable_args, **kwargs)
                        continue
                    return response
                except Exception as e:
                    last_exception = e
                    if param_adjuster:
                        mutable_args, kwargs = param_adjuster(self, *mutable_args, **kwargs)
            if response is not None:
                return response
            raise last_exception
        return wrapper
    return decorator

def ensure_length_limit(func: Callable) -> Callable:
    """utility decorator to ensure that lenght of address string is less than 40 chars 

    Args:
        func (Callable): Method of RuianFetcher which returns ApiResponse 

    Returns:
        Callable: decorated method
    """
    def wrapper(self, address: str, *args, **kwargs):
        
        address = self.address_formatter.remove(address)
        address = self.address_formatter.cleanse(address)

        while len(address) > 40:
            address = self.address_formatter.format_address(address)
        
        return func(self, address, *args, **kwargs)
    return wrapper


def aretry_adjust_api_call(retry_count: int = 3,
                    retry_condition: Optional[Callable[[Any], bool]] = None,
                    param_adjuster: Optional[Callable[..., Tuple[List, Dict]]] = None
                    ):
    """Async utility decorator for multiple api call retries

    Args:
        retry_count (int, optional): Retry count. Defaults to 3.
        retry_condition (Callable[[Any], bool], optional): Function that takes the result of the function call and 
            returns a boolean indicating whether to retry the call. Defaults to None.
        param_adjuster (Callable[..., Tuple[List, Dict]], optional): Function that adjusts the arguments
            for the next retry attempt. It should return a tuple containing the new arguments list and 
            kwargs dictionary. Defaults to None.
    """
    def decorator(func: Callable):
        async def wrapper(self, *args, **kwargs):
            mutable_args = list(args)
            response = None
            last_exception = None
            for attempt in range(retry_count):
                try:
                    response = await func(self, *mutable_args, **kwargs)
                    if retry_condition is not None and retry_condition(response):
                        if param_adjuster is not None:
                            mutable_args, kwargs = param_adjuster(self, *mutable_args, **kwargs)
                        continue
                    return response
                except Exception as e:
                    last_exception = e
                    if param_adjuster:
                        mutable_args, kwargs = param_adjuster(self, *mutable_args, **kwargs)
            if response is not None:
                return response
            raise last_exception
        return wrapper
    return decorator

def aensure_length_limit(func: Callable) -> Callable:
    """Async utility decorator to ensure that lenght of address string is less than 40 chars 

    Args:
        func (Callable): Method of RuianFetcher which returns ApiResponse 

    Returns:
        Callable: decorated method
    """
    async def wrapper(self, address: str, *args, **kwargs):
        
        if len(address) > 40:
            address = self.address_formatter.cleanse(address)
        while len(address) > 40:
            address = self.address_formatter.format_address(address)
        
        return await func(self, address, *args, **kwargs)
    return wrapper