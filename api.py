from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import Callable, List, Tuple

from ruian import RuianFetcher


app = FastAPI()

r = RuianFetcher()

# https://fastapi.tiangolo.com/tutorial/background-tasks/


@app.get("/")
def root():
    return {"RUIAN API WRAPPER": "Obtain ruian codes and coordinates for given address/ batch of addresses",
            "API Docs": "http://127.0.0.1:8000/docs"
    }

@app.get("/ruian/coordinates")
@app.get("/ruian/coordinates/{address}")
def get_coordinates(address: str = None):
    """
    Obtain coordinates for given address
    """
    pass

@app.get("/ruian/code")
@app.get("/ruian/code/{address}")
def get_coordinates(address: str = None):
    """
    Obtain ruian code for given address
    """
    pass

@app.get("/ruian/info")
@app.get("/ruian/info/{address}")
def get_coordinates(address: str = None):
    """
    Obtain ruian code & coordinates for given address
    """
    pass




