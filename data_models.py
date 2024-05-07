from pydantic import BaseModel
from typing import List, Optional

class RuianCodeItem(BaseModel):
    kod: int
    nazev: str
    
class RuianCodeApiResponse(BaseModel):
    polozky: List[RuianCodeItem]
    existujiDalsiPolozky: bool

class SpatialReference(BaseModel):
    wkid: int
    latestWkid: int

class Location(BaseModel):
    x: float
    y: float
    spatialReference: SpatialReference

class Attributes(BaseModel):
    Addr_type: str
    Loc_name: str
    Type: str
    City: str
    Country: str
    Match_addr: str
    Score: int

class Candidate(BaseModel):
    address: str
    location: Location
    score: int
    attributes: Attributes

class CoordinatesAPIResponse(BaseModel):
    spatialReference: SpatialReference
    candidates: List[Candidate]

class ApiResponse(BaseModel):
    response: Optional[CoordinatesAPIResponse | RuianCodeApiResponse] = None
    error_msg: Optional[str] = None
