from pydantic import BaseModel
from typing import List, Optional

class RuianCodeItem(BaseModel):
    kod: Optional[int]
    nazev: Optional[str]

class RuianCodeApiResponse(BaseModel):
    polozky: List[RuianCodeItem | None]
    existujiDalsiPolozky: bool
    error_msg: Optional[str] = None

class SpatialReference(BaseModel):
    wkid: int
    latestWkid: int

class Location(BaseModel):
    x: Optional[float]
    y: Optional[float]
    spatialReference: Optional[SpatialReference]

class Attributes(BaseModel):
    Addr_type: Optional[str]
    Loc_name: Optional[str]
    Type: Optional[str]
    City: Optional[str]
    Country: Optional[str]
    Match_addr: Optional[str]
    Score: Optional[int]

class Candidate(BaseModel):
    address: Optional[str]
    location: Optional[Location]
    score: Optional[int]
    attributes: Optional[Attributes]

class CoordinatesAPIResponse(BaseModel):
    spatialReference: SpatialReference
    candidates: List[Candidate | None]
    error_msg: Optional[str]