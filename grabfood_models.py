from pydantic import BaseModel
from typing import List, Optional,Any

class Offer(BaseModel):
    Title: Optional[str] = None
    SubTitle: Optional[str] = None


class MenuItem(BaseModel):
    item_id: Optional[str]
    name: Optional[str]
    description: Optional[str]
    price_display: Optional[float]
    available: bool
    images: Optional[str]=None


class Category(BaseModel):
    category_name: Optional[str]
    items: List[MenuItem] = []


class Restaurant(BaseModel):
    merchant_id: Optional[str]
    name: str|None
    cuisine: Optional[str]=None
    timingEveryday: Optional[str]=None
    distance: Optional[float]=None
    ETA: Optional[int]=None
    rating: Optional[float]=None
    DeliveryBy: Optional[str]=None
    DeliveryOption: Optional[List[str]] = []
    VoteCount: Optional[int]=None
    Tips: Optional[List[str|None]]
    BuisinessType: Optional[str]=None
    Offers: Optional[List[Offer]] = []
    menu: Optional[List[Category]] = []