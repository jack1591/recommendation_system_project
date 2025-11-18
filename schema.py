from datetime import datetime
from typing import Optional

from pydantic import BaseModel

# Модель для представления информации о пользователе
class UserGet(BaseModel):
    id: int
    age: int
    city: str
    country: str
    exp_group: int
    gender: int
    os: str
    source: str

    # чтобы напрямую использовать orm-объект
    class Config:
        orm_mode = True

# Модель для представления информации о постах
class PostGet(BaseModel):
    id: int
    text: str
    topic: str
    class Config:
        orm_mode = True

# Модель для представления информации об активностях пользователей
class FeedGet(BaseModel):
    action: str
    user_id: int
    user: UserGet
    post_id: int
    post: PostGet
    time: datetime
    class Config:
        orm_mode = True
