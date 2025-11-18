from sqlalchemy import Column, Integer, String, func, desc

from database import Base, SessionLocal


class User(Base):
    __tablename__='user'
    id = Column(Integer, primary_key = True)
    age = Column(Integer)
    city = Column(String)
    country = Column(String)
    exp_group = Column(Integer)
    gender = Column(Integer)
    os = Column(String)
    source = Column(String)

