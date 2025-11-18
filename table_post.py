from database import Base, SessionLocal
from sqlalchemy import Column, Integer, String

class Post(Base):
    __tablename__ = "post"
    id = Column(Integer, primary_key = True)
    topic = Column(String)
    text = Column(String)

