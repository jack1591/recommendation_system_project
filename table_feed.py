from sqlalchemy.orm import relationship

from database import Base
from sqlalchemy import DateTime, Column, String, Integer, ForeignKey
from table_post import Post
from table_user import User

class Feed(Base):
    __tablename__ = "feed_action"
    action = Column(String)
    user_id = Column(Integer, ForeignKey("user.id"), primary_key = True)
    user = relationship(User)
    post_id = Column(Integer, ForeignKey("post.id"), primary_key = True)
    post = relationship(Post)
    time = Column(DateTime)

