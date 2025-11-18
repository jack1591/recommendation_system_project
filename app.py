from datetime import datetime
from http.client import HTTPException
from typing import List
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import os
from catboost import CatBoostClassifier
import pandas as pd
from sqlalchemy import create_engine, desc
from loguru import logger

from database import SessionLocal
from schema import PostGet, UserGet, FeedGet
from table_feed import Feed
from table_post import Post
from table_user import User

# создаем генератор подключения к БД
engine = create_engine(
    "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
    "postgres.lab.karpov.courses:6432/startml"
)

# Получение пути, по которому находится загруженная на сервер модель
def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH

# Загрузка модели от сервера
def load_models():
    model_path = get_model_path("/my/super/catboost_model")
    from_file = CatBoostClassifier()
    model = from_file.load_model(model_path, format = 'cbm')
    return model

# загрузка таблиц с информацией о пользователях, постах
# и активностях пользователей относительно постов
def load_features():
    # таблица с информацией о просмотрах, лайках пользователей по постам
    logger.info('loading liked posts')
    liked_posts_query = (
        """
        SELECT distinct post_id, user_id 
        FROM public.feed_data
        where action = 'like'
        limit 100000
        """
    )
    liked_posts = batch_load_sql(liked_posts_query)

    # таблица с постами
    logger.info('loading posts features')
    posts_features = pd.read_sql(
        """SELECT * FROM public.bezyazychnyy_a_features_lesson_22""",
        con = engine
    )

    # таблица с пользователями
    logger.info('loading user features')
    user_features = pd.read_sql(
        """SELECT * FROM public.user_data""",
         con = engine
    )

    return [liked_posts, posts_features, user_features]

# загрузка таблицы по частям
def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    conn = engine.connect().execution_options(stream_results=True)
    # загружаем информацию по чанкам, потом их объединим
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)

# загрузка модели из текущей директории (для проверки локально)
def load_model_2():
    model_path = "catboost_model"
    from_file = CatBoostClassifier()
    model = from_file.load_model(model_path, format='cbm')
    return model

logger.info('loading model')
#model = load_models()
model = load_model_2()
logger.info('loading features')
features = load_features()
logger.info('server is running')


# создаем экземпляр fastapi
app = FastAPI()

def get_db():
    with SessionLocal() as db:
        return db


# Получение топ-limit рекомендация для конкретного пользователя
def get_top_recommendations(id:int, time:datetime, limit:int):
    logger.info('reading user features')
    user_features = features[2].loc[features[2].user_id == id]
    user_features = user_features.drop('user_id', axis = 1)

    logger.info('reading posts features')
    posts_features = features[1].drop(['index', 'text'], axis = 1)
    content = features[1][['post_id', 'text', 'topic']]

    logger.info('zipping everything')
    add_user_features = dict(zip(user_features.columns, user_features.values[0]))
    logger.info('assigning everything')
    user_posts_features = posts_features.assign(**add_user_features)
    user_posts_features = user_posts_features.set_index('post_id')

    logger.info('adding time info')
    user_posts_features['hour'] = time.hour
    user_posts_features['day'] = time.day
    user_posts_features['month'] = time.month
    logger.info(f"{user_posts_features.columns}")
    logger.info('predicting')
    logger.info(f"{model.feature_names_}")
    predictions = model.predict_proba(user_posts_features)[:, 1]
    user_posts_features['predicts'] = predictions

    #уберем записи, где пользователь ранее уже ставил лайк
    logger.info('deleting liked posts')
    liked_posts = features[0]
    liked_posts = liked_posts[liked_posts['user_id'] == id].post_id.values
    filtered_ = user_posts_features[~user_posts_features.index.isin(liked_posts)]

    # Выберем топ-limit постов
    recommended_posts = filtered_.sort_values('predicts')[-limit:].index

    return [
        PostGet(
            **{
                'id': i,
                'text': content[content.post_id == i].text.values[0],
                'topic': content[content.post_id == i].topic.values[0],
            }
        ) for i in recommended_posts
    ]

# эндпоинт для получения топ-limit рекомендаций для пользователя
@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(
		id: int,
		time: datetime,
		limit: int = 10) -> List[PostGet]:
     return get_top_recommendations(id, time, limit)

# Получение информации о limit пользователей (из таблицы user)
@app.get("/users/{limit}", response_model=List[UserGet])
def first_users(limit: int = 10, db: Session = Depends(get_db)) -> List[UserGet]:
    result = (db.query(User).limit(limit).all())
    if not result:
        raise HTTPException(404)
    return result

# Получение информации о пользователе (из таблицы user)
@app.get("/user/{id}", response_model=UserGet)
def get_user(id: int, db: Session = Depends(get_db))->UserGet:
    result = db.query(User).filter(User.id==id).first()
    if not result:
        raise HTTPException(404, "user not found!")
    return result

# Получение информации о посте (из таблицы post)
@app.get("/post/{id}", response_model = PostGet)
def get_post(id: int, db: Session = Depends(get_db)) -> PostGet:
    result = db.query(Post).filter(Post.id==id).first()
    if not result:
        raise HTTPException(404, "post not found!")
    return result

# Получение информации по активностям конкретного пользователя (из таблицы feed_action)
@app.get("/user/{id}/feed", response_model = List[FeedGet])
def get_user_feed(id: int, limit: int = 10, db: Session = Depends(get_db)) -> List[FeedGet]:
    result = (db.query(Feed)
              .filter(Feed.user_id == id)
              .order_by(desc(Feed.time))
              .limit(limit)
              .all())
    if not result:
        raise HTTPException(200, [])
    return result

# Получение информации об активностях пользователей по конкретному посту (из таблицы feed_action)
@app.get("/post/{id}/feed", response_model = List[FeedGet])
def get_user_feed(id: int, limit: int = 10, db: Session = Depends(get_db)) -> List[FeedGet]:
    result = (db.query(Feed)
              .filter(Feed.post_id == id)
              .order_by(desc(Feed.time))
              .limit(limit)
              .all())
    if not result:
        raise HTTPException(200, [])
    return result

