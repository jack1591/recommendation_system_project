from datetime import datetime
from typing import List
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from catboost import CatBoostClassifier
import pandas as pd
from sqlalchemy import create_engine
from loguru import logger

class PostGet(BaseModel):
    id: int
    text: str
    topic: str
    class Config:
        orm_mode = True


SQLALCHEMY_DATABASE_URL = "postgresql://robot-startml-ro:pheiph0hahj1Vaif@postgres.lab.karpov.courses:6432/startml"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

engine = create_engine(
    "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
    "postgres.lab.karpov.courses:6432/startml"
)

def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH

def load_models():
    model_path = get_model_path("/my/super/catboost_model")
    from_file = CatBoostClassifier()
    model = from_file.load_model(model_path, format = 'cbm')
    return model


def load_features():
    logger.info('loading liked posts')
    liked_posts_query = (
        """
        SELECT distinct post_id, user_id 
        FROM public.feed_data
        where action = 'like'
        """
    )
    liked_posts = batch_load_sql(liked_posts_query)

    logger.info('loading posts features')
    posts_features = pd.read_sql(
        """SELECT * FROM public.bezyazychnyy_a_features_lesson_22""",
        con = engine
    )

    logger.info('loading user features')
    user_features = pd.read_sql(
        """SELECT * FROM public.user_data""",
         con = engine
    )

    return [liked_posts, posts_features, user_features]

def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)

# Это загрузка модели с сервера karpov.courses - выполняется при проверке модели
#model = load_models()

# я использую загрузку модели локально
def load_model_2():
    model_path = "catboost_model"
    from_file = CatBoostClassifier()
    model = from_file.load_model(model_path, format='cbm')
    return model

logger.info('loading model')
model = load_model_2() # загрузили модель из текущей директории
logger.info('loading features')
features = load_features()
logger.info('server is running')

app = FastAPI()

def get_db():
    with SessionLocal() as db:
        return db


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

@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(
		id: int,
		time: datetime,
		limit: int = 10) -> List[PostGet]:
     return get_top_recommendations(id, time, limit)