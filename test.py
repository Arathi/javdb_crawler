# -*- coding: utf-8 -*-
import logging
import requests
from bs4 import BeautifulSoup

from sqlalchemy import Column, String, Integer, Date, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# config
baseUrl = 'https://www.busdmm.one'
parser = 'html.parser'  # 'html5lib'

# logging
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('crawler.log', encoding='utf-8')
fh.setFormatter(formatter)

logger = logging.getLogger('crawler')
logger.setLevel(logging.DEBUG)
logger.addHandler(fh)

# models
BaseModel = declarative_base()


class Tag(BaseModel):
    __tablename__ = 'tags'
    id = Column(String(8), primary_key=True)
    category = Column(String(8))
    name = Column(String(256))


class Actress(BaseModel):
    __tablename__ = 'actresses'
    id = Column(String(8), primary_key=True)
    name = Column(String(256))


class Movie(BaseModel):
    __tablename__ = 'movies'
    id = Column(String(32), primary_key=True)
    name = Column(String(256))
    release_at = Column(Date)
    length = Column(Integer)
    director = Column(String(8))
    studio = Column(String(8))
    publisher = Column(String(8))
    cover = Column(String(64))


class MovieActress(BaseModel):
    __tablename__ = 'movie_actress'
    bango = Column(String(32), primary_key=True)
    actress_id = Column(String(8), primary_key=True)


class MovieTag(BaseModel):
    __tablename__ = 'movie_tags'
    bango = Column(String(32), primary_key=True)
    tag_id = Column(String(8), primary_key=True)


# db
engine = create_engine('sqlite:///./javbus.db3')
DBSession = sessionmaker(bind=engine)
BaseModel.metadata.create_all(engine)


# 获取标签
def fetch_tags():
    url = baseUrl + '/genre'

    logger.info('正在获取' + url)
    resp = requests.get(url)
    html = resp.text

    logger.info('正在解析分类页面')
    doc = BeautifulSoup(html, parser)
    nodes = doc.find_all("div", "genre-box")

    logger.info('获取分类如下：')
    session = DBSession()
    for node in nodes:
        h4 = node.previous_element.previous_element
        category_name = h4

        links = node.find_all("a")
        for a in links:
            href = a['href']
            idx = href.rindex('/')
            genreId = href[idx+1:]
            genreName = a.text
            logger.info(category_name + " > " + genreId + " " + genreName)
            tag = Tag(id=genreId, category=category_name, name=genreName)
            session.add(tag)

    session.commit()
    session.close()


def fetch_movie(bango):
    url = baseUrl + '/' + bango

    logger.info('正在获取' + url)
    resp = requests.get(url)
    html = resp.text
    doc = BeautifulSoup(html, parser)
    movie = Movie()
    movie.id = bango

    session = DBSession()

    session.commit()
    session.close()


if __name__ == '__main__':
    fetch_tags()
