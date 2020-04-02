# -*- coding: utf-8 -*-
import logging
import time
from datetime import datetime
import re
import os

import requests
from bs4 import BeautifulSoup

from sqlalchemy import Column, String, Integer, Date, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# config
baseUrl = 'https://www.busdmm.one'
parser = 'html.parser'  # 'html5lib'

# logging
ts = int(round(time.time() * 1000))
logfile_name = 'crawler-{}.log'.format(ts)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('logs/' + logfile_name, encoding='utf-8')
fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.INFO)

logger = logging.getLogger('crawler')
logger.addHandler(fh)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

# region Models
BaseModel = declarative_base()


class Tag(BaseModel):
    __tablename__ = 'tags'
    id = Column(String(8), primary_key=True)
    category = Column(String(8))
    name = Column(String(256))

    def __str__(self):
        return '{}|{}|{}'.format(self.id, self.category, self.name)


class Actress(BaseModel):
    __tablename__ = 'actresses'
    id = Column(String(8), primary_key=True)
    name = Column(String(256))
    name_cn = Column(String(256))

    def __str__(self):
        return '{}|{}|{}'.format(self.id, self.name, self.name_cn)


class Director(BaseModel):
    __tablename__ = 'directors'
    id = Column(String(8), primary_key=True)
    name = Column(String(256))


class Studio(BaseModel):
    __tablename__ = 'studios'
    id = Column(String(8), primary_key=True)
    name = Column(String(256))


class Publisher(BaseModel):
    __tablename__ = 'publishers'
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
    series = Column(String(8))
    cover = Column(String(64))
    thumb = Column(String(64))

    def __str__(self):
        return '{}|{}'.format(self.id, self.name)


class MovieActress(BaseModel):
    __tablename__ = 'movie_actress'
    bango = Column(String(32), primary_key=True)
    actress_id = Column(String(8), primary_key=True)


class MovieTag(BaseModel):
    __tablename__ = 'movie_tags'
    bango = Column(String(32), primary_key=True)
    tag_id = Column(String(8), primary_key=True)
# endregion


# db
engine = create_engine('sqlite:///./javbus.db3')
DBSession = sessionmaker(bind=engine)
BaseModel.metadata.create_all(engine)


class JavbusCrawler:
    base_url = None
    cache_path = None

    def __init__(self, base_url='https://www.busdmm.one', cache_path=None):
        self.base_url = base_url
        self.cache_path = cache_path
        if self.cache_path is None:
            current_path = os.path.abspath('.')
            self.cache_path = current_path + "/cache"

    def get_request(self, uri):
        category = 'others'
        filename = None

        # 分类
        if uri == '/genre':
            category = 'others'
            filename = 'tags.html'

        # 演员
        m = re.match(r'/star/(\w+)/(\d+)?', uri)
        if m is not None:
            category = 'actresses'
            actress_id = m.group(1)
            page_id = m.group(2)
            if page_id is None:
                page_id = 1
            filename = '{}_{}.html'.format(actress_id, page_id)

        # 番号
        m = re.match(r'/([0-9A-Z]{1,5}-[0-9]{1,5})', uri)
        if m is not None:
            category = 'movies'
            bango = m.group(1)
            filename = '{}.html'.format(bango)

        if filename is None:
            filename = hash(uri)

        # 演员列表
        m = re.match(r'/actresses/(\d+)', uri)
        if m is not None:
            category = 'actresses'
            page_id = m.group(1)
            if page_id is None:
                page_id = 1
            filename = '{}_{}.html'.format('index', page_id)

        cache_dir = '{}/{}'.format(self.cache_path, category)
        os.makedirs(cache_dir, exist_ok=True)

        file_path = '{}/{}'.format(cache_dir, filename)
        if os.path.exists(file_path):
            logger.info('从缓存加载' + uri)
            fp = open(file_path, 'r', encoding='utf-8')
            html = fp.read()
            fp.close()
        else:
            url = '{}{}'.format(self.base_url, uri)
            logger.info('正在下载' + url)
            resp = requests.get(url)
            html = resp.text
            fp = open(file_path, 'w', encoding='utf-8')
            fp.write(html)
            fp.close()
        return html

    def fetch_tags(self):
        html = self.get_request('/genre')
        doc = BeautifulSoup(html, parser)
        nodes = doc.find_all("div", "genre-box")
        tags = []
        for node in nodes:
            category = node.previous_element.previous_element
            links = node.find_all("a")
            for a in links:
                href = a['href']
                t = href.rindex('/')
                id = href[t+1:]
                name = a.text
                tag = Tag(id=id, category=category, name=name)
                tags.append(tag)

        return tags

    def fetch_actress_page(self, act_id, page_id=1):
        uri = '/star/{}/{}'.format(act_id, page_id)
        html = self.get_request(uri)
        doc = BeautifulSoup(html, parser)
        movie_boxes = doc.select('div#waterfall a.movie-box')
        next_page_link = doc.select_one('a#next')
        if next_page_link is None:
            next_page = 0
        else:
            href = next_page_link.attrs['href']
            t = href.rindex('/')
            next_page = href[t+1:]
        movies = []
        for box in movie_boxes:
            thumb_url = box.find_next('img').attrs['src']
            t = thumb_url.rindex('/')
            thumb = thumb_url[t+1:]
            movie_url = box.attrs['href']
            t = movie_url.rindex('/')
            bango = movie_url[t+1:]
            movie, movie_actresses, movie_tags, names = self.fetch_movie(bango, thumb)
            movies.append(movie)
        return movies, next_page

    def fetch_movie(self, bango, thumb=None):
        url = '/{}'.format(bango)
        html = self.get_request(url)

        doc = BeautifulSoup(html, parser)
        info_nodes = doc.select('div.info span.header')
        cover_node = doc.select_one('div.screencap img')

        if cover_node is None:
            logger.warning('封面节点获取失败')

        movie = Movie()
        movie.id = bango
        movie.thumb = thumb

        cover_link = cover_node.attrs['src']
        t = cover_link.rindex('/')
        movie.cover = cover_link[t+1:]
        movie.name = cover_node.attrs['title']

        for info_node in info_nodes:
            if info_node.text == '發行日期:':
                release_at = info_node.next_sibling
                movie.release_at = datetime.strptime(release_at.strip(), '%Y-%m-%d')
            if info_node.text == '長度:':
                length_str = info_node.next_sibling
                m = re.match(r'\s*(\d+).*', length_str)
                movie.length = m.group(1)
            if info_node.text == '導演:':
                link = info_node.next_sibling.next_sibling.attrs['href']
                t = link.rindex('/')
                movie.director = link[t+1:]
            if info_node.text == '製作商:':
                link = info_node.next_sibling.next_sibling.attrs['href']
                t = link.rindex('/')
                movie.studio = link[t+1:]
            if info_node.text == '發行商:':
                link = info_node.next_sibling.next_sibling.attrs['href']
                t = link.rindex('/')
                movie.publisher = link[t+1:]
            if info_node.text == '系列:':
                link = info_node.next_sibling.next_sibling.attrs['href']
                t = link.rindex('/')
                movie.series = link[t+1:]

        movie_actresses = []
        movie_tags = []
        names = {}
        return movie, movie_actresses, movie_tags, names

    def fetch_actress_list(self, page):
        url = '/actresses/{}'.format(page)
        html = self.get_request(url)

        doc = BeautifulSoup(html, parser)
        act_nodes = doc.select('div#waterfall div.photo-frame img')
        actresses = []
        for act_node in act_nodes:
            link = act_node.parent.parent
            href = link.attrs['href']
            t = href.rindex('/')
            act_id = href[t+1:]
            actress = Actress()
            actress.id = act_id
            actress.name = act_node.attrs['title']
            actresses.append(actress)
        return actresses


def save_tags(tags):
    session = DBSession()

    for tag in tags:
        tc = session.query(Tag).filter_by(id=tag.id).count()
        if tc == 0:
            session.add(tag)
            logger.info("标签`{}`入库成功".format(tag))
        else:
            logger.debug("标签ID`{}`已存在".format(tag.id))
    session.commit()
    session.close()


def save_movie(movie):
    session = DBSession()

    count = session.query(Movie).filter_by(id=movie.id).count()
    if count == 0:
        session.add(movie)
        logger.info("影片信息`{}`入库成功".format(movie.id))
    else:
        logger.debug("影片信息`{}`已存在".format(movie.id))

    session.commit()
    session.close()


if __name__ == '__main__':
    logger.info("javbus crawler v0.1.0")
    logger.debug("javbus爬虫启动")
    crawler = JavbusCrawler()
    for i in range(1, 11):
        actresses = crawler.fetch_actress_list(1)
        for actress in actresses:
            page = 1
            while page > 0:
                movies, page = crawler.fetch_actress_page(actress.id, page)
                for movie in movies:
                    save_movie(movie)
                page = int(page)
