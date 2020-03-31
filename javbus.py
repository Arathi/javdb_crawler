# -*- coding: utf-8 -*-
import ConfigParser
import requests
from bs4 import BeautifulSoup

class JavbusConfig:
    url = 'https://www.javbus.com'

class ProxyConfig:
    enabled = False
    host = '127.0.0.1'
    port = '1080'
    protocol = 'socks5'
    username = ''
    password = ''

class CrawlerConfig:
    javbus = None
    proxy = None
    
class JavbusCrawler:
    baseUrl = 'https://www.javbus.com'

    def __init__(self, conf):
        self.baseUrl = conf.javbus.url

    def crawlActress(self):
        return
    
    def crawlActresses(self, page):
        return
    
    def crawlTags(self):
        url = baseUrl + '/genre'
        resp = requests.get(url)
        html = resp.text
        doc = BeautifulSoup(html, 'html5lib')
        genreBoxes = doc.
        return

def loadConfig():
    # config = ConfigParser.ConfigParser()
    # config.read('crawler.ini')
    # config.get('javbus', 'url')
    # config.get('proxy', 'host')
    # config.get('proxy', 'port')
    
    crawlerConfig = CrawlerConfig()
    
    javbus = JavbusConfig()
    javbus.url = 'https://www.busdmm.one'
    
    proxy = ProxyConfig()
    proxy.enabled = False
    
    crawlerConfig.javbus = javbus
    crawlerConfig.proxy = proxy
    
    return crawlerConfig

if __name__ == '__main__':
    conf = loadConfig()
    
    
    
