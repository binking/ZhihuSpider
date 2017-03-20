#-*- coding: utf-8 -*-
import re
import os
import sys
import json
import requests
from pprint import pprint
from datetime import datetime as dt
from bs4 import BeautifulSoup as bs
from zhihu_writer import ZHIHU_CURL, ZHIHU_API_CURL
from zc_spider.weibo_utils import retry, gen_abuyun_proxy, resolve_curl
reload(sys)
sys.setdefaultencoding('utf-8')

if os.environ.get('SPIDER_ENV') == 'test':
    print "*"*10, "Run in Test environment"
    USED_REDIS = LOCAL_REDIS
    USED_DATABASE = OUTER_MYSQL
elif 'centos' in os.environ.get('HOSTNAME'): 
    print "*"*10, "Run in Qcloud environment"
    USED_REDIS = QCLOUD_REDIS
    USED_DATABASE = QCLOUD_MYSQL
else:
    raise Exception("Unknown Environment, Check it now...")

for curl, api_curl in zip(ZHIHU_CURL, ZHIHU_API_CURL):
    _, header, post_data = resolve_curl(ZHIHU_CURL)
    # import ipdb; ipdb.set_trace()
    r = requests.get(url, headers=header, data=post_data, timeout=20)
    if r.status_code != 200:
        raise Exception("Response(%s) HTTP Code(%d)" % (url, r.status_code))
    else:
        print r.text