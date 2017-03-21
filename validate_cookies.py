#-*- coding: utf-8 -*-
import re
import os
import sys
import json
import requests
import traceback
from pprint import pprint
from datetime import datetime as dt
from bs4 import BeautifulSoup as bs
from zhihu_writer import ZHIHU_CURLS, ZHIHU_API_CURLS
from zc_spider.weibo_utils import resolve_curl
from zc_spider.weibo_config import (
    QCLOUD_MYSQL, OUTER_MYSQL,
    LOCAL_REDIS, QCLOUD_REDIS
)
reload(sys)
sys.setdefaultencoding('utf-8')

test_url = "https://www.zhihu.com/question/28066901"


def process_url(url, curl):
     """
     :param(str) url: like this : https://www.zhihu.com/question/19583984
     """
     ret = []
     # print "Parsing : ", url
     now_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
     _, header, post_data = resolve_curl(curl)
     # import ipdb; ipdb.set_trace()
     r = requests.get(url, headers=header, data=post_data, timeout=20)
     if r.status_code != 200:
        raise Exception("Response(%s) HTTP Code(%d)" % (url, r.status_code))
     try:
          parser = bs(r.text, 'html.parser')
          data_div = parser.find('div', attrs={'id': 'data'})
          data = json.loads(data_div['data-state'])
          print "$"*20, "availiable ..."
     except Exception as e:
          traceback.print_exc()
          return ret
     # get next url apiV4 url
     ques_id = url.replace('https://www.zhihu.com/question/', '')
     if data['question']['answers'][ques_id]['isDrained'] == False:
          next_url = data['question']['answers'][ques_id]['next'].replace('http://www-nweb-api-qa', 
               'https://www.zhihu.com/api/v4')  # api v4 url has "questions"
          # print "Next url: ", next_url
     else:
          print "It really Drained ..."
     answer_list = data['entities']['answers']
     print " DONE !!! There are %s answers ..." % len(answer_list)
     return next_url

def process_xhr(url, curl):
     ret = []
     # print "Parsing : ", url
     now_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
     _, header, post_data = resolve_curl(curl)
     # import ipdb; ipdb.set_trace()
     r = requests.get(url, headers=header, data=post_data, timeout=20)
     if r.status_code != 200:
        raise Exception("Response(%s) HTTP Code(%d)" % (url, r.status_code))
     try:
          data = json.loads(r.text)
          print "$"*20, "availiable ..."
     except ValueError as e:
          print str(e), " --> ", r.text
          return ret
     if data['paging']['is_end'] == True:
          print "No more answers ..."
     else:
          next_url = data['paging']['next'].replace('http://www-nweb-api-qa', 
               'https://www.zhihu.com/api/v4')  # api v4 url has "questions"
          # print "Next url : ", next_url
     answer_list =  data['data']
     print "*"*20, " DONE !!! There are %s answers ..." % len(answer_list)

for account, xhr_acc in zip(ZHIHU_CURLS, ZHIHU_API_CURLS):
    print "Using {0} and {1}".format(account, xhr_acc)
    next_url = process_url(test_url, ZHIHU_CURLS[account])
    process_xhr(next_url, ZHIHU_API_CURLS[xhr_acc])
    print "\n"