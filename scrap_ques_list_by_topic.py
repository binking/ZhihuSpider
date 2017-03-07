#-*- coding: utf-8 -*-
import re
import os
import sys
import time
import json
import requests
import pickle
import random
import urllib
import traceback
import multiprocessing as mp
from redis import StrictRedis
from datetime import date, timedelta
from urlparse import urlparse, parse_qs
from datetime import datetime as dt
from bs4 import BeautifulSoup as bs
from zhihu_writer import ZhihuTopicWriter
from zc_spider.weibo_config import (
    QCLOUD_MYSQL, OUTER_MYSQL,
    LOCAL_REDIS, QCLOUD_REDIS
)
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

ZHIHU_CURL = """curl 'https://www.zhihu.com/topic/19828558/top-answers?page=1' \
-H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;\
q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 \
(Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 \
Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,\
image/webp,*/*;q=0.8' -H 'Cookie: aliyungf_tc=AQAAAOltWDH8ew0Axjlk05aQBNC6UGPP; \
q_c1=34bdc85fdb4c416183480179f2c403ed|1488854789000|1488854789000; \
_xsrf=000b609b5646b246678113a4639aba45; cap_id="YWMzZTdiMGM3NjM0NGQzZjhjYWUyNjZmN\
TU2NTE0MGE=|1488854789|6239128d3ff39e178ab3b94f083101c7e10d2002"; l_cap_id="Njk4ZmU\
wOTliNzcxNDZiNDk3MDAyMjMyOWE0MmQ5YTU=|1488854789|51597f9f49d4050e7c2e2e89db34917f4\
2b4a63d"; d_c0="AGBCKfq9aQuPTpMXpKcohGO7Zy2eCCIcTac=|1488854793"; _zap=d9c6e5e4-8\
ea4-44e2-9309-a7af8a50ddf2; auth_type="c2luYQ==|1488854826|432b302abb3327df3e63a5e68\
ad4d1561ba26ffb"; token="Mi4wMENtYUpjR0VBNzIyRGJkYzFmZDRiMjUzeVRJM0Q=|1488854826|4dd\
fb5a0c77971d942748187b1066c3449938b0b"; client_id="NjA2MDU4NDA3NA==|1488854826|74edd\
90d3ec2c40c7cf96be7be0e6901220e3a55"; s-q=%E4%B9%9D%E9%BE%99%E6%B2%BB%E6%B0%B4; s-i=\
2; sid=asbhd4eo; z_c0=Mi4wQUhBQzJ5ZS1hUXNBWUVJcC1yMXBDeGNBQUFCaEFsVk5ONnpsV0FEa3ZKZn\
hUQzBSbXVMRUo0QWctUjduVVJTejRB|1488856756|facc84b1930eecb478aa62325b80abd2439f52c7; \
nweb_qa=heifetz; __utma=51854390.1659697083.1488854807.1488854807.1488854807.1; __ut\
mb=51854390.0.10.1488854807; __utmc=51854390; __utmz=51854390.1488854807.1.1.utmcsr\
=(direct)|utmccn=(direct)|utmcmd=(none); __utmv=51854390.100--|2=registration_date=\
20170307=1^3=entry_date=20170307=1' -H 'Connection: keep-alive' --compressed"""
TOPIC_URL = "https://www.zhihu.com/topic/{topic_id}/top-answers?page={page}"
ZHIHU_DOMAIN = "https://www.zhihu.com"
ZHIHU_TOPIC_URL = "zhihu:topic:url"
ZHIHU_QUESTION_LIST = "zhihu:question:list"


class ZC_Redis(StrictRedis):
    def rpushunit(self, name, *values):
        if self.lrem(name, 0, values[0]):
            return self.rpush(name, *values)
        else:
            return self.lpush(name, *values)

@retry((Exception, ))
def get_questions_by_topic(url, rconn):
    ret = []
    print "Parsing : %s" % url, 
    _, header, post_data = resolve_curl(ZHIHU_CURL)
    now_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    topic_id = re.search(r'/(\d+)/', url).group(1)
    # import ipdb; ipdb.set_trace()
    r =requests.get(url, headers=header, data=post_data, timeout=20)
    if r.status_code != 200:
        raise Exception("Response HTTP Code(%d)" % r.status_code)
    parser = bs(r.text, "html.parser")
    questions = parser.find_all("a", attrs={"class": "question_link"})
    footer = parser.find("div", attrs={"class": "zm-invite-pager"})
    if not (questions and footer):
        print r.text
        raise Exception("Unknown HTML Source Code")
    if "page=" not in url:
        max_page = max([int(link.text) for link in footer.find_all("a", 
            href=re.compile(r"\?page="), text=re.compile(r'\d+'))])
        max_page = max_page if max_page <= 10 else 10
        for i in range(2, max_page + 1):
            rconn.rpushunit(ZHIHU_TOPIC_URL, TOPIC_URL.format(topic_id=topic_id, page=i))
    for q in questions:
        data = {}
        if q.get("href"):
            data['question_url'] = ZHIHU_DOMAIN + q['href']
            data['uri'] = url
            data['topic_id'] = topic_id
            data['create_date'] = now_time
            ret.append(data)
    print "Done !"
    return ret


def get_questions_by_topic_multi(cache):
    while True:
        job = cache.blpop(TOPIC_URL, 0)[1]
        try:
            res = get_questions_by_topic(job, cache)
            if res:
                cache.rpush(ZHIHU_QUESTION_LIST, pickle.dumps(res))
            time.sleep(2)
        except Exception as e:  # no matter what was raised, cannot let process died
            traceback.print_exc()
            print 'Failed to Access Topic Url: ', job
            cache.rpush(TOPIC_URL, job) # put job back
        except KeyboardInterrupt:
            cache.rpush(TOPIC_URL, job) # put job back

def write_relation_multi(cache):
    while True:
        dao = ZhihuTopicWriter(USED_DATABASE)
        res = cache.blpop(ZHIHU_QUESTION_LIST, 0)[1]
        data = pickle.loads(res)
        try:
            dao.insert_topic_question_relation(data)
        except Exception:
            traceback.print_exc()
            cache.rpush(ZHIHU_QUESTION_LIST, res)
        except KeyboardInterrupt :
            cache.rpush(ZHIHU_QUESTION_LIST, res)


def test_get_questions_by_topic():
    dao = ZhihuTopicWriter(USED_DATABASE)
    r = ZC_Redis(**USED_REDIS)
    test_url = "https://www.zhihu.com/topic/19828558/top-answers"
    res = get_questions_by_topic(test_url, r)
    dao.insert_topic_question_relation(res)

def add_jobs(cache):
    if cache.llen(TOPIC_URL) > 0:
        return None
    dao = ZhihuTopicWriter(USED_DATABASE)
    for topic_id in dao.select_topic_ids():
        cache.rpushunit(TOPIC_URL, topic_id)


def main():
    try:
        r = ZC_Redis(**USED_REDIS)
        add_jobs(r)
        print "Redis has %d records in cache" % r.llen(TOPIC_URL)
        cp = mp.current_process()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
        job_pool = mp.Pool(processes=4,
            initializer=get_questions_by_topic_multi, initargs=(r, ))
        result_pool = mp.Pool(processes=4, 
            initializer=write_relation_multi, initargs=(r, ))
        job_pool.close(); result_pool.close()
        job_pool.join(); result_pool.join()
        print "+"*10, "jobs' length is ", r.llen(TOPIC_URL)
        print "+"*10, "results' length is ", r.llen(ZHIHU_QUESTION_LIST)
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in Rn all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"



if __name__ == '__main__':
    # test_get_questions_by_topic()
    main()