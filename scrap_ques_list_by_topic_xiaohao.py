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

ZHIHU_CURL = """curl 'https://www.zhihu.com/topic/19613907' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cookie: aliyungf_tc=AQAAAC2jp2SdFQAAxjlk0wPlNCHcKr7o; q_c1=f18ebc3260dd4078b97f34a851330b97|1488968419000|1488968419000; _xsrf=0c903d96f3b2eddf37bd5e249a91ef3c; cap_id="NjE5ODM5ZGZhZTRhNDQ3YWE5NjUyNWU4Y2M2MzAwYzI=|1488968419|a55d32833b31b1d3341da0af9182a2d3b03c39dc"; l_cap_id="ZDc5MjRkMWIyOGQ4NGU5YWFlOGZjMjZlZmU4OTY4MzM=|1488968419|3a5ea5926bfb120e2aa3e895341675731e21be90"; d_c0="AEDChG1vawuPTgpI8bEftd4Ih7JpgmEu2JM=|1488968419"; _zap=8114baca-497c-43a1-a0c1-88c2fdcd1817; auth_type="c2luYQ==|1488968435|6e9289138406a801f82e04ced41afd29a4c31908"; atoken=2.00gTuMiGEA722Db6f6b5daba4stobE; atoken_expired_in=2623165; token="Mi4wMGdUdU1pR0VBNzIyRGI2ZjZiNWRhYmE0c3RvYkU=|1488968435|22391abbd34f72742deb2c010aa26764947871a2"; client_id="NjE1MDAzMjgxNg==|1488968435|7d8d2e496db3bf5bdeb2fbad38870ebf56906225"; unlock_ticket="QUVBQ3RvaHZhd3NYQUFBQVlRSlZUUWZpdjFnbGEzeGU3X1NDOWxDU0lWMlVsLVMxazZwRWdBPT0=|1488968447|aa63c3e64f416623c0912576fae2bbde31cf7d4c"; __utma=51854390.235522764.1488968420.1488968420.1488968420.1; __utmb=51854390.0.10.1488968420; __utmc=51854390; __utmz=51854390.1488968420.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmv=51854390.100--|2=registration_date=20170308=1^3=entry_date=20170308=1; z_c0=Mi4wQUVBQ3RvaHZhd3NBUU1LRWJXOXJDeGNBQUFCaEFsVk5fMmZuV0FDdHNTbTZiLTRLcHN0VjY4QUZfWm9YbFRZQ0JB|1488968458|59889dea7f7da7922260014b66fb46c8eb4bc8c5; nweb_qa=heifetz' -H 'Connection: keep-alive' --compressed"""
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
        raise Exception("Response(%s) HTTP Code(%d)" % (url, r.status_code))
    parser = bs(r.text, "html.parser")
    questions = parser.find_all("a", attrs={"class": "question_link"})
    footer = parser.find("div", attrs={"class": "zm-invite-pager"})
    if not questions:
        print "has no question ..."
        return ret
    if "page=" not in url and footer:
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
        job = cache.blpop(ZHIHU_TOPIC_URL, 0)[1]
        try:
            res = get_questions_by_topic(job, cache)
            if res:
                cache.rpush(ZHIHU_QUESTION_LIST, pickle.dumps(res))
            time.sleep(5)
        except Exception as e:  # no matter what was raised, cannot let process died
            traceback.print_exc()
            print 'Failed to Access Topic Url: ', job
            cache.rpush(ZHIHU_TOPIC_URL, job) # put job back
        except KeyboardInterrupt:
            cache.rpush(ZHIHU_TOPIC_URL, job) # put job back

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
    if cache.llen(ZHIHU_TOPIC_URL) > 0:
        return None
    dao = ZhihuTopicWriter(USED_DATABASE)
    for topic_id in dao.select_topic_ids():
        job = "https://www.zhihu.com/topic/%s/top-answers" % topic_id
        cache.rpushunit(ZHIHU_TOPIC_URL, job)


def main():
    try:
        r = ZC_Redis(**USED_REDIS)
        add_jobs(r)
        print "Redis has %d records in cache" % r.llen(ZHIHU_TOPIC_URL)
        cp = mp.current_process()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
        job_pool = mp.Pool(processes=4,
            initializer=get_questions_by_topic_multi, initargs=(r, ))
        result_pool = mp.Pool(processes=4, 
            initializer=write_relation_multi, initargs=(r, ))
        job_pool.close(); result_pool.close()
        job_pool.join(); result_pool.join()
        print "+"*10, "jobs' length is ", r.llen(ZHIHU_TOPIC_URL)
        print "+"*10, "results' length is ", r.llen(ZHIHU_QUESTION_LIST)
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in Rn all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"



if __name__ == '__main__':
    main()