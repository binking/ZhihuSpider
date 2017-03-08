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

ZHIHU_URL = "curl 'https://www.zhihu.com/topic/19613907' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Referer: https://www.zhihu.com/search?type=topic&q=%E5%AE%B6%E8%A3%85' -H 'Cookie: aliyungf_tc=AQAAAMNiK3rsWwwAxjlk00A2Jsa/ku3H; q_c1=83d3e2d9ffcb40b78d5588f925bc9118|1488942219000|1488942219000; _xsrf=54f6c50b4aa2d1734bf15cbaabd76662; cap_id="YzI4MTM0MDU4NGVlNDFkMDgwMDVhNWZhODEwM2JiMjk=|1488942219|c2bf7158774859fb4714006bc5f6019cc00b45f6"; l_cap_id="NzE5YjM5Y2VmNjg4NDI4YTg5MTAyYjExNDFiZjA0NzM=|1488942219|fc94000d00d35abd851497a17dac09429df3fe1a"; d_c0="AHCCaHwLawuPTtpLASV6-CT7CoXbh9KjyjQ=|1488942220"; _zap=5d6979ac-ed54-4963-bc9b-b121d82922c9; auth_type="c2luYQ==|1488942234|756ab1d2f1830b664a97678c762e532ae5e4abff"; atoken=2.008OM8iGEA722D0a279d1eafQGkL6E; atoken_expired_in=2649366; token="Mi4wMDhPTThpR0VBNzIyRDBhMjc5ZDFlYWZRR2tMNkU=|1488942234|66956f5a047660a51b9d108e25ca24fead06a571"; client_id="NjE1MzQzNzY1NQ==|1488942234|6c4b7ef73ff07c61c8351a279e76346616b1e479"; nweb_qa=heifetz; unlock_ticket="QUlBQ1lKa0xhd3NYQUFBQVlRSlZUYko3djFpMDZQOFlCVEY0bThFV3FxLTlhT0xiZXhXOHZRPT0=|1488942250|b9752eae68c96864a9340d5d63036934b2873385"; z_c0=Mi4wQUlBQ1lKa0xhd3NBY0lKb2ZBdHJDeGNBQUFCaEFsVk5xZ0huV0FDOXk0a2dTaHdpVUYweTI3dFJkVzRGbGJyeExB|1488943587|1e0ebd611eca395a72b03b553e341bc73e6abb9c; s-q=%E5%AE%B6%E8%A3%85; s-i=1; sid=v867ibi8; __utma=51854390.1248837612.1488942220.1488942220.1488942220.1; __utmb=51854390.0.10.1488942220; __utmc=51854390; __utmz=51854390.1488942220.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmv=51854390.100--|2=registration_date=20170308=1^3=entry_date=20170308=1' -H 'Connection: keep-alive' --compressed"
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