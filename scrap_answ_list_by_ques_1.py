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
from pprint import pprint
from redis import StrictRedis
from datetime import date, timedelta
from urlparse import parse_qs, urljoin
from datetime import datetime as dt
from bs4 import BeautifulSoup as bs
from zhihu_writer import ZhihuAnswerWriter
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
# account: 18711147563  cisqce220
# https://www.zhihu.com/people/wang-zhe-lu-lu-xiu-45/activities
ZHIHU_CURL = """curl 'https://www.zhihu.com/question/24175751' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: aliyungf_tc=AQAAABvdIwVJNAUAxjlk099T4Sd/9/sv; q_c1=d803bf1452474193b5cd48a1ad87fab0|1489548630000|1489548630000; _xsrf=3b74bf02277904059c865c1cc44d2545; r_cap_id="ZjU2ZTVhYjE1Njg3NDExZmIzZGM5OGE3OGQ5MmQxMjY=|1489548630|edfd8ee9c9e4db39f30c160928d7d3fbf9d64f75"; cap_id="OTUzODM0MDU1YWFhNGFiMzg1YTEzYTRlMGZhOTJhMDA=|1489548630|736e15f55ac4d6a1aab3d91b1acb00b0d3d124d7"; l_cap_id="MTI2YTc0ODVjMTdkNGQ0M2IzMGJkMTk2ZDk3MDhlZWU=|1489548630|02e0a0ad93c05506eda5d47aacf6a29134c08033"; d_c0="ADBCEcUUdAuPTm97owCAksjtOxqnZn1Tk_E=|1489548633"; _zap=e861e337-9471-4b13-ab07-5ed1392e329f; z_c0=Mi4wQUVBQ3RvaHZhd3NBTUVJUnhSUjBDeGNBQUFCaEFsVk5ZRUx3V0FEQVVNRW9CRGVGenJyVHNvQnhDVGt5MEFtU2Zn|1489548644|9f34fbd7eb960fbca3ef2351138cfdd5e8686b99; nweb_qa=heifetz; __utma=51854390.1488839080.1489548634.1489548634.1489548634.1; __utmb=51854390.0.10.1489548634; __utmc=51854390; __utmz=51854390.1489548634.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmv=51854390.100--|2=registration_date=20170308=1^3=entry_date=20170308=1' -H 'Connection: keep-alive' --compressed"""
ZHIHU_API_CURL = """curl 'https://www.zhihu.com/api/v4/questions/24175751/answers?include=data%5B*%5D.is_normal%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccollapsed_counts%2Creviewing_comments_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Cmark_infos%2Ccreated_time%2Cupdated_time%2Crelationship.is_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cupvoted_followees%3Bdata%5B*%5D.author.is_blocking%2Cis_blocked%2Cis_followed%2Cvoteup_count%2Cmessage_thread_token%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics&offset=3&limit=20&sort_by=default' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'authorization: Bearer Mi4wQUVBQ3RvaHZhd3NBTUVJUnhSUjBDeGNBQUFCaEFsVk5ZRUx3V0FEQVVNRW9CRGVGenJyVHNvQnhDVGt5MEFtU2Zn|1489548644|9f34fbd7eb960fbca3ef2351138cfdd5e8686b99' -H 'accept: application/json, text/plain, */*' -H 'Referer: https://www.zhihu.com/question/24175751' -H 'x-udid: ADBCEcUUdAuPTm97owCAksjtOxqnZn1Tk_E=' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Connection: keep-alive' --compressed"""
ZHIHU_QUES_URL = "zhihu:question:url"
ZHIHU_QUES_INFO = "zhihu:answer:list"


def timestamp2datetime(time_str):
    return dt.fromtimestamp(int(time_str)).strftime('%Y-%m-%d %H:%M:%S')


def clear_html(content):
     parser = bs(content, 'html.parser')
     return parser.text


@retry((Exception, ))
def process_url(url, rconn):
     """
     :param(str) url: like this : https://www.zhihu.com/question/19583984
     """
     ret = []
     print "Parsing : ", url,
     now_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
     _, header, post_data = resolve_curl(ZHIHU_CURL)
     # import ipdb; ipdb.set_trace()
     r = requests.get(url, headers=header, data=post_data, timeout=20)
     if r.status_code != 200:
        raise Exception("Response(%s) HTTP Code(%d)" % (url, r.status_code))
     try:
          parser = bs(r.text, 'html.parser')
          data_div = parser.find('div', attrs={'id': 'data'})
          data = json.loads(data_div['data-state'])
     except Exception as e:
          traceback.print_exc()
          return ret
     # get next url apiV4 url
     ques_id = url.replace('https://www.zhihu.com/question/', '')
     if data['question']['answers'][ques_id]['isDrained'] == False:
          next_url = data['question']['answers'][ques_id]['next'].replace('http://www-nweb-api-qa', 
               'https://www.zhihu.com/api/v4')  # api v4 url has "questions"
          rconn.lpush(ZHIHU_QUES_URL, next_url)
     else:
          print "It really Drained ..."
     answer_list = data['entities']['answers']
     for user, answer in answer_list.items():
          info = {}
          # 一个应该不会出现一页两次吧
          try:
               info['like_num'] = answer['voteupCount']
               info['comment_num'] = answer['commentCount']
               info['ans_url'] = "https://www.zhihu.com/question/{0}/answer/{1}".format(
                    answer['question']['id'], answer['id'])
               info['user_id'] = answer['author']['id']
               info['user_url'] = "https://www.zhihu.com/{0}/{1}".format(
                    answer['author']['userType'],
                    answer['author']['urlToken'])
               info['user_name'] = answer['author']['name']
               info['user_intro'] = answer['author']['headline']
               info['user_img'] = answer['author']['avatarUrl']
               info['content'] = clear_html(answer['content'])
               info['sub_date'] = timestamp2datetime(answer['createdTime'])
               info['ques_url'] = 'https://www.zhihu.com/question/' + str(answer['question']['id'])
               info['create_date'] = now_time
               info['uri'] = url
               ret.append(info)
          except Exception as e:
               traceback.print_exc()
               pprint(answer, indent=4)
     print " DONE !!! "
     return ret


@retry((Exception, ))
def process_xhr(url, rconn):
     """
     :param(str) url : like this :https://www.zhihu.com/api/v4/questions/
          19583984/answers?sort_by=default&include=data%5B%2A%5D.is_normal%2C
          is_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2
          Ccollapsed_counts%2Creviewing_comments_count%2Ccan_comment%2C
          content%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2C
          comment_permission%2Cmark_infos%2Ccreated_time%2Cupdated_time%2C
          relationship.is_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2C
          upvoted_followees%3Bdata%5B%2A%5D.author.is_blocking%2Cis_blocked%2C
          is_followed%2Cvoteup_count%2Cmessage_thread_token%2Cbadge%5B%3F%28
          type%3Dbest_answerer%29%5D.topics&limit=3&offset=3
     """
     ret = []
     print "Parsing : ", url,
     now_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
     _, header, post_data = resolve_curl(ZHIHU_API_CURL)
     # import ipdb; ipdb.set_trace()
     r = requests.get(url, headers=header, data=post_data, timeout=20)
     if r.status_code != 200:
        raise Exception("Response(%s) HTTP Code(%d)" % (url, r.status_code))
     try:
          data = json.loads(r.text)
     except ValueError as e:
          print str(e), " --> ", r.text
          return ret
     if data['paging']['is_end'] == True:
          print "No more answers ..."
     else:
          next_url = data['paging']['next'].replace('http://www-nweb-api-qa', 
               'https://www.zhihu.com/api/v4')  # api v4 url has "questions"
          rconn.lpush(ZHIHU_QUES_URL, next_url)
     for answer in data['data']:
          info = {}
          try:
               info['like_num'] = answer['voteup_count']
               info['comment_num'] = answer['comment_count']
               info['ans_url'] = "https://www.zhihu.com/question/{0}/answer/{1}".format(
                    answer['question']['id'], answer['id'])
               info['user_id'] = answer['author']['id']
               info['user_url'] = "https://www.zhihu.com/{0}/{1}".format(
                    answer['author']['user_type'],
                    answer['author']['url_token'])
               info['user_name'] = answer['author']['name']
               info['user_intro'] = answer['author']['headline']
               info['user_img'] = answer['author']['avatar_url']
               info['content'] = clear_html(answer['content'])
               info['sub_date'] = timestamp2datetime(answer['created_time'])
               info['ques_url'] = 'https://www.zhihu.com/question/' + str(answer['question']['id'])
               info['create_date'] = now_time
               info['uri'] = url
               ret.append(info)
          except Exception as e:
               traceback.print_exc()
               pprint(answer, indent=4)
     print " DONE !!! "
     return ret


def test_process_xhr():
     r = StrictRedis(**USED_REDIS)
     dao = ZhihuAnswerWriter(USED_DATABASE)
     test_api = "https://www.zhihu.com/api/v4/questions/19583984/answers?sort_by=default&include=data%5B%2A%5D.is_normal%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccollapsed_counts%2Creviewing_comments_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Cmark_infos%2Ccreated_time%2Cupdated_time%2Crelationship.is_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cupvoted_followees%3Bdata%5B%2A%5D.author.is_blocking%2Cis_blocked%2Cis_followed%2Cvoteup_count%2Cmessage_thread_token%2Cbadge%5B%3F%28type%3Dbest_answerer%29%5D.topics&limit=3&offset=3"
     res = process_xhr(test_api, r)
     dao.insert_answer_list(res)


def test_process_url():
     r = StrictRedis(**USED_REDIS)
     dao = ZhihuAnswerWriter(USED_DATABASE)
     test_url = "https://www.zhihu.com/question/19583984"
     res = process_url(test_url, r)
     dao.insert_answer_list(res)


def get_answers_by_question_multi(cache):
    while True:
        job = cache.blpop(ZHIHU_QUES_URL, 0)[1]
        try:
            if "api/v4" in job:
               res = process_xhr(job, cache)
            else:
               res = process_url(job, cache)
            if res:
                cache.rpush(ZHIHU_QUES_INFO, pickle.dumps(res))
            time.sleep(5)
        except Exception as e:  # no matter what was raised, cannot let process died
            traceback.print_exc()
            print 'Failed to Access Question Url: ', job
            cache.rpush(ZHIHU_QUES_URL, job) # put job back
        except KeyboardInterrupt:
            cache.rpush(ZHIHU_QUES_URL, job) # put job back


def write_answers_multi(cache):
    while True:
        dao = ZhihuAnswerWriter(USED_DATABASE)
        res = cache.blpop(ZHIHU_QUES_INFO, 0)[1]
        data = pickle.loads(res)
        try:
            dao.insert_answer_list(data)
        except Exception:
            traceback.print_exc()
            cache.rpush(ZHIHU_QUES_INFO, res)
        except KeyboardInterrupt :
            cache.rpush(ZHIHU_QUES_INFO, res)


def main():
    try:
        r = StrictRedis(**USED_REDIS)
        print "Redis has %d records in cache" % r.llen(ZHIHU_QUES_URL)
        cp = mp.current_process()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
        job_pool = mp.Pool(processes=4,
            initializer=get_answers_by_question_multi, initargs=(r, ))
        result_pool = mp.Pool(processes=4, 
            initializer=write_answers_multi, initargs=(r, ))
        job_pool.close(); result_pool.close()
        job_pool.join(); result_pool.join()
        print "+"*10, "jobs' length is ", r.llen(ZHIHU_QUES_URL)
        print "+"*10, "results' length is ", r.llen(ZHIHU_QUES_INFO)
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in Rn all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"


if __name__ == '__main__':
    # test_process_url()
    # test_process_xhr()
    main()