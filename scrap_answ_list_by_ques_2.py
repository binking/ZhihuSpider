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

# https://www.zhihu.com/people/wu-wang-jian-zhi-suo-zhi/activities
ZHIHU_CURL = """curl 'https://www.zhihu.com/question/21423568' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cookie: aliyungf_tc=AQAAAJhBhwN84Q4Axjlk0zvT9NFuI3R9; q_c1=711474b88fb74c23b03e9a8401b78dd4|1489549210000|1489549210000; _xsrf=10b866213407e9955c58946a338ff0fe; r_cap_id="YmQ2YWFiYWJmYWNjNGQwNmE3NjVhMDljM2Q3NzE2MTc=|1489549209|05222ebf6b4492d74a0136280d67a4d83f4f552a"; cap_id="ODFjNzhmMGZlMzA0NDAxZTlkMTJiYzJjZDBkZTY2ZGU=|1489549209|6bfcb2751e4eef749f93e5fe625faf030a7e2c54"; l_cap_id="ZDVmYmQ2MGZmMjVkNGI2ODljMzU3OGUxY2VkMWM1NWI=|1489549210|479488254ae658af05f1e1f0bf933a97bd11d40f"; d_c0="AHACUvwWdAuPTpmSy4Xvg68adtsX735XBOk=|1489549214"; _zap=fa269659-b5a9-4c68-aec1-b742f969a0d3; __utmt=1; nweb_qa=heifetz; z_c0=Mi4wQUVBQ1ZzWXhjd3NBY0FKU19CWjBDeGNBQUFCaEFsVk5wa1R3V0FCYWNQc0NaOHc2SUlUSkVQUVpVSUk0bVY4MkxR|1489549267|881b596d3d3d193f8d8942a8c5f028f4fc8cc8bf; __utma=51854390.122920714.1489549214.1489549214.1489549214.1; __utmb=51854390.8.10.1489549214; __utmc=51854390; __utmz=51854390.1489549214.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmv=51854390.100--|2=registration_date=20170314=1^3=entry_date=20170314=1' -H 'Connection: keep-alive' --compressed"""
ZHIHU_API_CURL = """curl 'https://www.zhihu.com/api/v4/questions/21423568/answers?include=data%5B*%5D.is_normal%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccollapsed_counts%2Creviewing_comments_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Cmark_infos%2Ccreated_time%2Cupdated_time%2Crelationship.is_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cupvoted_followees%3Bdata%5B*%5D.author.is_blocking%2Cis_blocked%2Cis_followed%2Cvoteup_count%2Cmessage_thread_token%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics&offset=3&limit=20&sort_by=default' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'authorization: Bearer Mi4wQUVBQ1ZzWXhjd3NBY0FKU19CWjBDeGNBQUFCaEFsVk5wa1R3V0FCYWNQc0NaOHc2SUlUSkVQUVpVSUk0bVY4MkxR|1489549267|881b596d3d3d193f8d8942a8c5f028f4fc8cc8bf' -H 'accept: application/json, text/plain, */*' -H 'Referer: https://www.zhihu.com/question/21423568' -H 'x-udid: AHACUvwWdAuPTpmSy4Xvg68adtsX735XBOk=' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Connection: keep-alive' --compressed"""
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
               res = process_xhr(job)
            else:
               res = process_url(job)
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