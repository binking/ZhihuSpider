#coding=utf-8
import sys
import time
import traceback
from datetime import datetime as dt
import MySQLdb as mdb
from zc_spider.weibo_writer import DBAccesor, database_error_hunter

reload(sys)
sys.setdefaultencoding('utf-8')

ZHIHU_CURL = {
    '13762241719': """curl 'https://www.zhihu.com/question/36028004/answer/151542217' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Referer: https://www.zhihu.com/' -H 'Cookie: aliyungf_tc=AQAAABvy8xy1jgwAxjlk07G4ofVpryTI; q_c1=962e78a01fcf456c910c59f07fab12b8|1489631734000|1489631734000; _xsrf=e3bbc580baa1dc140b91bfa92006da71; cap_id="ZmFmYWZjYzJmNjRkNGJkZmE5MmVmMjlmNDU5NzZlZWM=|1489631734|3f8c014bc3f9e3dfca9d6583647be93252d7f15c"; l_cap_id="N2ZjMDQ0OWY3Y2QzNDZmYmFkOTE2YWI4NWVmZTA0NzE=|1489631734|38a7722d30c47976ec78c5890ddefbeb5465d5a9"; d_c0="ADDCDMdRdQuPTqj2GGV8HfN7YBsm72K8Hug=|1489631735"; _zap=dbff4eaf-72f7-477d-9791-aed2cf41488d; auth_type="c2luYQ==|1489631743|a11603ed7891accd8ea4acacaa58aa7c56b7f3b4"; atoken=2.0022WLiGEA722D13cd6658caK5ZE1E; atoken_expired_in=2651057; token="Mi4wMDIyV0xpR0VBNzIyRDEzY2Q2NjU4Y2FLNVpFMUU=|1489631743|19e0c32308775c9ecb9a28c05676ec37cfd39a6f"; client_id="NjE0OTcwMTcxNQ==|1489631743|a21f3e0b20ad6beaeff91fca78a42386a360a046"; unlock_ticket="QURCQzNPRlJkUXNYQUFBQVlRSlZUUnNCeWxqVUFGX1JOQ3RYTXE2UlBfbHlMaDFVaXdQcV9nPT0=|1489631763|5efdddfc22019b325f850556bd89dde7fd81ea4a"; __utma=51854390.1030336864.1489631736.1489631736.1489631736.1; __utmb=51854390.0.10.1489631736; __utmc=51854390; __utmz=51854390.1489631736.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmv=51854390.100--|2=registration_date=20170316=1^3=entry_date=20170316=1; z_c0=Mi4wQURCQzNPRlJkUXNBTU1JTXgxRjFDeGNBQUFCaEFsVk5FNGZ4V0FEZzhWbEVNYXpxelpld1hUMzVJd29WNEhCOENR|1489631783|383f87fe47605f24023da225b15db7be9ce11f16; nweb_qa=heifetz' -H 'Connection: keep-alive' -H 'Cache-Control: max-age=0' --compressed""",
    '15073229617': """curl 'https://www.zhihu.com/question/20472563/answer/15221623' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Referer: https://www.zhihu.com/' -H 'Cookie: aliyungf_tc=AQAAACXgLmkMTgsAxjlk00nshnjIxEnK; q_c1=f0506912bbff4c238fdfad69a5467ad4|1489631876000|1489631876000; _xsrf=8d22ea6eed99f1ffd43143cb696045d9; cap_id="ZDlkZDFhNGY4YzMzNDc3Yjk4YjJjNzgyMmZlODJkNDM=|1489631876|4addb912ebbd25bb651470be102379301a9afb6f"; l_cap_id="M2FiM2Y4ZmU0MzQ2NGQwZGE5MWFmYmVkNGExZGNhNDI=|1489631876|151d67b0cf37038a0910417e7be5d6109b042acb"; d_c0="AFDChlFSdQuPTpC6tNC2LYWG-cYNspok_P4=|1489631877"; _zap=1aed5875-a66d-4b3d-9404-fe4ab0b65ca1; auth_type="c2luYQ==|1489631914|2499abcb13b6d4319dc6d00876f3e37db6666df4"; atoken=2.00BVuMiGEA722D5c21789654upPfBB; atoken_expired_in=2650886; token="Mi4wMEJWdU1pR0VBNzIyRDVjMjE3ODk2NTR1cFBmQkI=|1489631914|c05c293da6a42ff4d1376aa73f8e92b64c918567"; client_id="NjE1MDAzMjg5OQ==|1489631914|94227963fa94bf7379242937b237adb517dab0dd"; unlock_ticket="QUNCQ2xueFNkUXNYQUFBQVlRSlZUYmtCeWxoeFlnOGZxOG1MelppR1UxSWZkV0dsWlg5czdnPT0=|1489631921|ecb03017b3479e1221dc44b43581a22608dbe418"; __utma=51854390.741577488.1489631877.1489631877.1489631877.1; __utmb=51854390.0.10.1489631877; __utmc=51854390; __utmz=51854390.1489631877.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmv=51854390.100--|2=registration_date=20170316=1^3=entry_date=20170316=1; z_c0=Mi4wQUNCQ2xueFNkUXNBVU1LR1VWSjFDeGNBQUFCaEFsVk5zWWZ4V0FDN1dVekNCdHluSjAxTUVaVmFmd3R1eWlJT2pB|1489631926|a22ee830748ed863b267e39d9b33f95584c071e2; nweb_qa=heifetz' -H 'Connection: keep-alive' -H 'Cache-Control: max-age=0' --compressed""",
    '13517439721': """curl 'https://www.zhihu.com/question/47647183/answer/151764142' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Referer: https://www.zhihu.com/' -H 'Cookie: aliyungf_tc=AQAAAMzqdlIQtAYAxjlk03g+6suIyJEt; q_c1=ab263b8dd8f245f89a83e2f28ed78281|1489632027000|1489632027000; _xsrf=bce5821b2bd91d482889b6fa7512a67b; cap_id="YTEzMTJiMGU5NmU1NGMwNDg2NDkxZjdlOTBhMWMzNjg=|1489632027|ca11c1c94c46f43855294eda21924e272edaba8d"; l_cap_id="YzIyOWE5ZDkwYTY2NDM5Y2E4NGE4YzY1ODk4MmE3MWE=|1489632027|94cb25e92a2340e048ac404356c96c4837513bfb"; d_c0="ABACyORSdQuPTqpYcK_nqTBK4uJKYeE0ab8=|1489632028"; _zap=930b1a79-54a2-4f97-8145-ddf5c8b65ae4; auth_type="c2luYQ==|1489632066|015922395a20d2e706602bb9f5799077f346e5c2"; atoken=2.00y2WLiGEA722D8fdf06fabe086Cup; atoken_expired_in=2650734; token="Mi4wMHkyV0xpR0VBNzIyRDhmZGYwNmZhYmUwODZDdXA=|1489632066|e814ffeae7f0f9c1d35335c39497358175756d14"; client_id="NjE0OTcwMTc1NA==|1489632066|865abc3a49e6671e1f2d62eb89d0d59d9c84f9b7"; unlock_ticket="QUNEQ3pCcFRkUXNYQUFBQVlRSlZUVnNDeWxqQjJ2RE1WSUZiWFBwNUkxbXd6UFVGd3BJSmxnPT0=|1489632083|4d3925842f36d558aab17e34c1b8fe3b78671655"; __utma=51854390.851504064.1489632028.1489632028.1489632028.1; __utmb=51854390.0.10.1489632028; __utmc=51854390; __utmz=51854390.1489632028.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmv=51854390.100--|2=registration_date=20170316=1^3=entry_date=20170316=1; z_c0=Mi4wQUNEQ3pCcFRkUXNBRUFMSTVGSjFDeGNBQUFCaEFsVk5VNGp4V0FBa2dLdndEcUdGMFJFeElHQzlTQW9KUlJhM2hn|1489632110|27f57ec84f5d3bf05c877f38564bab4f82205012; nweb_qa=heifetz' -H 'Connection: keep-alive' -H 'Cache-Control: max-age=0' --compressed""",
    '18773075418': """curl 'https://www.zhihu.com/question/19583984/answer/12305774' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: aliyungf_tc=AQAAAN/JWWip2AwAxjlk08mSM9p0v/8L; q_c1=87b8cc7441364d5db4d85c85da81d531|1489632228000|1489632228000; _xsrf=220347c2610d20e371de7639b637314b; cap_id="YmY1NGUyYTJiY2M1NDc2ZjllYzQ5ZGY5NTI3ZWE2YTE=|1489632228|3994776fdd613a26999237b5c0cd013717d3ab19"; l_cap_id="MDU2MjI1NDJjNTI5NGJlYmIyMjlhMDU4OWRkYTQxZTk=|1489632228|61891addbf05f3744aa5eafd94461985f9b13f73"; d_c0="ADACWqlTdQuPTsD_vUV2gqmvV7Ceqls7Qto=|1489632229"; nweb_qa=heifetz; _zap=66f0abf7-d2dc-42e8-bf43-6b1dd1c8edf5; auth_type="c2luYQ==|1489632236|be9428c5d2e8a15a8a1aa179ceb077977011e5ba"; atoken=2.00x2WLiGEA722D819b33ebd927jUBC; atoken_expired_in=2650564; token="Mi4wMHgyV0xpR0VBNzIyRDgxOWIzM2ViZDkyN2pVQkM=|1489632236|9116f1eac7ffea5cc1eeb6e2ecbe7589fb8e4c44"; client_id="NjE0OTcwMTc1Mw==|1489632236|4dcdbda6e8db09afe310c14832bf257dc37084b4"; unlock_ticket="QUlCQzRMZFRkUXNYQUFBQVlRSlZUZndDeWxqYkMtWHhCaVM4SVZ2VDRTME9CTGdVSlRZRTFRPT0=|1489632244|827409fde881b70a05cb3bbab084c0498c144675"; __utma=51854390.1961376742.1489632230.1489632230.1489632230.1; __utmb=51854390.0.10.1489632230; __utmc=51854390; __utmz=51854390.1489632230.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utmv=51854390.100--|2=registration_date=20170316=1^3=entry_date=20170316=1; z_c0=Mi4wQUlCQzRMZFRkUXNBTUFKYXFWTjFDeGNBQUFCaEFsVk45SWp4V0FEbDBBZ0swdXhZNkg0MVVHR1pEaUR5eUxIdWN3|1489632245|60cd02bc44051a0178acb804604dc184035f637d' -H 'Connection: keep-alive' --compressed""",
}
ZHIHU_API_CURL = {
    '13762241719': """curl 'https://www.zhihu.com/api/v4/questions/36028004/answers?include=data%5B*%5D.is_normal%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccollapsed_counts%2Creviewing_comments_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Cmark_infos%2Ccreated_time%2Cupdated_time%2Crelationship.is_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cupvoted_followees%3Bdata%5B*%5D.author.is_blocking%2Cis_blocked%2Cis_followed%2Cvoteup_count%2Cmessage_thread_token%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics&offset=&limit=3&sort_by=default' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'authorization: Bearer Mi4wQURCQzNPRlJkUXNBTU1JTXgxRjFDeGNBQUFCaEFsVk5FNGZ4V0FEZzhWbEVNYXpxelpld1hUMzVJd29WNEhCOENR|1489631783|383f87fe47605f24023da225b15db7be9ce11f16' -H 'accept: application/json, text/plain, */*' -H 'Referer: https://www.zhihu.com/question/36028004/answer/151542217' -H 'x-udid: ADDCDMdRdQuPTqj2GGV8HfN7YBsm72K8Hug=' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Connection: keep-alive' --compressed""",
    '15073229617': """curl 'https://www.zhihu.com/api/v4/questions/20472563/answers?include=data%5B*%5D.is_normal%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccollapsed_counts%2Creviewing_comments_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Cmark_infos%2Ccreated_time%2Cupdated_time%2Crelationship.is_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cupvoted_followees%3Bdata%5B*%5D.author.is_blocking%2Cis_blocked%2Cis_followed%2Cvoteup_count%2Cmessage_thread_token%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics&offset=&limit=3&sort_by=default' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'authorization: Bearer Mi4wQUNCQ2xueFNkUXNBVU1LR1VWSjFDeGNBQUFCaEFsVk5zWWZ4V0FDN1dVekNCdHluSjAxTUVaVmFmd3R1eWlJT2pB|1489631926|a22ee830748ed863b267e39d9b33f95584c071e2' -H 'accept: application/json, text/plain, */*' -H 'Referer: https://www.zhihu.com/question/20472563/answer/15221623' -H 'x-udid: AFDChlFSdQuPTpC6tNC2LYWG-cYNspok_P4=' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Connection: keep-alive' --compressed""",
    '13517439721': """curl 'https://www.zhihu.com/api/v4/questions/47647183/answers?include=data%5B*%5D.is_normal%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccollapsed_counts%2Creviewing_comments_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Cmark_infos%2Ccreated_time%2Cupdated_time%2Crelationship.is_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cupvoted_followees%3Bdata%5B*%5D.author.is_blocking%2Cis_blocked%2Cis_followed%2Cvoteup_count%2Cmessage_thread_token%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics&offset=&limit=3&sort_by=default' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'authorization: Bearer Mi4wQUNEQ3pCcFRkUXNBRUFMSTVGSjFDeGNBQUFCaEFsVk5VNGp4V0FBa2dLdndEcUdGMFJFeElHQzlTQW9KUlJhM2hn|1489632110|27f57ec84f5d3bf05c877f38564bab4f82205012' -H 'accept: application/json, text/plain, */*' -H 'Referer: https://www.zhihu.com/question/47647183/answer/151764142' -H 'x-udid: ABACyORSdQuPTqpYcK_nqTBK4uJKYeE0ab8=' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Connection: keep-alive' --compressed""",
    '18773075418': """curl 'https://www.zhihu.com/api/v4/questions/19583984/answers?include=data%5B*%5D.is_normal%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccollapsed_counts%2Creviewing_comments_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Cmark_infos%2Ccreated_time%2Cupdated_time%2Crelationship.is_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cupvoted_followees%3Bdata%5B*%5D.author.is_blocking%2Cis_blocked%2Cis_followed%2Cvoteup_count%2Cmessage_thread_token%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics&offset=&limit=3&sort_by=default' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'authorization: Bearer Mi4wQUlCQzRMZFRkUXNBTUFKYXFWTjFDeGNBQUFCaEFsVk45SWp4V0FEbDBBZ0swdXhZNkg0MVVHR1pEaUR5eUxIdWN3|1489632245|60cd02bc44051a0178acb804604dc184035f637d' -H 'accept: application/json, text/plain, */*' -H 'Referer: https://www.zhihu.com/question/19583984/answer/12305774' -H 'x-udid: ADACWqlTdQuPTsD_vUV2gqmvV7Ceqls7Qto=' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Connection: keep-alive' --compressed""",
}

class ZhihuTopicWriter(DBAccesor):
    def __init__(self, db_dict):
        DBAccesor.__init__(self, db_dict)

    def connect_database(self):
        return DBAccesor.connect_database(self)

    def insert_edge_node(self, parent, p_id, child, c_id, depth):
        conn = self.connect_database()
        cursor = conn.cursor()
        insert_sql = """
            INSERT INTO zhihutopictree (parent, p_id, child, c_id, depth)
            SELECT  %s, %s, %s, %s, %s
            FROM DUAL
            WHERE NOT EXISTS (SELECT id FROM zhihutopictree WHERE p_id = %s and c_id = %s)
        """
        if cursor.execute(insert_sql, (parent, p_id, child, c_id, depth, p_id, c_id)):
            print '$'*10, 'Insert parent-child edge succeeded !'
        conn.commit(); cursor.close(); conn.close()
        return True

    def insert_node_leaf(self, node, is_leaf):
        conn = self.connect_database()
        cursor = conn.cursor()
        insert_sql = """
            INSERT INTO zhihutopicleaf (node_id, is_leaf)
            SELECT %s, %s
            FROM DUAL 
            WHERE NOT EXISTS (SELECT id FROM zhihutopicleaf WHERE node_id=%s)
        """
        if cursor.execute(insert_sql, (node, is_leaf, node)):
            print '$'*10, 'Insert node/leaf succeeded !'
        conn.commit(); cursor.close(); conn.close()
        return True

    def select_node_in_one_layer(self, depth):
        conn = self.connect_database()
        cursor = conn.cursor()
        select_node = """
            SELECT zf.node_id FROM zhihutopictree zt, zhihutopicleaf zf
            WHERE zt.c_id=zf.node_id AND zt.depth=%s AND zf.is_leaf='N'
        """
        cursor.execute(select_node, (depth, ))
        for res in cursor.fetchall():
            yield res[0]

    def select_topic_ids(self):
        sql = """
            SELECT distinct node_id from ZhihuTopicLeaf ztl
            WHERE NOT EXISTS (
                SELECT id FROM ZhihuTopicQuestionRelation 
                WHERE topic_id=ztl.node_id)
            ORDER BY id desc;
            -- SELECT c_id FROM zhihutopictree
            -- WHERE p_id IN (
            --     SELECT c_id FROM zhihutopictree
            --     WHERE parent= '家居设计')
            -- UNION 
            -- SELECT c_id FROM zhihutopictree
            -- WHERE parent= '家居设计'
            -- UNION 
            -- SELECT p_id FROM zhihutopictree
            -- WHERE parent='家居设计';
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        cursor.execute(sql)
        for res in cursor.fetchall():
            yield res[0]

    def insert_topic_question_relation(self, relation_list):
        sql = """
            INSERT INTO ZhihuTopicQuestionRelation 
            (top_uri, topic_id, question_url, create_date)
            SELECT %s,%s,%s,%s
            FROM DUAL WHERE NOT EXISTS (
                SELECT id FROM ZhihuTopicQuestionRelation 
                WHERE topic_id = %s AND question_url = %s
            )
        """
        conn = self.connect_database()
        # import ipdb; ipdb.set_trace()
        for relation in relation_list:
            cursor = conn.cursor()
            if cursor.execute(sql, (
                relation['uri'], relation['topic_id'], 
                relation['question_url'], relation['create_date'],
                relation['topic_id'], relation['question_url'])):
                print '$'*10, 'Insert Relation(%s-->%s) succeeded !' % (relation['topic_id'], relation['question_url'])
                conn.commit(); cursor.close()
            else:
                print "Relation(%s-->%s) Existed ..." % (relation['topic_id'], relation['question_url'])
        conn.close() 


class ZhihuAnswerWriter(DBAccesor):
    def __init__(self, db_dict):
        DBAccesor.__init__(self, db_dict)

    def connect_database(self):
        return DBAccesor.connect_database(self)

    def insert_answer_list(self, data):
        theme = '知乎_问题和回答python'
        bucketName = 'list'
        insert_sql = """
            INSERT INTO ZhihuAnswer (
            fullpath, realpath, theme, middle, bucketName, 
            createdate, uri, answer_author_nickname, answer_author_intro, 
            answer_author_id, answer_author_url, answer_author_portrait, 
            answer_url, answer_content, sub_date, answer_comment_num, 
            answer_thumb_up_num, topic_url)
            SELECT  %s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s
            FROM DUAL WHERE NOT EXISTS (
            SELECT id FROM ZhihuAnswer WHERE answer_url=%s)
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        for info in data:
            if cursor.execute(insert_sql, (
                info['uri'][:254], info['uri'][:254], theme, theme, bucketName,
                info['create_date'], info['uri'][:254], info['user_name'], 
                info['user_intro'], info['user_id'], info['user_url'], 
                info['user_img'], info['ans_url'], info['content'], 
                info['sub_date'], info['comment_num'], info['like_num'],
                info['ques_url'], info['ans_url'])):
                print '$'*10, 'Insert Answer(%s) succeeded !' % info['ans_url']
        conn.commit(); cursor.close(); conn.close()
        return True

    def select_question_urls(self):
        sql = """
            SELECT DISTINCT question_url
            FROM zhihutopicquestionrelation zr
            WHERE not EXISTS (
            SELECT id FROM zhihuanswer 
            WHERE topic_url=zr.question_url);
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        cursor.execute(sql)
        for res in cursor.fetchall():
            yield res[0]
"""
fullpath
realpath
createdate 
uri 
answer_author_nickname
answer_author_intro
answer_author_id
answer_author_url
answer_author_portrait
answer_url
answer_content
sub_date
answer_comment_num
answer_thumb_up_num
topic_url  ques_url
"""