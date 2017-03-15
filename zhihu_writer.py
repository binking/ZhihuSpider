#coding=utf-8
import sys
import time
import traceback
from datetime import datetime as dt
import MySQLdb as mdb
from zc_spider.weibo_writer import DBAccesor, database_error_hunter

reload(sys)
sys.setdefaultencoding('utf-8')


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
                info['uri'], info['uri'], theme, theme, bucketName,
                info['create_date'], info['uri'], info['user_name'], 
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