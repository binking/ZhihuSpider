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
            SELECT c_id FROM zhihutopictree
            WHERE p_id IN (
                SELECT c_id FROM zhihutopictree
                WHERE parent= '家居设计')
            UNION 
            SELECT c_id FROM zhihutopictree
            WHERE parent= '家居设计'
            UNION 
            SELECT p_id FROM zhihutopictree
            WHERE parent='家居设计';
            -- WHERE NOT EXISTS (
            --    SELECT id FROM ZhihuTopicQuestionRelation 
            --    WHERE topic_id=ztl.node_id
            -- );
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
