#!/usr/bin/env python
#-*-coding:utf8-*-
import os
import json
import time
import redis
import MySQLdb as mdb
import requests
from zhihu_writer import ZhihuTopicWriter
from config import (ZHIHU_CURL, 
    QCLOUD_MYSQL, 
    LOCAL_REDIS,
    CHILD_URLS
)

R_CONN = redis.StrictRedis(**LOCAL_REDIS)  # set
TOPIC_CACHE = []



def traverse_tree_recusively(url, depth=0, max_depth=3):
    print 'current depth is %d, parsing: %s' % (depth, url)
    _, header, post_data = parse_curl(ZHIHU_CURL)
    try:
        db = ZhihuTopicWriter(QCLOUD_MYSQL)
        r = requests.post(url, headers=header, data=post_data)
        R_CONN.sadd(CHILD_URLS, url)
        response = r.text.encode('utf8') #  os.popen(ZHIHU_CURL).read()
        node = json.loads(response)
    except Exception as e:
        print e
        return {'topic': '', 'id': '', 'is_leaf': 'Unknown', 'child': []}
    msg_list = node['msg'][0]
    child_list = node['msg'][1]
    ret_dict = {'topic': msg_list[1], 'id': msg_list[2], 'child': []}

    if not node.get('msg'):
        return {'topic': '', 'id': '', 'is_leaf': 'Unknown', 'child': []}

    if len(child_list) == 0:
        print "Leaf: ", msg_list[1]
        ret_dict['is_leaf'] = 'Y'
        db.insert_node_leaf(msg_list[2], 'Y')
        return ret_dict
    elif depth >= max_depth:
        print "Deepest node: ", msg_list[1]
        ret_dict['is_leaf'] = 'N'
        db.insert_node_leaf(msg_list[2], 'N')
        return ret_dict
    
    for child in child_list:
        if child[0][1] == '加载更多'.decode('utf8'):
            print '加载更多 ...'
            child_url = 'https://www.zhihu.com/topic/19776749/organize/entire?child=%s&parent=%s' % (child[0][2], msg_list[2])
            ret_dict['child'].append(traverse_tree_recusively(child_url, depth, max_depth=max_depth))
        else:
            child_url = 'https://www.zhihu.com/topic/19776749/organize/entire?child=&parent=%s' % child[0][2]
            print "Child: ", child_url
            print '%s(%s) -> %s(%s) -> %d' % (msg_list[1], msg_list[2], child[0][1], child[0][2], depth+1)
            db.insert_edge_node(msg_list[1], msg_list[2], child[0][1], child[0][2], depth+1)
            ret_dict['child'].append(traverse_tree_recusively(child_url, depth+1, max_depth=max_depth))

    return ret_dict

def main():
    current_depth = 5
    node_loader = ZhihuTopicWriter(QCLOUD_MYSQL)
    # root = 'https://www.zhihu.com/topic/19776749/organize/entire'
    try:
        for node in node_loader.select_node_in_one_layer(current_depth):
            node_url = 'https://www.zhihu.com/topic/19776749/organize/entire?child=&parent=%s'%node
            traverse_tree_recusively(node_url, depth=current_depth, max_depth=current_depth+2)
            print "="*80, '\n%s DONE !!!\n'%node, "="*80
    except Exception as e:
        print e
        # with open('saved_topic_in_cache.txt', 'w') as fw:
        #     for record in TOPIC_CACHE:
        #         fw.write(record.encode('utf8')+'\n')
    # with open('zhihu_topic_structure.txt', 'w') as fw:
    #     for sen in text:
    #         print sen
    #         fw.write(sen.encode('utf-8')+'\n')

def print_tree(node, depth=0, indent=4, indent_sign='\t'):
    sentences = ['%s%s(%s)(is_leaf: %s)' % (indent_sign*depth, node['topic'], node['id'], node['is_leaf'])]
    if not node['child']:
        return sentences
    for child in node['child']:
        sentences.extend(print_tree(child, depth=depth+1, indent=indent, indent_sign=indent_sign))
    return sentences

if __name__=="__main__":
    main()

"""
def test_recursive():
    root = 'https://www.zhihu.com/topic/19776749/organize/entire'
    test_child = 'https://www.zhihu.com/topic/19776749/organize/entire?child=&parent=19610139'
    res = traverse_tree_recusively(test_child)
    import ipdb;ipdb.set_trace()

def test_print():
    with open('dili_topic_tree.json', 'r') as fr, open('zhihu_topic_structure.txt', 'w') as fw:
        tree = json.load(fr)
        text = print_tree(tree)
        for sen in text:
            print sen
            fw.write(sen.encode('utf-8')+'\n')
"""