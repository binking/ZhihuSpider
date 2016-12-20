#!/usr/bin/env python
#-*-coding:utf8-*-
import os
import json
import time
import requests
from config import ZHIHU_CURL

TOPIC_CACHE = []

def parse_curl(curl):
    """
    Given curl that was cpoied from Chrome, no matter baidu or sogou, 
    parse it and then get url and the data you will post/get with requests
    """
    url = ''
    header = {}
    data = {}
    tokens = curl.split("'")
    if not tokens:
        # curl is empty string
        return cookie_dict
    try:
        for i in range(0, len(tokens)-1, 2):
            if tokens[i].startswith("curl"):
                url = tokens[i+1]
            elif "-H" in tokens[i]:
                attr, value = tokens[i+1].split(": ")  # be careful space
                header[attr] = value
            elif "--data" in tokens[i]:
                attr, value = tokens[i+1].split("=")
                data[attr] = value
    except Exception as e:
        print "!"*20, "Parsed cURL Failed"
        traceback.print_exc()
    return url, header, data

def traverse_tree_recusively(url, depth=0, max_depth=3):
    print 'current depth is %d, parsing: %s' % (depth, url)
    _, header, post_data = parse_curl(ZHIHU_CURL)
    try:
        r = requests.post(url, headers=header, data=post_data)
        response = r.text.encode('utf8') #  os.popen(ZHIHU_CURL).read()
        node = json.loads(response)
    except Exception as e:
        print e
        time.sleep(5)
        return {'topic': '', 'id': '', 'child': []}
    msg_list = node['msg'][0]
    child_list = node['msg'][1]
    ret_dict = {'topic': msg_list[1], 'id': msg_list[2], 'child': []}

    if not node.get('msg'):
        return {'topic': '', 'id': '', 'child': []}

    if len(child_list) == 0:
        print "Leaf: ", msg_list[1]
        return ret_dict
    elif depth >= max_depth:
        print "Deepest node: ", msg_list[1]
        return ret_dict
    
    for child in child_list:
        print '%s(%s) -> %s(%s) -> %d' % (msg_list[1], msg_list[2], child[0][1], child[0][2], depth+1)
        TOPIC_CACHE.append('%s(%s) -> %s(%s) -> %d' % (msg_list[1], msg_list[2], child[0][1], child[0][2], depth+1))
        child_url = 'https://www.zhihu.com/topic/19776749/organize/entire?child=&parent=%s' % child[0][2]
        print "Child: ", child_url
        ret_dict['child'].append(traverse_tree_recusively(child_url, depth+1, max_depth=max_depth))

    return ret_dict

def print_tree(node, depth=0, indent=4, indent_sign='\t'):
    sentences = ['%s%s(%s)' % (indent_sign*depth, node['topic'], node['id'])]
    if not node['child']:
        return sentences
    for child in node['child']:
        sentences.extend(print_tree(child, depth=depth+1, indent=indent, indent_sign=indent_sign))
    return sentences

def main():
    root = 'https://www.zhihu.com/topic/19776749/organize/entire'
    try:
        res = traverse_tree_recusively(root, max_depth=2)
        import ipdb; ipdb.set_trace()
        text = print_tree(res)
    except Exception as e:
        print e
        # with open('saved_topic_in_cache.txt', 'w') as fw:
        #     for record in TOPIC_CACHE:
        #         fw.write(record.encode('utf8')+'\n')
    # with open('zhihu_topic_structure.txt', 'w') as fw:
    #     for sen in text:
    #         print sen
    #         fw.write(sen.encode('utf-8')+'\n')

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