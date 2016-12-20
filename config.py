#coding=utf-8
ZHIHU_CURL = """curl 'https://www.zhihu.com/topic/19776749/organize/entire' -H 'Cookie: d_c0="AIAAOdm30AqPTmD0ITfY9SuMoWChqOqtV38=|1478585530"; _zap=464b9ffd-2d0c-4a21-9f02-92cc1ac64d8f; imhuman_cap_id="MGNkYjk2NjJhODMxNGJjNjgwNDFjNTk5NTFlNGNhMjU=|1481875074|1a191e182ef370a7065e13461367abe35fe76085"; _xsrf=ea0fa2ddbf98d77bcc0e0982f7820a90; q_c1=9b9b414901054deba347a3e925d598d7|1482200255000|1482200255000; cap_id="M2JiZGQ0ODJiZjA0NDhjYmI0NDY5NjQ5NWE4OTc1NGQ=|1482202393|8ce8bfc262985e73876ec7a52ea66298ac6a8517"; l_cap_id="ZTBhZWI0NDg2ZDVjNDk1OGI1ZDYyNDkxNDQxNGE3MWE=|1482202393|d3a745447026a1ea09a9eeb8f26922801b62ec8c"; login="NzViYWRmNTM2YTZmNDQ3OTljNGEzYWZjNjJlMTc3OTk=|1482202393|8c41d996dbea5c5a27149a5a5232b6576da8764d"; r_cap_id="YmQxMDdmMzFiOGQ3NDI0ZDg5NmVkMGQxNzI3OGEwYWY=|1482202458|4eae9fc75be9b82636dff59d5683f8bf253e4918"; auth_type="c2luYQ==|1482202530|1f7cbf9474a1011fd03c2d76b0dc4fd9f4a7d048"; atoken=2.00QIOEXGEA722D219e78bcdeQZrgXE; atoken_expired_in=2649870; token="Mi4wMFFJT0VYR0VBNzIyRDIxOWU3OGJjZGVRWnJnWEU=|1482202530|a8904e31ec26e49aae82ddcc341e7ed9f9af6acd"; client_id="NTk4NTQyMzcyMA==|1482202530|388e49c2971f1111fecf77dbbc1ff233141625e5"; unlock_ticket="QUVEQ0pxdWRCZ3NYQUFBQVlRSlZUYnFrV0ZpRFJoa3JzMlRheTJEbEd1bG8xa0Fja2tCRFZnPT0=|1482202546|3c6addaa736bd9f5a618dd6b5cf34d3dbd70bcbe"; __utmt=1; __utma=51854390.678582963.1481772377.1482200262.1482202394.7; __utmb=51854390.46.10.1482202394; __utmc=51854390; __utmz=51854390.1482202394.7.5.utmcsr=zhihu.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utmv=51854390.100--|2=registration_date=20161220=1^3=entry_date=20161220=1; z_c0=Mi4wQUVEQ0pxdWRCZ3NBZ0FBNTJiZlFDaGNBQUFCaEFsVk5zaXFBV0FEOEQ3dG10dGVNOE15YTJjTkJFdXVydVFrSUNB|1482203190|23e61384d0ae3c91c2fa54a9cf410c101286b521' -H 'Origin: https://www.zhihu.com' -H 'Accept-Encoding: gzip, deflate, br' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded;charset=UTF-8' -H 'Accept: */*' -H 'Referer: https://www.zhihu.com/topic/19776749/organize/entire' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --data '_xsrf=ea0fa2ddbf98d77bcc0e0982f7820a90' --compressed"""

CHILD_URLS = 'zhihu:topic:urls'

QCLOUD_MYSQL = {
    'host': '10.66.110.147',
    'port': 3306,
    'db': 'webcrawler',
    'user': 'web',
    'passwd': 'Crawler20161231',
    'charset': 'utf8',
    'connect_timeout': 20,
}

LOCAL_REDIS = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
}

# Execute result number
SUCCESSED = 1
FAILED = -1
REQUEST_ERROR = -100
PARSE_HTML_ERROR = -101
ACCESS_URL_ERROR = -102
IGNORE_RECORD = -102

# Database Error number using 2XX
DB_WRITE_FAILED = -200
DB_CANNOT_CONNECT = -201
DB_LOST_CONNECTION = -203
DB_SYNTAX_ERROR = -204
DB_LOCK_WAIT_TIMEOUT = -205
DB_FOUND_DEADLOCK = -206
DB_SEVER_GONE_AWAY = -213
DB_UNICODE_ERROR = -207
DB_UNKNOW_ERROR = -299

# Network Error number using 3XX
NETWORK_CONNECTION_ERROR = -300
NETWORK_PROXY_ERROR = -301
NETWORK_TIMEOUT = -302

# Execute result message
ERROR_MSG_DICT = {
    SUCCESSED: "Succeeded",
    FAILED: "Failed",
    REQUEST_ERROR: "Request Target Fialed",
    PARSE_HTML_ERROR: "Parsed url or html FAILED",
    ACCESS_URL_ERROR: "Send requests FAILED",
    IGNORE_RECORD: "The record would be ignored",

    # for database
    DB_WRITE_FAILED: "Write data into database FAILED",
    DB_CANNOT_CONNECT: "Can't connect to MySQL.(服务器资源紧张，导致无法连接)",
    DB_LOCK_WAIT_TIMEOUT: "Lock wait timeout exceeded, try restarting transaction and check if someone manages the table",
    DB_SEVER_GONE_AWAY: "MySQL server has gone away",
    DB_SYNTAX_ERROR: "You have an error in your SQL syntax.(SQL语句有语法错误)",
    DB_LOST_CONNECTION: "Lost connection to MySQL server during query",
    DB_FOUND_DEADLOCK: "Deadlock found when trying to get lock; try restarting transaction",
    DB_UNICODE_ERROR: "Incorrect string value.(不能写入字符串到数据库)",
    DB_UNKNOW_ERROR: "Unknown Program or Operation Errors",

    # for network
    NETWORK_CONNECTION_ERROR: "Connection Error",
    NETWORK_TIMEOUT: "Timeout",
    NETWORK_PROXY_ERROR: "(连接池满了)Max retries exceeded with url",
}