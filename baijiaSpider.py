# -*- coding:utf-8 -*-

import pymongo
import time
import random
import re
import json
import requests
import threading
from fake_useragent import UserAgent
from pymysql import connect
from bs4 import BeautifulSoup as bs
import redis
import sys

sys.setrecursionlimit(10000)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

'''
百家号文章内容抓取：启用多线程抓取
'''

class baiduSpider(object):
    def __init__(self):
        # self.db = connect(host="secret", port=3306, db="zhan_db", user="root", password="secret", charset="utf8")
        # self.cursor = self.db.cursor()
        try:
            self.redis_cli = redis.Redis(host='secret', port=6379, password='secret', db=5, decode_responses=True)
            self.redis_cli2 = redis.Redis(host="secret", port=6480, db=1, password="secret", decode_responses=True)
        except Exception as e:
            print("连接redis数据库失败",e)
        try:
            mongoUri = 'mongodb://mongouser:password@ip:27017/admin'
            client = pymongo.MongoClient(mongoUri)
            mDB = client.Baijia
            self.collection = mDB.baijiaIncrement
        except Exception as e:
            print("连接mongo数据库失败",e)

        self.useragent = UserAgent()
        #用来计算获取当前第几位号主的文章
        self.caculate_user = 0
        # 获取文章的url
        self.url = 'https://author.baidu.com/list?type=article&context={'
        self.timesiamp = '&_={}'
        self.context = '"offset":"-1_{}","app_id":"{}","last_time":"{}","pageSize":20'
        # 获取评论的url
        self.comment_url = "https://mbd.baidu.com/webpage?type=homepage&action=comment&format=jsonp&params=[{}]&_={}&callback=jsonp3"

        self.hesders2 = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': 'BIDUPSID=9642A005C6C3D6F07C767CA906D5360D; PSTM=1525347482; BAIDUID=ED8B058FB7DA02227103D1CCF3A2F11B:FG=1; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; FP_UID=e695153e61b57e0d9203ab24d4bddde2; cflag=15%3A3; pgv_pvi=5318036480; H_PS_PSSID=26525_1461_21078_26808; PSINO=7',
            'Host': 'baijiahao.baidu.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': self.useragent.random,
        }
        
        self.hesders3 = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': 'BIDUPSID=9642A005C6C3D6F07C767CA906D5360D; PSTM=1525347482; BAIDUID=ED8B058FB7DA02227103D1CCF3A2F11B:FG=1; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; FP_UID=e695153e61b57e0d9203ab24d4bddde2; cflag=15%3A3; pgv_pvi=5318036480; H_PS_PSSID=26525_1461_21078_26808; PSINO=7',
            'Host': 'mbd.baidu.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
        }
        
        self.f = open('error.txt', 'a+', encoding='utf-8')
        # self.data = {"offset": 0, "user_id": "1566831857178535", "end_time": "1538943100", "source_url": "", "userName": ""}
        pass

    def __del__(self):
        # self.db.close()
        self.f.close()
        pass

    def run(self):  #
        while True:
            user_data = self.redis_cli.rpop('baiJiaUser')
            self.caculate_user += 1
            user_data = json.loads(re.sub('\'', '\"', user_data))
            print('正在获取第', self.caculate_user, '号主的文章',user_data['userId'])
            last_time = int(time.time())
            self.end_time = user_data['end_time']
            #获取爬取的近7天文章
            if(last_time - self.end_time > 259200):
                self.end_time = last_time - 259200
            else:
                self.end_time = self.end_time

            # 将一个用户的数据查看完，就把用户个人数据放回redis数据库
            user_data['end_time'] = self.end_time
            self.redis_cli.lpush('baiJiaUser', user_data)
            print('*' * 60, "放回redis数据库")

            self.offset = user_data['offset']
            userName = user_data['userName']
            userId = user_data['userId']

            source_url = 'https://baijiahao.baidu.com/u?app_id={}'.format(userId)
            res = self.get_res(source_url=source_url, userId=userId, last_time=last_time, offset=self.offset)
            time.sleep(0.2)
            self.parse_itemlist(res=res, userName=userName, source_url=source_url, userId=userId)

    def get_res(self, source_url, userId, last_time, offset):
        headers = {
            # GET /list?type=article&context={%22offset%22:%22-1_6%22,%22app_id%22:%221554130297341500%22,%22last_time%22:%221531896119%22,%22pageSize%22:14}&_=1531896156107&callback=jsonp3 HTTP/1.1
            'Host': 'author.baidu.com',
            'Accept': '*/*',
            'Connection': 'keep-alive',
            'Cookie': 'BDORZ=FAE1F8CFA4E8841CC28A015FEAEE495D; BAIDUCUID=0uHwu0PQ2a_Ia2uU08vHagPHBilkuHi6g8SKu_am2ijV8viq_ivdilfvWOrFfQO-IhUmA; WISE_HIS_PM=0; MBD_AT=1531799562; BAIDUID=3492ECBD8832D857A7FC6FEDA223D424:FG=1',
            # 'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E216 light%2F1.0 info baiduboxapp/3.3.0.10 (Baidu-5; P2 11.3)',
            'User-Agent': self.useragent.random,
            'Accept-Language': 'zh-cn',
            # 'Referer': 'https://mbd.baidu.com/webpage?type=profile&action=profile&context=%7B%22app_id%22%3A1554130297341500%7D',
            'Referer': source_url,  # 这个是首页url
            'Accept-Encoding': 'br, gzip, deflate',
        }
        # 构造获取用户发布的文章列表的url

        timestamp = int(round(time.time() * 1000))
        context = self.context.format(offset, userId, last_time)
        url = self.url + context + '}' + self.timesiamp.format(timestamp)
        #获取ip
        ip = self.redis_cli2.srandmember("IP")
        proxies = {
            # "https":"https://{}".format(ip[0]),
            "https": "https://{}".format(ip),
            # "http": "http://125.126.208.11:22829"
        }
        try:
            res = requests.get(url=url, headers=headers, verify=False, proxies=proxies, timeout=3)
            print("用户ID-->", userId, "返回数据")
            return res
        except Exception as e:
            print("用户ID-->", userId, 'ip无效，重新请求', e)
            res = self.get_res(source_url=source_url, userId=userId, last_time=last_time, offset=self.offset)
            return res

    # 解析文章列表
    def parse_itemlist(self, res, userName, source_url, userId):
        self.offset += 20
        try:
            data = json.loads(res.text)

        except Exception as e:
            print("抛出异常，文章-->", source_url, userName, userId)
            return
        try:
            data = data['data']
            items = data['items']
        except:
            items = []
        if items == []:
            self.f.write('无文章列表：' + userName)
            return
        else:
            has_more = data['has_more']
            # lasttime = data['last_time']
            # logging.info("lasttime",lasttime)

            #设置断点时间戳，方便下次抓取时在此处抓起(翻页的时候会用到)
            lasttime = items[len(items)-1]['created_at']

            # 获取评论数
            comment_id = []
            item = {}
            item['comment_count'] = {}
            for i in items:
                comment_id.append(i['did'])
            comment = self.parse_comment(comment_id)

            item['comment_count'] = comment
            # print(item['comment_count'])

            # 遍历文章，爬取文章必要信息
            for i in items:
                #文章发布的时间戳
                timestamp = i['created_at']
                #时间戳　爬取时间戳之后的文章，这个是每次爬取根据各个号主变化的（需要在用户信息里面添加end_time来表示）
                if int(timestamp) > self.end_time:
                    print(userName, "---", userId, "该户主发表了新的文章")
                    item['user_id'] = userId
                    item['save_time'] = int(time.time())
                    item['create_time'] = timestamp
                    item['did'] = i['did']
                    # item['source_url'] = i['href']
                    item['source_url'] = i['url']  # url=http://baijiahao.baidu.com/s?id=1606390504348584783
                    item['item_id'] = i['article_id']
                    item['category'] = i['type']
                    item['title'] = i['title']
                    item['author'] = userName
                    item['comment_count'] = i['comment_amount']
                    item['total_read_count'] = i['read_amount']
                    # item['comment_count']['comment_id'] = i['did']

                    try:
                        # 转换成localtime
                        time_local = time.localtime(timestamp)
                        # 转换成新的时间格式(2016-05-05 20:28:54)
                        dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)

                        # item['datetime'] = i['updatedAt']
                        item['datetime'] = dt
                    except:
                        item['datetime'] = i['updatedAt']
                    # 文章是 新闻（可能是视频或其他的内容）
                    if item['category'] == 'news':
                        try:
                            # 获取新闻文章的 内容
                            content = self.parse_content(item['source_url'])
                            item['content'] = content
                            # print('内容：', item['content'])
                        except Exception as e:
                            print('*' * 100, e)
                            self.f.write(item['author'] + '获取content错误' + '\n')
                            item['content'] = '[]'
                    else:
                        item['content'] = item['category']

                    try:

                        if(item['category'] != 'video' and item['category'] != 'gallery'):
                            # item['category'] = self.parse_article_category(item)
                            # print("正在更新文章的分类，如需详情，可以查看文章链接--->", item['source_url'])
                            self.save_data_mongodb(item)
                        else:
                            print('抓取的新闻类型是音频类或图集，过滤掉...')
                            pass
                    except Exception as e:
                        # print("----", "qps频率过高", "----")
                        # self.save_data_mongodb(item)
                        print("=====================================", e, "==================================")
                        pass
                    # url=http://baijiahao.baidu.com/s?id=1606390504348584783  did

                else:
                    return
            # comment = self.parse_comment(comment_id)
            # item['comment_count'] = comment
        if has_more == True:
            # 下拉获取14条 js请求
            # offset +=20
            res = self.get_res(source_url, userId, lasttime, offset=self.offset)
            self.parse_itemlist(res, userName, source_url, userId)
        else:
            pass

    def parse_content(self, source_url):
        res = requests.get(url=source_url, headers=self.hesders2, verify=False).text
        body = bs(res, 'lxml')
        data = body.select('.article-content')
        data = str(data[0])
        return data

    def parse_comment(self, comment_id):
        urll = []
        for i in comment_id:
            urll.append('{' + '"did":"{0}"'.format(i) + '}')
            url = ','.join(urll)

        t = time.time()
        t = round(t * 1000)
        comment_url = self.comment_url.format(url, t)
        ip = self.redis_cli2.srandmember("IP")
        proxies = {
            # "https":"https://{}".format(ip[0]),
            "https": "https://{}".format(ip),
            # "http": "http://125.126.208.11:22829"
        }
        try:
            res = requests.get(url=comment_url, headers=self.hesders3, proxies=proxies, verify=False, timeout=3).text
        except:
            self.parse_comment(comment_id)
            return
        try:
            res = re.findall(r'jsonp3\((.*?)\)', res)[0]
        except Exception as e:
            print(e, "遇到错误，正在查看,评论链接：", comment_url)

        # data = json.loads(re.sub('\'', '\"', res))
        data = json.loads(res)
        comment = data['data']['homepage_comment']  # str
        return comment

    def save_data_mongodb(self, item):  # 个
        try:
            collection = {}
            collection['total_read_count'] = item['total_read_count']
            collection['comment_count'] = item['comment_count']
            collection['save_time'] = item['save_time']
            item['date_collection'] = []
            item['date_collection'].append(collection)

            insert_items = dict()
            insert_items['source_url'] =  item['source_url']
            insert_items['category'] = item['category']
            insert_items['title'] =  item['title']
            insert_items['author'] = item['author']
            insert_items['datetime'] = item['datetime']
            insert_items['keywords'] =  '[]'
            insert_items['item_id'] = item['item_id']
            insert_items['label'] =  '[]'
            insert_items['create_time'] =  item['create_time']
            insert_items['total_read_count'] =  item['total_read_count']
            insert_items['internal_visit_count'] =  0
            insert_items['external_visit_count'] =  0
            insert_items['comment_count'] = item['comment_count']
            insert_items['share_count'] = 0
            insert_items['impression_count'] = 0
            insert_items['article_content'] = item['content']
            insert_items['is_cluster'] = 'false'
            insert_items['platfrom'] = '百家号'
            insert_items['user_id'] = item['user_id']
            insert_items['save_time'] = item['save_time']
            insert_items['date_collection'] = item['date_collection']
            insert_items['optional_data'] = '[]'

            try:
                self.collection.insert(insert_items)
                print('数据插入Mongo数据库')
            except:
                self.collection.update(
                    {"source_url": item['source_url']},
                    {
                        "$push": {"date_collection": collection},
                        '$set': {"save_time": item['save_time'],
                                 "total_read_count": item['total_read_count'],
                                 "comment_count": item['comment_count'],
                                 "category": item['category']
                                 }
                    })
                print('更新数据')

        except Exception as e:
            print('插入数据库错误', e)
            self.f.write(item['author'] + '插入数据库错误' + '\n')

    #获取access_token的代码，只是当expire_in过期，才调用这个方法
    def get_access_token(self):
        # client_id 为官网获取的AK， client_secret 为官网获取的SK
        url = 'https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id=XiFkuZS2Lr4UytaArMUhHP9k&client_secret=dnmZ5M9n5OUDNf0TLgx3PNRGSH6LbHVL'
        headers = {
            'Content-Type': 'application/json; charset=UTF-8'
        }
        response = requests.get(url, headers=headers)
        content = response.text
  
        if content:
            return json.loads(content)["access_token"]

    def parse_article_category(self, item):
        access_token = '24.3ada8db369e484bc181bfea924189280.2592000.1544254053.282335-11141637'
        pattern = re.compile(r'<[^>]+>', re.S)
        token_url = 'https://aip.baidubce.com/rpc/2.0/nlp/v1/topic?access_token={}'.format(access_token)
        token_content = item['content']
        token_title = item['title']
        # 把文章中的标签都去掉
        token_content = pattern.sub('', token_content)
        token_headers = {
            'Content-Type': 'application/json'
        }
        data = '{"title": \"%s\", "content": \"%s\"}' % (token_title, token_content)
        data = data.encode('gbk')
        try:
            response = requests.post(token_url, data=data, headers=token_headers)
        except Exception as e:
            #如果出错，应该就是access_token过期了，重新获取access_token的值
            print("access_token过期，重新获取access_token的值")
            access_token = self.get_access_token()
            token_url = 'https://aip.baidubce.com/rpc/2.0/nlp/v1/topic?access_token={}'.format(access_token)
            response = requests.post(token_url, data=data, headers=token_headers)

        category = json.loads(response.content.decode("gbk"))['item']['lv1_tag_list']
        category = eval((str(category))[1:-1])
        category = category['tag']
        if category:
            return category
        else:
            return "其他"

    def qps_too_quick(self, item):
        try:
            item['category'] = self.parse_article_category(item)
        except:
            time.sleep(3)
            print("----------解析文章类别过快，睡眠3秒----------")
            self.qps_too_quick(item)
            
if __name__ == '__main__':
    for i in range(5):
        baidu = baiduSpider()
        w = threading.Thread(target=baidu.run)
        w.start()
