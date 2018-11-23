# coding:utf-8

import json
import time
import re
import requests
import threading
from fake_useragent import UserAgent
from pymysql import connect
from bs4 import BeautifulSoup as bs
import redis
import sys
sys.setrecursionlimit(10000)
#去除https的证书警告
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class Baidu(object):
    def __init__(self):
        self.db = connect(host="secret", port=61979, db="01", user="root", password="secret",
                          charset="utf8")
        self.cursor = self.db.cursor()

        try:
            self.redis_cli = redis.Redis(host='secret', port=6480, password='secret', db=10,
                                     decode_responses=True)
            self.redis_cli2 = redis.Redis(host="secret", port=6379, db=1, password="secret",
                                          decode_responses=True)
        except Exception as e:
            print("连接redis数据库失败", e)

        self.useragent = UserAgent()

        #用来计算获取当前第几位号主的文章
        self.caculate_user = 0
        # 获取文章的url
        self.url = 'https://author.baidu.com/list?type=article&context={'
        self.timesiamp = '&_={}'
        self.context = '"offset":"-1_{}","app_id":"{}","last_time":"{}","pageSize":20'

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
            # 'Connection': 'keep-alive',
            'Cookie': 'BIDUPSID=9642A005C6C3D6F07C767CA906D5360D; PSTM=1525347482; BAIDUID=ED8B058FB7DA02227103D1CCF3A2F11B:FG=1; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; FP_UID=e695153e61b57e0d9203ab24d4bddde2; cflag=15%3A3; pgv_pvi=5318036480; H_PS_PSSID=26525_1461_21078_26808; PSINO=7',
            'Host': 'mbd.baidu.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',

        }
        self.f = open('error.txt', 'a+', encoding='utf-8')
        self.data = {"offset": 0, "user_id": "1566831857178535", "end_time": "1538943100", "source_url": "", "userName": ""}
        pass

    def __del__(self):
        self.db.close()
        self.f.close()
        pass

    def run(self): 
        while True:
            try:
                user_data = self.redis_cli.rpop('baiJiaHaoUser')
                bankup_user_data = user_data
                user_data = json.loads(re.sub('\'', '\"', user_data))

                # self.redis_cli.lpush('baiJiaHaoUser',user_data)  # 放回
                last_time = int(time.time())
                self.end_time = user_data['end_time']

                # 获取爬取的近3天文章
                if (last_time - self.end_time > 259200):
                    self.end_time = last_time - 259200
                else:
                    self.end_time = self.end_time

                self.offset = user_data['offset']
                userName = user_data['userName']
                userId = user_data['userId']
                source_url = 'https://baijiahao.baidu.com/u?app_id={}'.format(userId)

                res = self.get_res(source_url=source_url, userId=userId, last_time=last_time, offset=self.offset)

                time.sleep(0.2)

                self.parse_itemlist(res=res, userName=userName, source_url=source_url, userId=userId)

                #将一个用户的数据查看完，就把用户个人数据放回redis数据库
                user_data['end_time'] = self.end_time
                # self.redis_cli.lpush('baiJiaHaoUser', user_data)

            except Exception as e:
                self.redis_cli.lpush('baiJiaHaoUser', bankup_user_data)
                bankup_user_data = json.loads(re.sub('\'', '\"', bankup_user_data))
                print(bankup_user_data['userName'], "-->户主数据取出来拿来出来的过程中出了问题", e)

    def get_res(self, source_url, userId, last_time, offset):

        headers = {
            # GET /list?type=article&context={%22offset%22:%22-1_6%22,%22app_id%22:%221554130297341500%22,%22last_time%22:%221531896119%22,%22pageSize%22:14}&_=1531896156107&callback=jsonp3 HTTP/1.1
            'Host': 'author.baidu.com',
            'Accept': '*/*',
            'Connection': 'close',
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

        ip = self.redis_cli2.srandmember("IP")
        print('ip:', ip)

        proxies = {
            # "https":"https://{}".format(ip[0]),
            "https": "https://{}".format(ip),
            # "http": "http://125.126.208.11:22829"
        }

        res = requests.get(url=url, headers=headers, verify=False, proxies=proxies, timeout=3)
        time.sleep(0.01)
        return res

    # 解析文章列表
    def parse_itemlist(self, res, userName, source_url, userId):
        try:
            data = res.json()
            data = data['data']
        except:
            print("data['data']不存在，可能ip被封了")

        try:
            items = data['items']
        except:
            items = []

        try:
            if items == []:
                print("items为空")
                sql = "update baiduUser2 set last_time = '%s' where userId = '%s'" % ("", userId)
                self.cursor.execute(sql)
                self.db.commit()
                print("插入成功")
            else:
                print("items不为空")
                best_news = int(items[0]['created_at'])
                sql = "update baiduUser2 set last_time = '%s' where userId = '%s'" % (best_news, userId)
                self.cursor.execute(sql)
                self.db.commit()
                print("插入成功")
        except Exception as e:
            print("插入数据库异常，将回滚事务", e)
            self.db.rollback()


if __name__ == '__main__':
    for i in range(4):
        baidu = Baidu()
        w = threading.Thread(target=baidu.run)
        w.start()
