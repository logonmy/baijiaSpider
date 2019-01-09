# coding=utf-8

import requests
import time
import redis
from lxml import etree
from datetime import datetime
import pymysql

class Taojinge(object):
    def __init__(self):
        self.redis_cli = redis.Redis(host='192.168.0.21', port=6379, db=1, password='123456', charset='utf8', decode_responses=True)
        self.db = pymysql.connect(host='192.168.0.21', port=3306, db='db_juejinlian', user='user_juejinlian', password='ac21acWq18E2', charset='utf8')
        self.cursor = self.db.cursor()
        self.page = 1

    def __del__(self):
        self.db.close()

    def get_tjg_page(self,items,str_time,end_time):
        platform = items['platform']
        tag_en = items['tag_en']
        orderY = '4'
        page = str(self.page)

        url = 'http://www.51taojinge.com/{}/index.php?tag={}&count=&str_time={}&end_time={}&orderY={}&page={}'.format(platform,tag_en,str_time,end_time,orderY,page)
        print(url)

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'www.51taojinge.com',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'http://www.51taojinge.com/uc/index.php',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Mobile Safari/537.36'
        }
        try:
            response = requests.get(url, headers=headers,timeout=5).text
            time.sleep(1.1)
            resp = etree.HTML(response)
            data_list = resp.xpath('/html/body/div[1]/div[2]/div/div/table/tbody/tr')
            self.parse_page(data_list, items)
            if len(data_list) < 15:
                return
        except Exception as e:
            print('something is wrong!!',e)

        self.page += 1
        if self.page > 10:
            return
        self.get_tjg_page(items,str_time,end_time)


    def parse_page(self, data_list, items):
        for data in data_list:
            #文章url
            source_url = data.xpath('./td[2]/a/@href')[0].strip()
            #文章标题
            title = data.xpath('./td[2]/a/text()')[0].strip()
            #文章评论数
            comment_count = data.xpath('./td[3]/text()')[0].strip()
            #文章阅读数
            read_count = data.xpath('./td[4]/text()')[0].strip()
            #文章发布时间
            time_str = data.xpath('./td[6]/text()')[0].strip()
            time_d = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            publish_time = int(time.mktime(time_d.timetuple()))
            #文章所在平台
            platform = items['platform']
            #文章类型
            tag = items['tag_cn']
            #文章类型ID(jjb)
            channel_id = items['channel_id']
            #创建时间
            create_time = int(time.time())

            print(source_url)
            print(title)
            print(comment_count)
            print(read_count)
            print(publish_time)
            print(platform)
            print(tag)
            print(channel_id)

            item = {
                'title': title,
                'url': source_url,
                'publish_time': publish_time,
                'read_count': read_count,
                'comment_count': comment_count,
                'platform': platform,
                'tag': tag,
                'create_time': create_time,
                'channel_id': channel_id
            }

            sql = """replace into jjb_tjg_article(title,url,publish_time,read_count,comment_count,platform,tag,create_time,channel_id)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            try:
                self.cursor.execute(sql, (
                item['title'], item['url'], item['publish_time'], item['read_count'], item['comment_count'], item['platform'],
                item['tag'],item['create_time'],item['channel_id']))
                self.db.commit()
                print('ok!')
            except Exception as e:
                print('insert sql is wrong! ', e)
                self.db.rollback()


    def run(self):
        while True:
            end_stamp = int(time.time()) - 1800
            start_stamp = end_stamp - 43200  # 12h
            e_time = time.localtime(end_stamp)
            s_time = time.localtime(start_stamp)
            e_d = time.strftime("%Y-%m-%d", e_time)
            e_h = time.strftime("%H", e_time)
            e_m = time.strftime("%M", e_time)
            s_d = time.strftime("%Y-%m-%d", s_time)
            s_h = time.strftime("%H", s_time)
            s_m = time.strftime("%M", s_time)

            end_time = str(e_d) + '+' + str(e_h) + '%3A' + str(e_m) + '%3A00'
            str_time = str(s_d) + '+' + str(s_h) + '%3A' + str(s_m) + '%3A00'

            self.page = 1
            data = self.redis_cli.lpop('spider_tjg_tag')
            items = str(data)
            self.redis_cli.rpush('spider_tjg_tag', items)
            try:
                items = eval(items)
                self.get_tjg_page(items,str_time,end_time)
            except Exception as e:
                print(e)

if __name__ == "__main__":
    t = Taojinge()
    t.run()
