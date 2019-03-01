# coding=utf-8

import requests
import time
import redis
import re
from lxml import etree
from datetime import datetime
import pymysql

class Taojinge(object):
    def __init__(self):
        self.redis_cli = redis.Redis(host='xxx', port=6379, db=0, password='xxx', charset='utf8', decode_responses=True)
        self.db = pymysql.connect(host='xxx', port=3306, db='xxx', user='xxx', password='xxx', charset='utf8')
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

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'www.51taojinge.com',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'http://www.51taojinge.com/uc/index.php',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Mobile Safari/537.36',
            'Cookie': 'UM_distinctid=167d43a934b681-055290f7067ee4-3a3a5d0c-1fa400-167d43a934c288; Hm_lvt_72aa476a79cf5b994d99ee60fe6359aa=1549950240; Hm_lpvt_72aa476a79cf5b994d99ee60fe6359aa=1549950240; token=009784b18a2dda2c93726ea63a88b92c5eacd73c; uid=21030; phone=18927476407; viptime=1565511521; toutiaoURLname=www.51taojinge.com; CNZZDATA1261342782=1429839834-1545452194-null%7C1551254261'
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
        if self.page > 100:
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

            #print(source_url)

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
                info = self.cursor.execute(sql, (
                item['title'], item['url'], item['publish_time'], item['read_count'], item['comment_count'], item['platform'],
                item['tag'],item['create_time'],item['channel_id']))
                self.db.commit()
                #print('ok!')
            except Exception as e:
                print('insert sql is wrong! ', e)
                self.db.rollback()
                info = 0

            if info == 1:
                self.redis_article(item)

    def redis_article(self, item):
        platform = item['platform']
        items = {
            'read_count': item['read_count'],
            'comment_count': item['comment_count'],
            'publish_time': item['publish_time'],
            'channel_id': item['channel_id']
        }

        if platform == 'baidu':
            url = item['url']
            context = url.split('context=')[-1]
            items['context'] = context
            self.redis_cli.rpush('spider_tjg_baijia_article', str(items))
        elif platform == 'toutiao':
            url = item['url']
            pattern = re.compile('\d+')
            cid = re.findall(pattern, url)[0]
            items['content_id'] = cid
            self.redis_cli.rpush('spider_tjg_toutiao_article', str(items))
        '''
        elif platform == 'uc':
            url = item['url']
            article_id = url.split('wm_aid=')[-1]
            items['article_id'] = article_id
            self.redis_cli.rpush('spider_tjg_dayu_article', str(items))
        elif platform == 'kuaibao':
            url = item['url']
            article_id = url.split('s')[-1][1:-1].strip()
            items['article_id'] = article_id
            self.redis_cli.rpush('spider_tjg_kuaibao_article', str(items))
        elif platform == 'souhu':
            url = item['url']
            article_id = url.split('/')[-1].strip()
            items['article_id'] = article_id
            self.redis_cli.rpush('spider_tjg_sohu_article', str(items))
        elif platform == 'wangyi':
            url = item['url']
            article_id = url.split('/')[-1].strip()[:-5]
            items['article_id'] = article_id
            self.redis_cli.rpush('spider_tjg_wangyi_article', str(items))
        elif platform == 'fenghuang':
            url = item['url']
            article_id = url.split('=')[-1].strip()
            items['article_id'] = article_id
            self.redis_cli.rpush('spider_tjg_fenghuang_article', str(items))
        else:
            url = item['url']
            items['url'] = url
            self.redis_cli.rpush('spider_tjg_kandian_article', str(items))
        '''

    def time_task(self):
        now_stamp = int(time.time())
        end_stamp = now_stamp - 43200  # 12h(删除12h的内容)
        sql = '''DELETE from jjb_tjg_article WHERE publish_time < %d''' % (end_stamp)
        try:
            self.cursor.execute(sql)
            self.db.commit()
            #print('ok!')
        except Exception as e:
            print('DELETE sql is wrong! ', e)
            self.db.rollback()

    def run(self):
        while True:
            end_stamp = int(time.time()) - 1800
            start_stamp = end_stamp - 43200  #12h
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
                self.time_task()
            except Exception as e:
                print('delete data is wrong!!!',e)
            try:
                items = eval(items)
                self.get_tjg_page(items,str_time,end_time)
            except Exception as e:
                print(e)

if __name__ == "__main__":
    t = Taojinge()
    t.run()
