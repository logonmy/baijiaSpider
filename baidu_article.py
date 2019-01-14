# coding=utf-8

import requests
import json
import re
import redis
import time
import random
import hashlib
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from urllib.parse import unquote
from datetime import datetime
from threading import Thread
from requests.packages.urllib3.exceptions import InsecureRequestWarning
#禁用安全请求警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class Baidu(object):
    def __init__(self):
        self.redis_cli = redis.Redis(host='xxx', port=6379, db=1, password='xxx', charset='utf8', decode_responses=True)

    def get_baidu_article(self,item):
        context = item['context']
        source_url = 'http://rym.quwenge.com/baidu_tiaozhuan.php?url=https://baijiahao.baidu.com/po/feed/share?context={}'.format(context)
        ua = UserAgent()
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Host': 'rym.quwenge.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': ua.random
        }
        resp = requests.get(source_url, headers=headers, timeout=5, verify=False).text
        time.sleep(random.randint(1, 2) / 4)
        response = BeautifulSoup(resp, 'lxml')

        #当前请求Unix时间戳
        mt = int(time.time())
        # API签名字符串
        para = 'xxx + 'xxx' + str(mt)
        sign = hashlib.md5(para.encode(encoding='UTF-8')).hexdigest()
        #媒体ID(即用户ID)
        mid = 0
        #文章内容
        content = response.find_all('div', {'id': 'content'})[0]
        #文章摘要
        describe = content.get_text().strip().split('。')[0]
        #文章图片
        #img_list = [con.get('data-bjh-origin-src') for con in content.find_all('img')]
        img_list = json.dumps([con.get('src') for con in content.find_all('img')])
        #文章标题
        title = response.find_all('h1', {'class': 'title'})[0].get_text().strip()
        #文章URL
        url = 'https://mbd.baidu.com/newspage/data/landingshare?context={}'.format(context)
        #文章作者
        author = response.find_all('div', {'class': 'name'})[0].get_text().strip()
        #作者logo
        full_avatar = response.find_all('div', {'class': 'author-level'})[0].find_all('img')[0].get('src')
        avatar = full_avatar.split('src')[-1]
        avatar = unquote(unquote(avatar)[1:])  # 对编码的网址进行解码
        #文章的发布时间
        try:
            time_str = response.find_all('span', {'class': 'read'})[0].get_text().strip()
            time_d = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            publish_time = int(time.mktime(time_d.timetuple()))
        except:
            publish_time = item['publish_time']
        #文章阅读数
        read_count = item['read_count']
        #文章评论数
        comment_count = item['comment_count']
        #文章所在平台
        platform = 'baidu'
        #文章频道id
        channel_id = item['channel_id']

        content = str(content).strip()

        print('content: ', content)
        print('describe: ', describe)
        print('img_list: ', img_list)
        print('title: ', title)
        print('url: ', url)
        print('author: ', author)
        print('avatar: ', avatar)
        print('publish_time: ', publish_time)
        print('read_count: ', read_count)
        print('comment_count: ', comment_count)
        print('platform: ', platform)
        print('channel_id: ', channel_id)

        items = {
            'mt': mt,
            'sign': sign,
            'mid': mid,
            'channel_id': channel_id,
            'url': url,
            'title': title,
            'img_url': img_list,
            'author': author,
            'author_logo': avatar,
            'spider': '淘金阁',
            'source': '百家',
            'describe': describe,
            'read_count': read_count,
            'comment_count': comment_count,
            'publish_time': publish_time,
            'content': content,
        }

        if len(content) > 10:
            # 文章信息存储
            try:
                url = 'xxxx'
                requests.post(url, data=items)
            except Exception as e:
                print('insert wrong!!!!', e)

        self.get_thread_id(url)

    def get_thread_id(self,url):
        ua = UserAgent()
        hesders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': 'BAIDUID=A105AAAEDB8417FEB897AF399BD16129:FG=1; PSTM=1544404985; BIDUPSID=27BE7DB527DC587772070DFEBD73CC92; x-logic-no=5; BDSFRCVID=v4AOJeCwM6l72iO95h9TUmjK9KSqThrTH6aoSKHXf57azABHiLP7EG0PjU8g0Kubh02GogKK3gOTH4PF_2uxOjjg8UtVJeC6EG0P3J; H_BDCLCKID_SF=tJP8VI-XJDD3fP36qRbHhRD8hlrt2D62aKDs2qL2BhcqEIL4QntM3MISetkJ3tQPaCLH_DoXXMOOOxbSj4QoX4Ar-Jr3QUTW0Ktebhn15p5nhMJNb67JDMP0-mci243y523iob3vQpPMVhQ3DRoWXPIqbN7P-p5Z5mAqKl0MLIOkbC_Ce5K2DT50eNLjJj3bKCnKsJOOaCvseKJOy4oTj6Dj5-IfLjL8LGvD3M5E2Do8DIQDQnjj3MvB-trhK63CQjreLpc-yRnASJRKQft20-LIeMtjBbQaK23C0b7jWhk5ep72y5OmQlRX5q79atTMfNTJ-qcH0KQpsIJMDUC0D63-DGLeJ6Ksb5vfsJcV5-5HDR3g-trSMDCShUFsWMnJB2Q-5KL-3bbvJbIGeJrGQ-uAhf6ihqLDLgQ92MbdJJjoqMOF-PbVKJ-QXMJHLf73QeTxoUJ_MInJhhvG-CcaMMLebPRiJ-b9Qg-JbpQ7tt5W8ncFbT7l5hKpbt-q0x-jLn7ZVD_2tD_BhC0CePA_24kthfQH-4oX2D5KWjrJabC38M7VXU6qLn-IjxkfJtc9fm0q_tQGaRcvHpTbKqOsKl0njxQyQMn7yIFt_bjL0lo1jqRMjUonDh8q3H7MJUntKD-joq3O5hvvhn3O3MAM0MKmDloOW-TB5bbPLUQF5l8-sq0x05bke6j3DGK8Jj-qfK-XXJnVaRjKfJjkM4rHhCCShUFs-j3CB2Q-5-3zJJ4bhRTOejoseq0Ahf6iqxng-Gn3QfbdJJjoVpvSW-DhjP_hLt57ax7TJ2TxoUJ_QCnJhhvG-4A2Dx-ebPRiJ-b9Qg-JbpQ7tt5W8ncFbT7l5hKpbt-q0x-jLn7ZVJO-KKCKMIIleMK; PSINO=6; delPer=0; ZD_ENTRY=google; pgv_pvi=7889258496; pgv_si=s1887410176; H_PS_PSSID=1467_21092_28206_28132_28267_28140; BD_BOXFO=_a2Oiguq2_GsC; H_WISE_SIDS=126124_126707_128491_114550_127684_128065_127483_125696_120141_123018_128619_118878_118861_118851_118832_118804_127181_128039_128457_107320_126995_127772_127405_127768_117332_117435_128450_128402_127836_128589_127807_124627_128447_128441_128498_128246_128004_124938_127796_126722_128525_127872_127764_128240_124030_128341_110085_123289_127124_127319_127379_128604_127417',
            'Host': 'mbd.baidu.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': ua.random,
        }
        res = requests.get(url=url, headers=hesders, timeout=5, verify=False).text
        response = BeautifulSoup(res, 'lxml')
        data = response.find_all('script',{'type':'text/javascript'})[1]
        soup = str(data).replace('<script type="text/javascript">(function (global) {global.jsonData=', '')
        soup = str(soup).replace(";global.host = global.jsonData.data.pageInfo.common.host || 'https://mbd.baidu.com';})(typeof(page) === 'undefined' ? window : page);</script>",'')
        soup = json.loads(soup)

        thread_id = soup['data']['pageInfo']['common']['thread_id']
        print(thread_id)

    def run(self):
        while True:
            data = self.redis_cli.lpop('spider_baijia_article')
            if data == None:
                time.sleep(600)
                continue
            data = eval(data)  # str转成dict

            try:
                self.get_baidu_article(data)
            except Exception as e:
                print(e)
                continue

if __name__ == "__main__":
    for i in range(5):
        t = Baidu()
        work_thread = Thread(target=t.run)
        work_thread.start()
