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
        self.redis_cli = redis.Redis(host='xxx', port=6379, db=0, password='xxx', charset='utf8', decode_responses=True)
        self.start = 0

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
        #API签名字符串
        para = 'xxx' + 'xxx.com' + str(mt)
        sign = hashlib.md5(para.encode(encoding='UTF-8')).hexdigest()
        #媒体ID(即用户ID)
        mid = 0
        #文章内容
        content = response.find_all('div', {'id': 'content'})[0]
        try:
            #文章摘要
            describe = content.get_text().strip().split('。')[0]
        except Exception as e:
            print('describe is wrong!!!', e)
            describe = ''
        #文章图片
        try:
            img_url = json.dumps([con.get('src') for con in content.find_all('img')])
        except Exception as e:
            print('img_url is wrong!!!', e)
            img_url = '[]'
        #文章标题
        title = response.find_all('h1', {'class': 'title'})[0].get_text().strip()
        if len(title) == 0:
            return
        #文章URL
        url = 'https://mbd.baidu.com/newspage/data/landingshare?context={}'.format(context)
        #文章作者
        author = response.find_all('div', {'class': 'name'})[0].get_text().strip()
        #作者logo
        full_avatar = response.find_all('div', {'class': 'author-level'})[0].find_all('img')[0].get('src')
        avatar = full_avatar.split('src')[-1]
        avatar = unquote(unquote(avatar)[1:])  #对编码的网址进行解码
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
        #文章来源
        spider = '淘金阁'
        source = '百家'
        #创建时间
        create_time = int(time.time())

        content = str(content).strip()

        print('content: ', content)
        print('describe: ', describe)
        print('img_url: ', img_url)
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
            'img_url': img_url,
            'author': author,
            'author_logo': avatar,
            'spider': spider,
            'source': source,
            'describe': describe,
            'read_count': read_count,
            'comment_count': comment_count,
            'publish_time': publish_time,
            'content': content,
            'create_time': create_time
        }

        if len(content) > 10:
            #文章信息存储
            try:
                article_test_url = 'http://xxx'
                res = requests.post(article_test_url, data=items)
                print(res.text)
            except Exception as e:
                print('insert wrong!!!!', e)
      
        try:
            self.get_thread_id(url)
        except Exception as e:
            print('get_thread_id is wrong!!!', e)

    def get_thread_id(self,url):
        ua = UserAgent()
        hesders = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': 'BAIDUID=A105AAAEDB8417FEB897AF399BD16129:FG=1; PSTM=1544404985; BIDUPSID=27BE7DB527DC587772070DFEBD73CC92; x-logic-no=5; BDSFRCVID=v4AOJeCwM6l72iO95h9TUmjK9KSqThrTH6aoSKHXf57azABHiLP7EG0PjU8g0Kubh02GogKK3gOTH4PF_2uxOjjg8UtVJeC6EG0P3J; H_BDCLCKID_SF=tJP8VI-XJDD3fP36qRbHhRD8hlrt2D62aKDs2qL2BhcqEIL4QntM3MISetkJ3tQPaCLH_DoXXMOOOxbSj4QoX4Ar-Jr3QUTW0Ktebhn15p5nhMJNb67JDMP0-mci243y523iob3vQpPMVhQ3DRoWXPIqbN7P-p5Z5mAqKl0MLIOkbC_Ce5K2DT50eNLjJj3bKCnKsJOOaCvseKJOy4oTj6Dj5-IfLjL8LGvD3M5E2Do8DIQDQnjj3MvB-trhK63CQjreLpc-yRnASJRKQft20-LIeMtjBbQaK23C0b7jWhk5ep72y5OmQlRX5q79atTMfNTJ-qcH0KQpsIJMDUC0D63-DGLeJ6Ksb5vfsJcV5-5HDR3g-trSMDCShUFsWMnJB2Q-5KL-3bbvJbIGeJrGQ-uAhf6ihqLDLgQ92MbdJJjoqMOF-PbVKJ-QXMJHLf73QeTxoUJ_MInJhhvG-CcaMMLebPRiJ-b9Qg-JbpQ7tt5W8ncFbT7l5hKpbt-q0x-jLn7ZVD_2tD_BhC0CePA_24kthfQH-4oX2D5KWjrJabC38M7VXU6qLn-IjxkfJtc9fm0q_tQGaRcvHpTbKqOsKl0njxQyQMn7yIFt_bjL0lo1jqRMjUonDh8q3H7MJUntKD-joq3O5hvvhn3O3MAM0MKmDloOW-TB5bbPLUQF5l8-sq0x05bke6j3DGK8Jj-qfK-XXJnVaRjKfJjkM4rHhCCShUFs-j3CB2Q-5-3zJJ4bhRTOejoseq0Ahf6iqxng-Gn3QfbdJJjoVpvSW-DhjP_hLt57ax7TJ2TxoUJ_QCnJhhvG-4A2Dx-ebPRiJ-b9Qg-JbpQ7tt5W8ncFbT7l5hKpbt-q0x-jLn7ZVJO-KKCKMIIleMK; PSINO=6; delPer=0; ZD_ENTRY=google; pgv_pvi=7889258496; pgv_si=s1887410176; H_PS_PSSID=1467_21092_28206_28132_28267_28140; H_WISE_SIDS=126124_126707_128491_114550_127684_128065_127483_125696_120141_123018_128619_118878_118861_118851_118832_118804_127181_128039_128457_107320_126995_127772_127405_127768_117332_117435_128450_128402_127836_128589_127807_124627_128447_128441_128498_128246_128004_124938_127796_126722_128525_127872_127764_128240_124030_128341_110085_123289_127124_127319_127379_128604_127417; BDUSS=U4xZ2dpSEI3ZVd2elBvd0psREdSVjcxbWd4V0UyVFhrU3MwYWswVE5jYlBIR0JjQVFBQUFBJCQAAAAAAAAAAAEAAACbM7VGy7XBy9TZvPtnaGYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM-POFzPjzhcZ',
            'Host': 'mbd.baidu.com',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': ua.random,
            #'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
        }
        res = requests.get(url, headers=hesders, timeout=5, verify=False).text
        time.sleep(random.randint(1, 2) / 4)
        response = BeautifulSoup(res, 'lxml')
        data = response.find_all('script',{'type':'text/javascript'})[1]
        soup = str(data).replace('<script type="text/javascript">(function (global) {global.jsonData=', '')
        soup = str(soup).replace(";global.host = global.jsonData.data.pageInfo.common.host || 'https://mbd.baidu.com';})(typeof(page) === 'undefined' ? window : page);</script>",'')
        soup = json.loads(soup)

        thread_id = soup['data']['pageInfo']['common']['thread_id']
        print('thread_id', thread_id)

        try:
            self.get_baidu_comment(thread_id, url)
        except Exception as e:
            print('get_baidu_comment: ', e)

    def get_baidu_comment(self, tid, url):
        page = self.start
        source_url = 'https://ext.baidu.com/api/comment/v1/comment/getlist?appid=101&sid=t7&cuid=&isInf=1&start={}&num=20&use_uk=1&use_list=1&is_need_at=1&order=12&thread_id={}'.format(page, tid)
        headers = {
            #'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
            'Host': 'ext.baidu.com',
            'Connection': 'keep-alive',
            'Accept': '*/*',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 4.4.2; OPPO R11 Build/NMF26X) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36 light/1.0 baiduboxapp/8.5 (Baidu; P1 4.4.2)',
            'Accept-Encoding': 'gzip,deflate',
            'Accept-Language': 'zh-CN,en-US;q=0.8',
            'X-Requested-With': 'com.baidu.searchbox',
            'Cookie': 'BAIDUID=B3064AC5527FB50280DEBDE2C795D389:FG=1; MBD_AT=1547002997; WISE_HIS_PM=1; H_WISE_SIDS=125819_126125_128490_128700_127238_128070_127484_123018_125580_120176_128638_127769_107320_118893_118870_118847_118830_118802_128364_127771_128038_117331_126167_128402_128005_128246_127404_128451_117430_128967_128869_128448_128589_126995_124625_128819_128790_127796_127990_126720_128805_127873_127764_125873_128684_128824_128533_124030_110086_127229_128480_123289_128602_127128_127380_127994_128195_128962; delPer=0; PSINO=6; BAIDUCUID=gaSC8l8jSign8BuojaHmaYak2a0Savut_8Hb80uJ2i8wu28x_a2O8_agvi_Da2fHA; BAIDULOC=12197937_3611197_100_132_1548310001406; _ucmt_ls=1103000014292851-1109151708107069914-1109151708021065512-1109151703644768912-1109151698670755211-1109151686395095111-1109151684911662215-1109151683591026613-1109151681877653114-1109151681478499219-1109151679883941800-1109151676548689714-1109151676409786319-1109151676396931414-1109151674468933511-1109151670047576610-1109151651493177316-1109151638790299113-1109151637960064432-1109151637216232134-1109151636845301318-30'
        }
        resp = requests.get(source_url, headers=headers, timeout=5, verify=False).json()
        time.sleep(random.randint(1, 2) / 8)
        data_list = resp['ret']['list']
        if data_list == []:
            return
        for data in data_list:
            #当前请求Unix时间戳
            mt = int(time.time())
            #API签名字符串
            para = 'xxx' + 'xxx.com' + str(mt)
            sign = hashlib.md5(para.encode(encoding='UTF-8')).hexdigest()
            #评论用户名称
            user_name = data['uname']
            #评论用户头像链接
            user_img_url = data['avatar']
            #评论内容text
            text = data['content']
            #评论时间
            create_time = data['create_time']
            #评论内容点赞数
            digg_count = data['like_count']
            #评论回复数
            reply_comment = data['reply_list']
            reply_count = len(reply_comment)

            print('user_name: ', user_name)
            print('user_img_url: ', user_img_url)
            print('text: ', text)
            print('create_time: ', create_time)
            print('digg_count: ', digg_count)
            print('reply_count: ', reply_count)
            print('url: ', url)

            #获取回复comment
            reply_list = []
            if reply_count > 0:
                for comment in reply_comment:
                    # 回复内容text
                    text = comment['content']
                    # 回复时间
                    create_time = comment['create_time']
                    # 点赞数
                    digg_count = comment['like_count']
                    # 用户名
                    user_name = comment['uname']
                    # 用户头像链接
                    avatar_url = comment['avatar']

                    items = {
                        'nickname': user_name,
                        'avatar': avatar_url,
                        'content': text,
                        'fabulous': digg_count,
                        'comment_time': create_time,
                    }
                    print('items：', items)
                    reply_list.append(items)

            reply_list = json.dumps(reply_list)
            print('reply_list: ', reply_list)

            items = {
                'mt': mt,
                'sign': sign,
                'arc_url': url,
                'nickname': user_name,
                'avatar': user_img_url,
                'content': text,
                'reply': reply_count,
                'fabulous': digg_count,
                'comment_time': create_time,
                'reply_list': reply_list,
            }

            #文章评论信息存储
            try:
                comment_test_url = 'http://xxx'
                body = requests.post(comment_test_url, data=items)
                print(body.text)
            except Exception as e:
                print('insert db wrong!!!!', e)

        total_number = len(data_list)
        if total_number < 20:
            return
        else:
            self.start += 20
            if self.start > 80:
                return
            self.get_baidu_comment(tid, url)

    def run(self):
        while True:
            data = self.redis_cli.lpop('spider_tjg_baijia_article')
            print(type(data), data)
            if data == None:
                time.sleep(600)
                continue
            data = eval(data)  #str转成dict
            self.start = 0

            try:
                self.get_baidu_article(data)
            except Exception as e:
                print('get_baidu_article is wrong!!!', e)
                time.sleep(5)

if __name__ == "__main__":
    for i in range(5):
        b = Baidu()
        work_thread = Thread(target=b.run)
        work_thread.start()
