# coding:utf-8

import pymongo
import re
import json
import requests
import time
import redis

'''
百家号文章内容类别分类
'''

class ArticleCategory(object):
    def __init__(self):
        try:
            self.redis_cli2 = redis.Redis(host="secret", port=6379, db=1, password="secret",
                                      decode_responses=True)
        except:
            print("连接redis数据库失败")
            return
        try:
            mongoUri = 'mongodb://mongouser:password@ip:port/admin'
            client = pymongo.MongoClient(mongoUri)
            mDB = client.baijia
            self.collection = mDB.baijiaIncrement
        except Exception as e:
            print("连接mongo数据库失败", e)
            return

    def run(self):
        # client_id 为官网获取的AK， client_secret 为官网获取的SK
        url = 'https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id=XiFkuZS2Lr4UytaArMUhHP9k&client_secret=dnmZ5M9n5OUDNf0TLgx3PNRGSH6LbHVL'
        headers = {
            'Content-Type': 'application/json; charset=UTF-8'
        }
        response = requests.get(url, headers=headers)
        content = response.text
        if content:
            return json.loads(content)["access_token"]

    def get_split(self, access_token):
        while True:
            last_time = int(time.time())
            print("开始更新文章分类的时间:", last_time)
            create_time = last_time - 259200
            results = self.collection.find({"create_time": {"$gte": create_time}}, no_cursor_timeout=True)
            temp = 0
            for result in results:
                if result['category'] == 'news':
                    print("文章链接：", result['source_url'])
                    url = 'https://aip.baidubce.com/rpc/2.0/nlp/v1/topic?access_token={}'.format(access_token)
                    content = result['article_content']
                    title = result['title']
                    pattern = re.compile(r'<[^>]+>', re.S)
                    content = pattern.sub('', content)
                    #把内容中的双引号改为单引号，不改会导致无效属性的错误
                    content = content.replace("\"", "\'")
                    if len(content) > 30000:
                        content = content[0:30000]
                    headers = {
                        'Content-Type': 'application/json'
                    }
                    data = '{"title": \"%s\", "content": \"%s\"}' % (title, content)
                    try:
                        data = data.encode('gbk')
                    except:
                        print("解码出问题")
                        data = data.encode("gbk", "ignore")
                    try:
                        response = requests.post(url, data=data, headers=headers, timeout=3)
                    except:
                        # 如果出错，应该就是access_token过期了，重新获取access_token的值
                        print("access_token过期，重新获取access_token的值")
                        access_token = self.run()
                        token_url = 'https://aip.baidubce.com/rpc/2.0/nlp/v1/topic?access_token={}'.format(access_token)
                        response = requests.post(token_url, data=data, headers=headers)
                    try:
                        category = json.loads(response.content.decode("gbk"))['item']['lv1_tag_list']
                    except:
                        print("可能访问百度云API接口过快，将适当调整再启动...")
                        print("错误文章链接：", result['source_url'])
                        time.sleep(1)
                        response = requests.post(url, data=data, headers=headers, timeout=3)
                        try:
                            category = json.loads(response.content.decode("gbk"))['item']['lv1_tag_list']
                        except:
                            continue

                    category = eval((str(category))[1:-1])
                    category = category['tag']
                    temp += 1
                    print("更新文章分类到了第", temp, "条")
                    if category:
                        self.collection.update(
                            {"source_url": result['source_url']},
                            {
                                '$set': {"category": category}
                            })
                        print("-------------", result['source_url'], "<---->", result['category'], "-------------")
                        continue
                    else:
                        self.collection.update(
                            {"source_url": result["source_url"]},
                            {
                                '$set': {"category": "其他"}
                            })
                        continue

                else:
                    print("========文章类别不用更新========", result['source_url'], result['category'])
                    continue

            print("========更新一次文章分类完成=======")
            # self.collection.close()

if __name__=='__main__':
    articleCategory = ArticleCategory()
    access_token = articleCategory.run()
    articleCategory.get_split(access_token)
