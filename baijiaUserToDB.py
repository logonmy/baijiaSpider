# coding:utf-8

from pymysql import connect
import redis
import json
import re

'''
百家号存入MySQL或者Redis
'''

class DB(object):
    def __init__(self):
        self.redis_cli = redis.Redis(host="secret", port=6480, db=5, password="secret",
                                     decode_responses=True)
        self.db = connect(host="secret", port=61979, db="zhan_db", user="root", password="secret",
                          charset="utf8")
        self.cursor = self.db.cursor()
    
    #百家号存入Redis
    def save_to_redis(self):
        # SELECT id,userId,mid FROM HZ1
        sql = "SELECT userName,userId,source_url from baiduUser9"
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        print(type(data))
        print('===== start =====')
        num = 0
        for d in data:
            print('从mysql把户主数据导入redis数据库中，正在导入第', num + 1, '个户主')
            # self.redis_cli2.sadd('baiduUserId',d[0])
            d = list(d)
            user_dict = {}
            user_dict['userName'] = d[0]
            user_dict['userId'] = d[1]
            user_dict['source_url'] = d[2]
            user_dict['offset'] = 0
            user_dict['end_time'] = 1514736000
            #
            self.redis_cli.lpush('baijia_2018_history_data', user_dict)
            num += 1
        print('over', num)
    
    #百家号存入MySQL
    def save_to_mysql(self):
        try:
            all_data = self.redis_cli.lrange('baijia_newUsers', 0, -1)
        except Exception as e:
            print("出错")
            return
        # print("获取到值了", type(all_data))

        k = 0
        try:
            for i in all_data:
                i = json.loads(re.sub('\'', '\"', i))
                sql = "insert into baiduUser7 (userName,userId,source_url,last_time) values ('%s', '%s', '%s', '%s')" % (
                i['userName'], i['userId'], i['source_url'], i['end_time'])
                self.cursor.execute(sql)
                k = k + 1
                if (k > 1000):
                    self.db.commit()
                    print("插入成功！")
                    k = 0

            self.db.commit()
        except Exception as e:
            print("sql语句出错", e)
            return
        print("完成导入")

if __name__ == '__main__':
    save_db = DB()
    save_db.save_to_redis()
    save_db.save_to_mysql()
