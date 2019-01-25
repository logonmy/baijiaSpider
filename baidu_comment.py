#coding=utf-8

import requests
import json
import time
import hashlib

class Comment(object):
    def __init__(self):
        self.start = 0

    def get_baidu_comment(self, tid, url):
        page = self.start
        source_url = 'https://ext.baidu.com/api/comment/v1/comment/getlist?appid=101&sid=t7&cuid=&isInf=1&start={}&num=20&use_uk=1&use_list=1&is_need_at=1&order=12&thread_id={}'.format(page,tid)
        headers = {
            # 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
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
                    
                    reply_list.append(items)

            reply_list = json.dumps(reply_list)

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

        total_number = len(data_list)
        if total_number < 20:
            return
        else:
            self.start += 20
            if self.start > 80:
                return
            self.get_baidu_comment(tid, url)

if __name__ == "__main__":
    c = Comment()
    tid = 1118000014328218
    url = 'https://baijiahao.baidu.com/po/feed/share?context={"sourceFrom"%3A"bjh"%2C"nid"%3A"news_9951842500806386818"}'
    c.get_baidu_comment(tid,url)
