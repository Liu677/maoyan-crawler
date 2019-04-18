#-*-coding:utf8-*-\
import re
import os
import requests
import pymysql
from fontTools.ttLib import TTFont
from lxml import etree

class spider:
    def __init__(self,myheaders):
        self.myheaders = myheaders
        self.faildid = []
        print('开始爬取嘀嘀嘀')

    def returnpage(self): #返回一个url列表
        first_url = 'https://maoyan.com/films?showType=3&sourceId=2&yearId=10&offset=0' #15年国产电影第一页
        total_page = [45,67,57,47,22] #2015年至2019年国产电影在猫眼电影上显示的页数
        url_group = [] #url列表
        for j in range(0,5):
            each_url = re.sub('yearId=\d+&offset', 'yearId=%s&offset' % (10+j), first_url, re.S)
            for i in range(0,total_page[j]):
                each_url = re.sub('offset=\d+','offset=%s'%(30*i),each_url,re.S)
                url_group.append(each_url)
        return url_group

    def getid(self,url): #返回一个url的所有id组成的列表
        print('开始爬取'+url)
        html = requests.get(url, headers=self.myheaders).text
        id_group = re.findall('"{movieid:(.*?)}">', html, re.S)
        return id_group

    def code2num(self,HexNum,woffFileName): #猫眼上的数字如评分，票房都经过了加密处理，这一步将爬到的编码转换成对应的数字
        HexNum = 'uni'+HexNum.upper()
        base_num = dict()  # 编号—数字
        base_obj = dict()  # 编号—对象
        base_num["uniF444"] = "0"
        base_num["uniF852"] = "1"
        base_num["uniE254"] = "2"
        base_num["uniE0A2"] = "3"
        base_num["uniF1F3"] = "4"
        base_num["uniF3BD"] = "5"
        base_num["uniEDBB"] = "6"
        base_num["uniE2EC"] = "7"
        base_num["uniEFFF"] = "8"
        base_num["uniE762"] = "9"
        BaseFontfile = TTFont('woff\\base.woff')
        try:
            for key in base_num:
                base_obj[key] = BaseFontfile['glyf'][key]
            fontFile = TTFont(woffFileName)
            obj = fontFile['glyf'][HexNum]
            for key in base_obj:  # 遍历找到相同的字体对象
                if obj == base_obj[key]:
                    return base_num[key]
        except:
            return '0'


    def geteachmvinfo(self,id): #返回一个id所对应电影的所有信息的字典
        #电影id、电影名称、演员信息、上映时间、用户评分、评论人数、累计票房等
        info = {}
        try:
            mvurl = 'https://maoyan.com/films/' + id
            mvhtml = requests.get(mvurl, headers=self.myheaders).text
            selector = etree.HTML(mvhtml)
            info['id'] = id
            try:
                info['name'] = selector.xpath('/html/body/div[3]/div/div[2]/div[1]/h3/text()')[0]
            except IndexError:
                info['name'] = '暂无信息'

            try:
                actorspart = re.search('演员\n  </div>(.*?)</ul>\n</div>', mvhtml, re.S).group(1)
                actors = re.findall('target="_blank" class="name">\n      (.*?)\n', actorspart, re.S)
                info['cast']=''
                for actor in actors:
                    info['cast'] = info['cast']+actor+','
                info['cast'] = info['cast'][:-1]
            except AttributeError:
                info['cast'] = ['未知']

            try:
                info['time'] = selector.xpath('/html/body/div[3]/div/div[2]/div[1]/ul/li[3]/text()')[0]
            except IndexError:
                info['time'] = '未知'

            woff_url = 'http://vfile' + re.search("vfile(.*?)woff",mvhtml).group(1) + 'woff' #下载.woff文件
            local_name = 'woff\\' + os.path.basename(woff_url)
            if not os.path.exists(local_name): #可能会有一样的woff就不用下载了
                with open(local_name, 'wb+') as f:
                    f.write(requests.get(woff_url).content)

            try:
                scorepart = re.search('<p class="movie-index-title">用户评分</p>(.*?)</span>', mvhtml, re.S).group(1)
                number_in_score = re.findall('&#x(.*?);', scorepart, re.S)
                info['score'] = self.code2num(number_in_score[0],local_name)+'.'+self.code2num(number_in_score[1],local_name)
                person_time = re.search('<span class="stonefont">(.*?)</span>人评分</span>', mvhtml).group(1)
                number_in_person_time = re.findall('&#x(.*?);', person_time, re.S)
                for x in number_in_person_time:
                    person_time = person_time.replace('&#x'+x+';',self.code2num(x,local_name))
                info['person_time'] = person_time
            except:
                info['score'] = '暂无评分'
                info['person_time'] = '0'

            moneypart = re.search('<p class="movie-index-title">累计票房</p>(.*?)</div>', mvhtml, re.S).group(1)
            try:
                money1 = re.findall('<span class="stonefont">(.*?)</span><span', moneypart)[0]
                number_money1 = re.findall('&#x(.*?);', money1)
                for x in number_money1:
                    money1 = money1.replace('&#x' + x + ';', self.code2num(x, local_name))
                money2 = re.search('class="unit">(.*?)</span>', moneypart).group(1)
                info['box_office'] = money1+money2
            except IndexError:
                info['box_office'] = '暂无票房数据'

            comment = re.findall('<div class="comment-content">(.*?)</div>',mvhtml)
            if comment:
                info['comment'] = ''
                for c in comment:
                    info['comment'] = info['comment']+c+'\n'
                    info['comment'] = info['comment'][:-1]
            else:
                info['comment'] = '暂无'



        except AttributeError:
            self.faildid.append(id)
            print('faild:'+id)
        return info


myheaders = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}
myspider = spider(myheaders)
conn = pymysql.connect(host='127.0.0.1',port=3306,user='root',passwd='123',db='bigdata',charset = 'utf8mb4',use_unicode = False)
cur = conn.cursor()
create_table = '''create table maoyan(
	id char(16),
	name varchar(100),
	cast varchar(300),
	time varchar(40),
	score varchar(20),
	person_time varchar(20),
	box_office varchar(20),
	comment longtext
);'''
cur.execute(create_table)

my_url_group = myspider.returnpage()
for each_url in my_url_group:
    id_group = myspider.getid(each_url)
    for id in id_group:
        each = myspider.geteachmvinfo(id)
        print(each)
        add_table= '''insert into maoyan VALUES (%s,%s,%s,%s,%s,%s,%s,%s);'''
        cur.execute(add_table, (each['id'],each['name'],each['cast'],each['time'],each['score'],each['person_time'],each['box_office'],each['comment']))
        conn.commit()
cur.close()
conn.close()
