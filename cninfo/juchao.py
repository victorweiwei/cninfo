import requests
import pymysql
import datetime
import urllib2
import logging
import httplib
import socket
from multiprocessing.pool import Pool
import sys
import os
import ConfigParser  



#生成config对象  
conf = ConfigParser.ConfigParser()  
#用config对象读取配置文件  
conf.read("config.cfg")
filepath = conf.get('file', 'filepath')
logpath = conf.get('file', 'logpath')
host_address = conf.get('database', 'host_address')
users = conf.get('database', 'user')
password = conf.get('database', 'password')
port = int(conf.get('database', 'port'))
mysqldb=conf.get('database', 'mysqldb')
if os.path.exists(filepath) == False:
    os.makedirs(filepath)

try:
    db = pymysql.connect(host=host_address, port=port, user=users, passwd=password, db=mysqldb, charset="utf8")
    cursor = db.cursor()
except Exception as  e:
    logging.debug("connect err")



def create_assist_date(datestart = None,dateend = None):
    # 创建日期辅助表

    if datestart is None:
        datestart = '2016-01-01'
    if dateend is None:
        dateend = datetime.datetime.now().strftime('%Y-%m-%d')

    # 转为日期格式
    datestart=datetime.datetime.strptime(datestart,'%Y-%m-%d')
    dateend=datetime.datetime.strptime(dateend,'%Y-%m-%d')
    date_list = []
    date_list.append(datestart.strftime('%Y-%m-%d'))
    while datestart<dateend:
        # 日期叠加一天
        datestart+=datetime.timedelta(days=+1)
        # 日期转字符串存入列表
        date_list.append(datestart.strftime('%Y-%m-%d'))
    return date_list




def get_page(date,pageNum):
    data = {
    'column':'szse',
    'columnTitle':'历史公告查询',
    'pageNum':pageNum,
    'pageSize':30,
    'seDate':date,
    'tabName':'fulltext'
        }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36'}

    url = 'http://www.cninfo.com.cn/cninfo-new/announcement/query'
    ses = requests.session()
    try:
        response = ses.post(url,data=data,timeout=3,headers=headers)
        if response.status_code ==200:
            logging.info('success to request，%s %s' %(date,pageNum) )
            return response.json()
    except Exception as e :
        logging.info('Failed to request，date,pageNum' )
        return None

def detail(json):
    try:

        if json.get('announcements'):
            data = json.get('announcements')
            for item in data:
                secCode = item.get('secCode')#股票代码
                secName = item.get('secName')#公司名字
                announcementId =item.get('announcementId')
                Title = item.get('announcementTitle')#标题
                adjunctUrl =item.get('adjunctUrl')#发布时间
                antime = adjunctUrl.split('/')[1]
                downurl = 'http://www.cninfo.com.cn/cninfo-new/disclosure/szse/bulletin_detail/true/{0}?announceTime={1}'.format(announcementId,antime)
                #pdf下载公告地址
                downpdf(secName, announcementId, downurl)

                sql_insert = '''insert into announcements(secCode, secName,announcementId,Title,antime,downurl)
                 VALUES (%s,%s,%s,%s,%s,%s) 
                 ON DUPLICATE KEY UPDATE downurl=VALUES(downurl)'''
                try:
                    cursor.execute(sql_insert ,(secCode, secName,announcementId,Title,antime,downurl))
                    db.commit()
                except Exception as e:
                    logging.info(e)

    except Exception as  e:
        logging.debug(e)
		

def downpdf(secName,announcementId,downurl):
    try:
        contentpage = urllib2.urlopen(downurl)
        content_pdf = contentpage.read()

    except socket.timeout:
        downpdf(secName,downurl)
    
    else:
        f_temp = open(
            contentpath + announcementId + '.pdf', 'w+')
        f_temp.write(content_pdf)
        f_temp.close()
        logger.info('成功下载： %s   %s ' % (secName, downurl))
    finally:
            contentpage.close()



def main(date,pageNum):
    json = get_page(date,pageNum)
    detail(json)





if  __name__ =='__main__':
    date =create_assist_date("2018-04-15")
    for  i in date:
        for p in range(50):
            main(i,p)
    # pool = Pool()
    # pool.map(main, i)
    # pool.close()
    # pool.join()


