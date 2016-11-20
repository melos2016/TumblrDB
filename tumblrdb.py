#coding:gbk

import logging
import logging.config
import os
import sys
import requests
requests.packages.urllib3.disable_warnings()
import xmltodict
from six.moves import queue as Queue
from threading import Thread
import re
import json
import apsw
import time

conn=apsw.Connection("tumblr.db")
cu=conn.cursor()
logging.config.fileConfig("logger.conf")
logger = logging.getLogger("example01")
logger03 = logging.getLogger("example03")

ss = requests.Session()

# Setting timeout
TIMEOUT = 15

# Retry times
RETRY = 5

# Medium Index Number that Starts from
START = 0

# Numbers of photos/videos per page
MEDIA_NUM = 50


def handlesql(sites):
    sql='''create table if not exists blogs ("name" TEXT primary key NOT NULL unique,"type1" TEXT,"record1" INTEGER DEFAULT 0,"total1" INTEGER,"type2" TEXT,"record2" INTEGER DEFAULT 0,"novideo" INTEGER DEFAULT 0,"total2" INTEGER,"record" INTEGER DEFAULT 0,"total" INTEGER,"userlastpost" TEXT,"lastrecordpostid" INTEGER,"lastupdatetime" TEXT,lasttimestamp INTEGER DEFAULT 0)'''
    cu.execute(sql)
    sql='''create table if not exists posts ("name" TEXT,"postid" INTEGER primary key NOT NULL unique,"recorded" INTEGER)'''
    cu.execute(sql)
    sql='''create table if not exists novideo ("name" TEXT,"postid" INTEGER primary key NOT NULL unique)'''
    cu.execute(sql)
    sql="create trigger if not exists total after update of total1,total2 on blogs  begin update blogs set total=total1+total2; end;"
    cu.execute(sql) 
    sql="create trigger if not exists record after update of record1,record2,novideo on blogs  begin update blogs set record=record1+record2+novideo; end;"
    cu.execute(sql)    
    for site in sites:
        sql='''create table if not exists "%s" ("post" INTEGER,"type" TEXT,"targeturl" TEXT NOT NULL,"filename"  TEXT  primary key NOT NULL unique,"recordtime"  TEXT,"date-gmt"  TEXT,"unix-timestamp"  INTEGER,"url"  TEXT,"slug"  TEXT)''' % site
        cu.execute(sql)
        sql="select name from blogs where name='%s'" % site                
        if len(cu.execute(sql).fetchall()) == 0:
            cu.execute("insert into blogs(name) values('%s')" % site)


class CrawlerScheduler(object):

    def __init__(self, sites, proxies=None):
        self.sites = sites
        self.proxies = proxies
        self.queue = Queue.Queue()
        self.scheduling()
    
    def scheduling(self):
        for site in self.sites:
            arr=[]
            medium_type="photo"
            sql="select * from blogs where name='%s'" % site
            lastinfo=cu.execute(sql).fetchone()
            for i in range(2):
                base_url = "https://{0}.tumblr.com/api/read?type={1}&num=1"
                media_url = base_url.format(site,medium_type)
                retry_times = 0
                while retry_times < RETRY:
                    try:
                        response = ss.get(media_url,
                                    proxies=self.proxies,
                                    verify=False,
                                    timeout=TIMEOUT)
                        break
                    except Exception as e:
                        logger.warning("Error: " + str(e) + " ... retrying")
                        # try again
                        pass
                    retry_times += 1
                data = xmltodict.parse(response.content)
                if i==0:
                    sql="update blogs set type1='%s',total1='%s' where name='%s'" % (medium_type,data["tumblr"]["posts"]["@total"],str(site))
                else:
                    sql="update blogs set type2='%s',total2='%s' where name='%s'" % (medium_type,data["tumblr"]["posts"]["@total"],str(site))
                cu.execute(sql)
                medium_type="video"
                if int(data["tumblr"]["posts"]["@total"])>0:
                    arr.append(data["tumblr"]["posts"]["post"]['@unix-timestamp'])
                else:
                    arr.append(0)
            timestamp=arr[0] if arr[0]>=arr[1] else arr[1]
            timestamp=float(timestamp)
            userlastpost=time.strftime("%Y-%m-%d %H:%M:%S GMT", time.gmtime(timestamp))
            #sql="update blogs set userlastpost='%s' where name='%s'" % (userlastpost,str(site))
            #cu.execute(sql)
            beginstamp=int(time.time())
            lastupdatetime=time.strftime("%Y-%m-%d %H:%M:%S GMT+8", time.localtime())
            #sql="update blogs set lastupdatebegin='%s' where name='%s'" % (lastupdatebegin,str(site))
            #cu.execute(sql)
            sql="update blogs set userlastpost='%s',lastupdatetime='%s',lasttimestamp='%s' where name='%s'" % (userlastpost,lastupdatetime,beginstamp,str(site))
            cu.execute(sql)
            self.crwl_queue(site,lastinfo)
            #lastupdateend=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            #sql="update blogs set lastupdateend='%s' where name='%s'" % (lastupdateend,str(site))
            #cu.execute(sql)


    def crwl_queue(self, site,lastinfo):
    	base_url = "https://{0}.tumblr.com/api/read?num={1}&start={2}"
        i=0
        start=START
        logger.info("开始收集用户 %s 数据..........." % site)
        while True:
            media_url = base_url.format(site, MEDIA_NUM, start)
            logger.info("解析链接: %s" % media_url)
            retry_times = 0
            while retry_times < RETRY:
                try:
                    response = ss.get(media_url,
                                    proxies=self.proxies,
                                    verify=False,
                                    timeout=TIMEOUT)
                    break
                except Exception as e:
                    logger.warning("Error: " + str(e) + " ... retrying")
                    # try again
                    pass
                retry_times += 1
            data = xmltodict.parse(response.content)
            try:
                posts = data["tumblr"]["posts"]["post"]
                if isinstance(posts,dict):
                    posts=[posts]
            except KeyError as e:
                #print("Key error: " + str(e))
                logger.info("此链接无数据！")
                logger.info("更新用户 %s 数据完成！" % site)
                break
            if int(posts[0]['@unix-timestamp'])>int(lastinfo[13]):
                logger.info("用户提交时间 :%s" % str(posts[0]['@unix-timestamp']))
                logger.info("上次更新时间 :%s" % lastinfo[13])
                logger.info("用户提交时间晚于更新时间!!")
                pass
            elif i==0:
                start=int(lastinfo[8]/MEDIA_NUM)*MEDIA_NUM-MEDIA_NUM*3
                if start<0:start=0
                i+=1
                logger.info("用户提交时间 :%s" % str(posts[0]['@unix-timestamp']))
                logger.info("上次更新时间 :%s" % lastinfo[13])
                logger.info("需要重新设置链接偏移数据！")
                logger.info("重新设置偏移为 %s !" % start)
            self.crwl_url(site,posts)
            #offset[site]=start
            #with open("./json.json",'w') as f:
                #f.write(json.dumps(offset, indent=4, sort_keys=True))
                #f.close()
            logger.info("链接数据解析完成！")
            start += MEDIA_NUM


    def crwl_url(self,site,posts):
        try:
            try:
                #posts = data["tumblr"]["posts"]["post"]
                recordtime=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                for post in posts:
                    medium_type=post['@type']
                    index=1
                    # select the largest resolution
                    # usually in the first element
                    if post.has_key("photoset"):
                        photosets=post["photoset"]["photo"]
                        logger03.info("解析用户[%s]-[%s]数据中......" % (site,str(post['@id'])))
                        zt=[]
                        for photos in photosets:                             
                            durl = photos["photo-url"][0]["#text"].replace('http:', 'https:')
                            t=[post['@id'],post['@type'],'','',recordtime,post['@date-gmt'],post['@unix-timestamp'],post['@url'],post['@slug']]                           
                            filename = os.path.join(
                                '%s_%s.%s' % (post['@id'], index, durl.split('.')[-1]))
                            t[2]=durl
                            t[3]=filename
                            logger03.info("照片%s链接：%s" % (index,durl.encode('gbk')))
                            sql="select filename from '%s' where filename='%s'" % (site,str(t[3]))
                            if len(cu.execute(sql).fetchall()) == 0:
                                zt.append(t)
                            index += 1
                        logger03.info("判断POST：%s是否已记录！" % str(post['@id']))
                        if len(zt) > 0:
                            sql='insert into "%s" values(?,?,?,?,?,?,?,?,?)' % site
                            cu.executemany(sql,(zt))
                            logger.info("用户[%s]-[%s]数据记录完成......" % (site,str(post['@id'])))
                            sql='update blogs set record1=record1+1,lastrecordpostid=%s where name="%s"' % (post['@id'],str(site))
                            cu.execute(sql)
                        else:
                            logger03.info("用户[%s]-[%s]已在数据库中！" % (site,str(post['@id'])))
                        continue
                    logger03.info("解析用户[%s]-[%s]数据中......" % (site,str(post['@id'])))
                    zt=[]
                    t=[post['@id'],post['@type'],'','',recordtime,post['@date-gmt'],post['@unix-timestamp'],post['@url'],post['@slug']]
                    if medium_type =="photo":
                        durl = post["photo-url"][0]["#text"].replace('http:', 'https:')
                        filename = os.path.join(
                                '%s_%s.%s' % (post['@id'], index, durl.split('.')[-1]))
                        logger03.info("照片链接：%s" % durl.encode('gbk'))
                    else:                      
                        #if post["video-player"][1].has_key("#text"):
                        if medium_type =="video":
                            video_player = post["video-player"][1]["#text"]
                        else:
                            continue
                        pattern = re.compile(r'[\S\s]*src="(\S*tumblr_[^/]*)\S*" ')
                        match = pattern.match(video_player)                
                        if match is not None and isinstance(post["video-source"],dict):
                            durl=match.group(1)
                            logger03.info("初始视频链接：%s" % str(durl))
                            durl="%s//%s/%s.%s" % (durl.split("/")[0],"vtt.tumblr.com",durl.split("/")[-1],post["video-source"]["extension"])
                            logger03.info("重定向视频链接：%s" % str(durl))
                        else:
                            self.novideo(site,post["@id"])
                            logger.info("用户[%s]-[%s]没有视频链接" % (site,str(post["@id"])))
                            continue                  
                        filename = os.path.join('%s.%s' % (post['@id'], post["video-source"]["extension"]))
                    t[2]=durl
                    t[3]=filename
                    sql="select filename from '%s' where filename='%s'" % (site,str(t[3]))
                    if len(cu.execute(sql).fetchall()) == 0:
                        zt.append(t)
                    logger03.info("判断POST：%s是否已记录！" % str(post['@id']))
                    if len(zt) > 0:
                        sql='replace into "%s" values(?,?,?,?,?,?,?,?,?)' % site
                        cu.executemany(sql,(zt))
                        if medium_type =="photo":
                            sql='update blogs set record1=record1+1,lastrecordpostid=%s where name="%s"' % (post['@id'],str(site))
                            cu.execute(sql)
                        if medium_type =="video":
                            sql='update blogs set record2=record2+1,lastrecordpostid=%s where name="%s"' % (post['@id'],str(site))
                            cu.execute(sql)
                        logger.info("用户[%s]-[%s]数据记录完成......" % (site,str(post['@id'])))
                    else:
                        logger03.info("用户[%s]-[%s]已在数据库中！" % (site,str(post['@id'])))
            except KeyError as e:
                logger.warning("Key error: " + str(e))
                logger.info("出现错误！")
                if str(e)=="'#text'":
                    logger.info("用户[%s]-[%s]没有视频链接" % (site,str(post['@id'])))
                    self.novideo(site,str(post['@id']));
        except KeyError as e:
            logger.warning("Key error: " + str(e))


    def novideo(self,site,postid):
        sql="select postid from novideo where postid='%s'" % postid
        logger.info("查询数据库中........")
        if len(cu.execute(sql).fetchall()) == 0:
            sql='update blogs set novideo=novideo+1 where name="%s"' % str(site)
            cu.execute(sql)
            sql="insert into novideo values('%s',%s)" % (site,postid)
            cu.execute(sql)
            logger.info("增加没有视频的VIDEO-POST：%s数据成功！"% str(postid))
        logger.info("已有此数据！")

if __name__ == "__main__":
    sites = None

    proxies = None
    if os.path.exists("./proxies.json"):
        with open("./proxies.json", "r") as fj:
            try:
                proxies = json.load(fj)
                if proxies is not None and len(proxies) > 0:
                    print("You are using proxies.\n%s" % proxies)
            except:
                illegal_json()
                sys.exit(1)

    if len(sys.argv) < 2:
        # check the sites file
        filename = "sites.txt"
        if os.path.exists(filename):
            with open(filename, "r") as f:
                sites = f.read().rstrip().lstrip().split(",")
        else:
            usage()
            sys.exit(1)
    else:
        sites = sys.argv[1].split(",")

    if len(sites) == 0 or sites[0] == "":
        usage()
        sys.exit(1)

    #if os.path.exists("./json.json"):
        #with open("./json.json", "r") as fj:
            #offset=json.load(fj)
            #fj.close()

    handlesql(sites)

    CrawlerScheduler(sites, proxies=proxies)