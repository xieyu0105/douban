# -*- coding: utf-8 -*-
import pymysql.cursors
from douban.settings import mysql_host,mysql_port,mysql_db_name,mysql_user,mysql_pass
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


class DoubanPipeline(object):
    def __init__(self):
        host = mysql_host
        port = mysql_port
        user = mysql_user
        passwd = mysql_pass
        db_name = mysql_db_name
        self.connect = pymysql.Connect(
            host = host,
            port = port,
            db = db_name,
            user = user,
            passwd = passwd,
            charset = 'utf8',
            use_unicode = True)
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):
        self.cursor.execute(
            """insert into douban_movie(id,movie_name,introduce,star,evaluate,describes)
            value (%s,%s,%s,%s,%s,%s)
            """,
            (item['serial_number'],item['movie_name'],item['introduce'],item['star'],item['evaluate'],item['describe'])
        )
        self.connect.commit()
        return item
