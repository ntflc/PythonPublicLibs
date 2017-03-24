#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import ConfigParser
import pymongo


class MongoDBConnect:
    def __init__(self, sfile="conf/database.ini", param="mongodb"):
        """
        初始化ini配置文件信息，连接MongoDB数据库
        :param sfile: ini配置文件路径
        :param param: ini配置文件的paramater
        """
        self.__sfile = sfile
        self.__param = param
        self.__client, self.__db = self.__connect_mongodb()

    def __get_mongodb_conf(self):
        """
        通过配置文件获取数据库相关信息
        :return: MongoDB数据库地址、数据库名
        """
        if not os.path.exists(self.__sfile):
            print "Error: %s doesn't exist" % self.__sfile
            sys.exit(-1)
        conf = ConfigParser.ConfigParser()
        conf.read(self.__sfile)
        try:
            host = conf.get(self.__param, "host")
            database = conf.get(self.__param, "database")
        except Exception as e:
            print "Error:", e
            sys.exit(-2)
        return host, database

    def __connect_mongodb(self):
        """
        连接数据库
        :return: client, db
        """
        host, database = self.__get_mongodb_conf()
        try:
            client = pymongo.MongoClient(host=host)
            db = client[database]
        except Exception as e:
            print "Error:", e
            sys.exit(-3)
        return client, db

    def get(self, col):
        """
        查询文档
        :param col: 集合
        :return: 查询结果
        """
        col = self.__db[col]
        return col.find()

    def insert(self, col, doc):
        """
        插入文档
        :param col: 集合
        :param doc: 文档
        :return: 插入结果
        """
        col = self.__db[col]
        return col.insert(doc)

    def remove(self, col, condition):
        """
        删除文档
        :param col: 集合
        :param condition: 条件
        :return: 删除结果
        """
        col = self.__db[col]
        return col.remove(condition)

    def close(self):
        """
        关闭数据库连接
        :return: 无
        """
        self.__client.close()
