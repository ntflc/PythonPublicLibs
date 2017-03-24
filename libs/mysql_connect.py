#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import ConfigParser
import base64
import MySQLdb
import MySQLdb.cursors


class MySQLConnect:
    def __init__(self, sfile="conf/database.ini", param="mysql"):
        """
        初始化ini配置文件信息，连接MySQL数据库
        :param sfile: ini配置文件路径
        :param param: ini配置文件的paramater
        """
        self.__sfile = sfile
        self.__param = param
        self.__db = self.__connect_mysql()

    def __get_mysql_conf(self):
        """
        通过配置文件获取数据库相关信息
        :return: MySQL数据库地址、用户名、密码、数据库名
        """
        if not os.path.exists(self.__sfile):
            print "Error: %s doesn't exist" % self.__sfile
            sys.exit(-1)
        conf = ConfigParser.ConfigParser()
        conf.read(self.__sfile)
        try:
            host = conf.get(self.__param, "host")
            user = conf.get(self.__param, "user")
            passwd_base64 = conf.get(self.__param, "passwd")
            passwd = base64.decodestring(passwd_base64)
            database = conf.get(self.__param, "database")
        except Exception, e:
            print "Error:", e
            sys.exit(-2)
        return host, user, passwd, database

    def __connect_mysql(self):
        """
        连接数据库
        :return: db
        """
        host, user, passwd, database = self.__get_mysql_conf()
        try:
            db = MySQLdb.connect(host, user, passwd, database, charset="utf8")
        except Exception, e:
            print "Error:", e
            sys.exit(-3)
        return db

    def __get_mysql_data(self, cmd):
        """
        执行MySQL搜索命令
        :param cmd: SQL语句
        :return: 搜索结果
        """
        try:
            cursor = self.__db.cursor()
            cursor.execute(cmd)
            data = cursor.fetchall()
        except Exception, e:
            print "Error:", e
            sys.exit(-4)
        return data

    def __execute_mysql_cmd(self, cmd):
        """
        执行MySQL命令
        :param cmd: SQL语句
        :return: 是否执行成功，True或False
        """
        try:
            cursor = self.__db.cursor()
            cursor.execute(cmd)
            self.__db.commit()
            return True
        except Exception, e:
            self.__db.rollback()
            print "Error:", e
            return False

    def get(self, cmd):
        """
        获取MySQL搜索结果
        :param cmd: SQL语句
        :return: 搜索结果
        """
        data = self.__get_mysql_data(cmd)
        return data

    def execute(self, cmd):
        """
        执行MySQL命令
        :param cmd: SQL语句
        :return: 是否执行成功，True或False
        """
        status = self.__execute_mysql_cmd(cmd)
        return status

    def close(self):
        """
        关闭数据库连接
        :return: 无
        """
        try:
            self.__db.close()
        except Exception, e:
            print "Error:", e
            sys.exit(-5)
