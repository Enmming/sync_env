# -*- coding: utf-8 -*-
import os
import json
import pandas as pd
import datetime
import sys
import paramiko
import subprocess
import shutil
import settings
import restart_port
from pymongo import MongoClient
from scp import SCPClient


prod_mongo_server = settings.PROD_MONGODB_SERVER
prod_mongodb_port = settings.PROD_MONGODB_PORT
prod_mongodb_username = settings.PROD_MONGODB_USERNAME
prod_mongodb_password = settings.PROD_MONGODB_PASSWORD
prod_mongodb_dbname = settings.PROD_MONGODB_DBNAME

prod_server = settings.PROD_SERVER
prod_server_ssh_port = settings.PROD_SERVER_SSH_PORT
prod_server_ssh_username = settings.PROD_SERVER_SSH_USERNAME
prod_server_ssh_password = settings.PROD_SERVER_SSH_PASSWORD

file_list = settings.FILE_LIST or list()

sync_exclude_collections = settings.SYNC_EXCLUDE_COLLECTIONS or list()

mongodump_dir = os.path.join('/tmp', 'mongodump_dir')

client = MongoClient(settings.LOCAL_MONGODB_ADDRESS, settings.LOCAL_MONGODB_PORT)


def createSSHClient(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client


if __name__ == "__main__":
    # 删除dump文件夹
    if os.path.isdir(mongodump_dir):
        shutil.rmtree(mongodump_dir)
    os.makedirs(mongodump_dir)

    # 下载数据
    ssh = createSSHClient(prod_server, prod_server_ssh_port, prod_server_ssh_username, prod_server_ssh_password)
    for file in file_list:
        print 'Downloading...... ' + file
        with SCPClient(ssh.get_transport()) as scp:
            scp.get(os.path.join(settings.DATA_FILE_PATH, file), settings.DATA_FILE_PATH)
            scp.close()
        print 'Downloaded  ' + file

    # 同步mongodb数据库
    print 'Mongodumping......'
    mongodump_cmd = "mongodump --host={host} --port={port} --db={db} --out={out_dir} --excludeCollection={excludeCollections}".format(
        host=prod_mongo_server, port=prod_mongodb_port, db=prod_mongodb_dbname, out_dir=mongodump_dir,
        excludeCollections=' '.join(sync_exclude_collection for sync_exclude_collection in sync_exclude_collections))
    print subprocess.check_output(mongodump_cmd, shell=True)

    print 'Finished Mongodump.'

    # Dropdatabase
    client.drop_database(settings.MONGODB_DBNAME)
    mongorestore_cmd = "mongorestore --dir {mongodump_dir}".format(mongodump_dir=mongodump_dir)
    print subprocess.check_output(mongorestore_cmd, shell=True)

    for port in range(settings.SITE_PORT, settings.SITE_PORT + settings.SITE_NUMBER_OF_PROCESSES):
        restart_port.restart_port(port)
    print 'All Done.'
