#!/usr/bin/env python3

import config
import hashlib
import minorimpact
import os
import os.path
import pickle
import pika
import re
import subprocess
import time

download_dir = config.download_dir
synkler_server = config.synkler_server
rsync = config.rsync

connection = pika.BlockingConnection(pika.ConnectionParameters(host=synkler_server))
channel = connection.channel()

# message queues
channel.exchange_declare(exchange='synkler', exchange_type='direct')
channel.queue_declare(queue='new')
channel.queue_bind(exchange='synkler', queue='new')
channel.queue_declare(queue='upload')
channel.queue_bind(exchange='synkler', queue='upload')
channel.queue_declare(queue='download')
channel.queue_bind(exchange='synkler', queue='download')
channel.queue_declare(queue='done')
channel.queue_bind(exchange='synkler', queue='done')

files = {}
while True:
    churn = True
    while churn:
        churn = False
        for f in os.listdir(download_dir):
            if (re.search("^\.", f)):
                continue
            size = minorimpact.dirsize(config.download_dir + "/" + f)
            mtime = os.path.getmtime(config.download_dir + "/" + f)
            if (f in files):
                if (size == files[f]["size"] and files[f]["mtime"] == mtime):
                    if (files[f]["md5"] is None):
                        md5 = minorimpact.md5dir(config.download_dir + "/" + f)
                        files[f]["md5"] = md5
                else:
                    churn = True
                    files[f]["size"] = size
                    files[f]["mtime"] = mtime
            else:
                files[f]  = {"filename":f, "size":size, "md5":None, "mtime":mtime}
                churn = True
        time.sleep(1)

    #    upload_data = {"filename":f, "md5":md5, "size":size, "pickle_protocol":pickle.HIGHEST_PROTOCOL}
    #    print("sending about " + f + " to synkler")
    #    print(upload_data)
    #    # report the list of files to the central server.  It doesn't matter if
    #    #   we've sent them before, the server will keep track of duplicates.
    #    channel.basic_publish(exchange='synkler', routing_key='new', body=pickle.dumps(upload_data))

    print("downloads:")
    download = channel.basic_get( 'download', True)
    while download[2] != None:
        file_data = pickle.loads(download[2])
        print(file_data)
        filename = file_data['filename']
        dl_dir = file_data['dir']
        md5 = file_data['md5']
        size = file_data["size"]
        if (filename not in files or files[filename]["size"] != size or  md5 != files[filename]["md5"]):
            rsync_command = [rsync, "--archive", "--partial", synkler_server + ":" + dl_dir + "/\"" + filename + "\"", download_dir + "/"]
            print(' '.join(rsync_command))

            return_code = subprocess.call(rsync_command)
            print("Output: ", return_code)
        # get the next file from the queue
        download = channel.basic_get('download', True)

    print("done")
    for f in files:
        print(files[f])
        channel.basic_publish(exchange='synkler', routing_key='done', body=pickle.dumps(files[f]))
    print("\n")

    time.sleep(5)


# close the connection
connection.close()



