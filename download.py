#!/usr/bin/env python3

import argparse
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

parser = argparse.ArgumentParser(description="Synkler download script")
parser.add_argument('--verbose', help = "extra loud output", action='store_true')
args = parser.parse_args()

connection = pika.BlockingConnection(pika.ConnectionParameters(host=synkler_server))
channel = connection.channel()

# message queues
channel.exchange_declare(exchange='synkler', exchange_type='topic')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='download')

files = {}
while True:
    #if (args.verbose): print(f"checking {download_dir}")
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
                        if (args.verbose): print(f"{f} md5:{md5}")
                else:
                    churn = True
                    files[f]["size"] = size
                    files[f]["mtime"] = mtime
        time.sleep(1)

    
    #if (args.verbose): print("checking for synkler commands")
    method, properies, body = channel.basic_get( queue_name, True)
    while body != None:
        file_data = pickle.loads(body)
        f = file_data['filename']
        dl_dir = file_data['dir']
        md5 = file_data['md5']
        size = file_data["size"]
        mtime = file_data["mtime"]
        if (f not in files):
            if (args.verbose): print(f"new file:{f}")
            files[f]  = {"filename":f, "size":0, "md5":None, "mtime":0, "dir":config.download_dir, "state":"download"}
        elif (files[f]["size"] != size or md5 != files[f]["md5"] or files[f]["mtime"] != mtime):
            rsync_command = [rsync, "--archive", "--partial", synkler_server + ":\"" + dl_dir + "/" + f + "\"", download_dir + "/"]
            if (args.verbose): print(' '.join(rsync_command))
            return_code = subprocess.call(rsync_command)
            if (return_code == 0):
                files[f]["size"] = minorimpact.dirsize(config.download_dir + "/" + f)
                files[f]["mtime"] = os.path.getmtime(config.download_dir + "/" + f)
                files[f]["md5"] = minorimpact.md5dir(config.download_dir + "/" + f)
            
            #if (args.verbose): print("Output: ", return_code)
        else:
            if (args.verbose): print(f"{f} done")
            files[f]["state"] = "done"
        # get the next file from the queue
        method, properies, body = channel.basic_get( queue_name, True)

    #if (args.verbose): print("reporting download status to synkler")
    for f in files:
        if (files[f]["state"] == "done"):
            #if (args.verbose): print(f"{f} done")
            channel.basic_publish(exchange='synkler', routing_key='done', body=pickle.dumps(files[f]))
    #if (args.verbose): print("\n")

    # TODO: Figure out how to clear a file out of the list once it's finished and removed
    #   from them upload server.  I can use 'mtime' remove anything that hasn't been updated in like half an hour,,
    #    but I have to convert the filenames into an array and then delete it that way -- otherwise I get the stupid
    #   "dictionary has changed" error.  It's not hard, I'm just tired.

    time.sleep(5)

# close the connection
connection.close()



