#!/usr/bin/python3

import argparse
from datetime import datetime
import config
import hashlib
import minorimpact
import os
import os.path
import pickle
import pika
import re
import shutil
import time

parser = argparse.ArgumentParser(description="Synkler middle manager")
#parser.add_argument('filename', nargs="?")
parser.add_argument('--verbose', help = "extra loud output", action='store_true')
#parser.add_argument('--logging', help = "turn on logging", action='store_true')
args = parser.parse_args()

connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.synkler_server))
channel = connection.channel()

channel.exchange_declare(exchange='synkler', exchange_type='topic')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='new')
channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='done')

files = {}
while (True):
    #if (args.verbose): print("checking for new files")
    method, properties, body = channel.basic_get( queue=queue_name, auto_ack=True)
    while body != None:
        routing_key = method.routing_key
        file_data = pickle.loads(body)
        f = file_data["filename"]
        if (routing_key == "new"):
            if (f not in files):
                if (args.verbose): print(f"receiving {f}")
                files[f] = {"filename":f, "dir":config.download_dir, "size":0, "mtime":None, "md5":None, "state":"upload"}
            elif (files[f]["size"] == file_data["size"] and files[f]["mtime"] == file_data["mtime"] and files[f]["md5"] == file_data["md5"]):
                if (args.verbose): print(f"supplying {f}")
                files[f]["state"] = "download"
        elif (routing_key == "done"):
            # TODO: compare the specs (md5, etc) and make sure the new file matches the local file.
            # TODO: file is done, like, delete it, or whatever.
            if (f in files):
                files[f]["state"] = "done"
                if (args.verbose): print(f"{f} done")
        method, properties, body = channel.basic_get( queue=queue_name, auto_ack=True)

    for f in os.listdir(config.download_dir):
        if (re.search("^\.", f)):
            continue

        size = minorimpact.dirsize(config.download_dir + "/" + f)
        mtime = os.path.getmtime(config.download_dir + "/" + f)
        if (f not in files):
            # These files are more than 30 minutes old and haven't been reported in, they can be
            #   axed.
            if (int(time.time()) - mtime > 1800):
                if (args.verbose): print("deleting " + config.download_dir + "/" + f)
                if (os.path.isdir(config.download_dir + "/" + f)):
                    shutil.rmtree(config.download_dir + "/" + f)
                else:
                    os.remove(config.download_dir + "/" + f)
            continue

        if (f in files):
            if (files[f]["state"] == "done"):
                # remove the file from the dictionary one they're reported 'done'.  Don't physically delete them
                #   until they've been gone for at least 30 minutes, though.
                if (args.verbose): print(f"clearing {f}")
                del files[f]
                continue
            if (files[f]["state"] == "download"):
                continue

            if (size == files[f]["size"] and files[f]["mtime"] == mtime):
                # The file has stopped changing, we can assume it's no longer being written to -- grab the md5sum.
                if (files[f]["md5"] == None):
                    md5 = minorimpact.md5dir(config.download_dir + "/" + f)
                    files[f]["md5"] = md5
                    if (args.verbose): print(f"{f} md5:{md5}")
            else:
                files[f]["size"] = size
                files[f]["mtime"] = mtime
        else:
            # Not actually sure what to do with files that exist here but are not coming at us from the upload
            #   server.  Delete them? download them just to be on the safe side?
            md5 = minorimpact.md5dir(config.download_dir + "/" + f)
            files[f]  = {"filename":f, "dir":config.download_dir, "size":size, "md5":md5, "mtime":mtime, "state":"unknown"}

    for f in files:
        if (files[f]["state"] == "upload"):
            #if (args.verbose): print(f"retrieving {files[f]}")
            channel.basic_publish(exchange='synkler', routing_key='upload', body=pickle.dumps(files[f], protocol=4))
        elif (files[f]["state"] == "download"):
            #if (args.verbose): print(f"sending {files[f]}")
            channel.basic_publish(exchange='synkler', routing_key='download', body=pickle.dumps(files[f], protocol=4))

    #if (args.verbose): print("\n")
    time.sleep(5)


