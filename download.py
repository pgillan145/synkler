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

download_dir = config.file_dir
synkler_server = config.synkler_server
rsync = config.rsync

parser = argparse.ArgumentParser(description="Synkler download script")
parser.add_argument('-v', '--verbose', help = "extra loud output", action='store_true')
parser.add_argument('--id', nargs='?', help = "id of a specific synkler group", default="default")
args = parser.parse_args()

pidfile = config.pidfile if hasattr(config, 'pidfile') and config.pidfile is not None else "/tmp/synkler.pid"
if (minorimpact.checkforduplicates(pidfile)):
    if (args.verbose): sys.exit('already running')
    else: sys.exit()

connection = pika.BlockingConnection(pika.ConnectionParameters(host=synkler_server))
channel = connection.channel()

# message queues
channel.exchange_declare(exchange='synkler', exchange_type='topic')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='download.' + args.id)

files = {}
while True:
    #if (args.verbose): print(f"checking {download_dir}")
    for f in os.listdir(download_dir):
        if (re.search("^\.", f)):
            continue
        size = minorimpact.dirsize(config.file_dir + "/" + f)
        mtime = os.path.getmtime(config.file_dir + "/" + f)
        if (f in files):
            if (size == files[f]["size"] and files[f]["mtime"] == mtime):
                if (files[f]["md5"] is None):
                    md5 = minorimpact.md5dir(config.file_dir + "/" + f)
                    files[f]["md5"] = md5
                    if (args.verbose): print(f"{f} md5:{md5}")
            else:
                files[f]["size"] = size
                files[f]["mtime"] = mtime

    
    #if (args.verbose): print("checking for synkler commands")
    # Just do one download per loop, so the rest of the messages don't pile up, and we can report
    #   files as done right away.
    download = False
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
            files[f]  = {"filename":f, "size":0, "md5":None, "mtime":0, "dir":config.file_dir, "state":"download"}

        if (files[f]["size"] != size or md5 != files[f]["md5"] or files[f]["mtime"] != mtime):
            if (download is False):
                download = True
                rsync_command = [rsync, "--archive", "--partial", synkler_server + ":\"" + dl_dir + "/" + f + "\"", download_dir + "/"]
                if (args.verbose): print(' '.join(rsync_command))
                return_code = subprocess.call(rsync_command)
                if (return_code == 0):
                    files[f]["size"] = minorimpact.dirsize(config.file_dir + "/" + f)
                    files[f]["mtime"] = os.path.getmtime(config.file_dir + "/" + f)
                    files[f]["md5"] = minorimpact.md5dir(config.file_dir + "/" + f)
                elif (args.verbose): print("Output: ", return_code)
        else:
            if (files[f]["state"] != "done"):
                files[f]["state"] = "done"
                
        # get the next file from the queue
        method, properies, body = channel.basic_get( queue_name, True)

    filenames = [key for key in files]
    for f in filenames:
        if (files[f]["state"] == "done"):
            if (args.verbose): print(f"{f} done")
            channel.basic_publish(exchange='synkler', routing_key='done.' + args.id, body=pickle.dumps(files[f]))
            del files[f]
    #if (args.verbose): print("\n")

    time.sleep(5)

# close the connection
connection.close()



