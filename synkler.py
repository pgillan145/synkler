#!/usr/bin/python3

from datetime import datetime
import config
import hashlib
import minorimpact
import os
import os.path
import pickle
import pika
import re
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.synkler_server))
channel = connection.channel()

channel.exchange_declare(exchange='synkler', exchange_type='topic')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='new')
channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='done')

files = {}
while (True):
    print("new files:")
    method, properties, body = channel.basic_get( queue=queue_name, auto_ack=True)
    while body != None:
        routing_key = method.routing_key
        file_data = pickle.loads(body)
        print(file_data)
        f = file_data["filename"]
        if (routing_key == "new"):
            if (f not in files):
                print(f"setting {f} state to 'upload'")
                files[f] = {"filename":f, "dir":config.download_dir, "size":0, "mtime":None, "md5":None, "state":"upload"}
            elif (files[f]["size"] == file_data["size"]): # and files[f]["mtime"] == file_data["mtime"] and files[f]["md5"] == file_data["md5"]):
                print(f"setting {f} state to 'download'")
                files[f]["state"] = "download"
        elif (routing_key == "done"):
            # TODO: compare the specs (md5, etc) and make sure the new file matches the local file.
            # TODO: file is done, like, delete it, or whatever.
            files[f]["state"] = "done"
        method, properties, body = channel.basic_get( queue=queue_name, auto_ack=True)

    for f in os.listdir(config.download_dir):
        if (re.search("^\.", f)):
            continue
        if (f not in files):
            # TODO: delete these files from the download directory after X minutes have passed -- there's a delay
            #   during the upload.py startup that would cause these to be deleted as soon as this script starts and
            #   then they'd be uploaded again a few seconds later... maybe?
            print("DELETE:" + f + "?")
            continue

        size = minorimpact.dirsize(config.download_dir + "/" + f)
        mtime = os.path.getmtime(config.download_dir + "/" + f)
        if (f in files):
            if (files[f]["state"] == "done"):
                print(f"DONE: {f}")
                # TODO: Delete the local files once they're marked done.
                print(f"DELETE: {f}")
                continue
            if (files[f]["state"] == "download"):
                continue

            if (size == files[f]["size"] and files[f]["mtime"] == mtime):
                # The file has stopped changing, we can assume it's no longer being written to -- grab the md5sum.
                if (files[f]["md5"] == None):
                    md5 = minorimpact.md5dir(config.download_dir + "/" + f)
                    files[f]["md5"] = md5
                    # TODO: compare this, make sure it's right
                    #if (f in new_files):
                    #    if (size == new_files[f]["size"] and mtime == new_files[f]["mtime"] and md5 == new_files[f]["md5"]):
                    #        files[f]["state"] = "error"
                    #    else:
                    #        files[f]["state"] = "done"
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
            # TODO: the upload script blocks while running the rsync, so this script just pumps out command after command -- causing
            #   the upload script to eventually execute a shitload of redundant rsync commands.  It still "works", but it's
            #   shit.
            print(f"uploading {files[f]}")
            channel.basic_publish(exchange='synkler', routing_key='upload', body=pickle.dumps(files[f], protocol=4))
        elif (files[f]["state"] == "download"):
            print(f"downloading {files[f]}")
            channel.basic_publish(exchange='synkler', routing_key='download', body=pickle.dumps(files[f], protocol=4))

    print("\n")
    time.sleep(5)


