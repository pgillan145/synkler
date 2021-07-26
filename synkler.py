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

channel.exchange_declare(exchange='synkler', exchange_type='direct')
channel.queue_declare(queue='new')
channel.queue_bind(exchange='synkler', queue='new')
channel.queue_declare(queue='upload')
channel.queue_bind(exchange='synkler', queue='upload')
channel.queue_declare(queue='download')
channel.queue_bind(exchange='synkler', queue='download')

files = {}
new_files = {}
def process_new_files(ch, body):
    file_data = pickle.loads(body)
    print(file_data)
    filename = file_data["filename"]
    new_files[filename] = file_data
    #if filename in files:
    #    print("skipping " + filename + " already in the queue")
    #    return

    #now = datetime.now()
    #files[filename]  = {"filename":filename, "dest_dir":config.download_dir, "size":0, "md5":None, "state":"uploading", "mtime":now}
    #print("starting upload for " + filename)
    #ch.basic_publish(exchange='synkler', routing_key='upload', body=pickle.dumps(files[filename], protocol=4)) #file_data["pickle_protocol"]))

while (True):
    print("new files:")
    new = channel.basic_get( queue='new', auto_ack=True)
    while new[2] != None:
        process_new_files(channel, new[2])
        new = channel.basic_get( queue='new', auto_ack=True)

    for f in os.listdir(config.download_dir):
        if (re.search("^\.", f)):
            continue
        if (f not in new_files):
            # TODO: delete these files from the download directory after X minutes have passed -- there's a delay
            #   during the upload.py startup that would cause these to be deleted as soon as this script starts and
            #   then they'd be uploaded again a few seconds later.
            print("DELETE:" + f)
            continue

        size = minorimpact.dirsize(config.download_dir + "/" + f)
        mtime = os.path.getmtime(config.download_dir + "/" + f)
        if (f in files):
            if (size == files[f]["size"] and files[f]["mtime"] == mtime):
                if (files[f]["md5"] == None):
                    md5 = minorimpact.md5dir(config.download_dir + "/" + f)
                    files[f]["md5"] = md5
                    #if (f in new_files):
                    #    if (size == new_files[f]["size"] and mtime == new_files[f]["mtime"] and md5 == new_files[f]["md5"]):
                    #        files[f]["state"] = "error"
                    #    else:
                    #        files[f]["state"] = "done"
            else:
                files[f]["size"] = size
                files[f]["mtime"] = mtime
        else:
            files[f]  = {"filename":f, "dir":config.download_dir, "size":0, "md5":None, "mtime":None}

    for f in new_files:
        # For now we're just auto adding anything in the new_files list to the upload queue, but at some point we need to check and make sure
        #   we've got space before we just blindly starting pulling everyting under the sun.
        if (f not in files):
            # TODO: confirm there's enough space to handle these new files before we sat yes to an upload.
            files[f]  = {"filename":f, "dest_dir":config.download_dir, "size":0, "md5":None, "mtime":None}

    for f in files:
        if (f in new_files):
            print("upload " + f)
            channel.basic_publish(exchange='synkler', routing_key='upload', body=pickle.dumps(files[f], protocol=4))
        if (files[f]['md5'] is not None):
            print("download " + f)
            channel.basic_publish(exchange='synkler', routing_key='download', body=pickle.dumps(files[f], protocol=4))

    print("\n")
    time.sleep(5)


