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
channel.queue_declare(queue='upload', durable=True)
channel.queue_bind(exchange='synkler', queue='upload')
channel.queue_declare(queue='archive', durable=True)
channel.queue_bind(exchange='synkler', queue='archive')
channel.queue_declare(queue='download', durable=True)
channel.queue_bind(exchange='synkler', queue='download')

upload_status = {}
def process_new_files(ch, body):
    file_data = pickle.loads(body)
    print(file_data)
    filename = file_data["filename"]
    if filename in upload_status:
        print("skipping " + filename + " already in the queue")
        return

    now = datetime.now()
    upload_status[filename]  = { "md5":file_data['md5'], "size":file_data['size'], "state":'new', "date":now, "pickle_protocol":file_data["pickle_protocol"]}
    upload_data = {"filename":file_data['filename'], "dest_dir":config.download_dir, "md5":file_data['md5'], "date":now}
    print("starting upload for " + filename)
    ch.basic_publish(exchange='synkler', routing_key='upload', body=pickle.dumps(upload_data, protocol=file_data["pickle_protocol"]))
    upload_status[filename]["state"] = "uploading"


def process_new(ch, method, properties, body):
    file_data = pickle.loads(body)
    print(file_data)
    ch.basic_publish(exchange='synkler', routing_key='upload', body=file_data['filename'])

#channel.basic_consume( queue='new', on_message_callback=process_new, auto_ack=True)
#channel.start_consuming()

while (True):
    print("scanning " + config.download_dir)
    for f in os.listdir(config.download_dir):
        if (re.search("^\.", f)) or f not in upload_status:
            continue

        if (f in upload_status and upload_status[f]["state"] == "uploading"):
            size = minorimpact.dirsize(config.download_dir + "/" + f)
            print("found " + f)
            if (size == upload_status[f]["size"]):
                print("  size matches")
                md5 = minorimpact.md5dir(config.download_dir + "/" + f)
                if (md5 == upload_status[f]["md5"]):
                    print(f + " has completed")
                    upload_status[f]["state"] = "archive"
                    print("starting archive for " + f)
                    channel.basic_publish(exchange='synkler', routing_key='archive', body=f)
                    print("starting download for " + f)
                    download_data = {"filename":f, "md5":md5, "download_dir":config.download_dir}
                    channel.basic_publish(exchange='synkler', routing_key='download', body=pickle.dumps(download_data))
    #channel.basic_consume( queue='new', on_message_callback=process_new, auto_ack=True)
    #channel.start_consuming()
    print("checking for new files")
    new = channel.basic_get( queue='new', auto_ack=True)
    if new[2] != None:
        process_new_files(channel, new[2])
        new = channel.basic_get( queue='new', auto_ack=True)
    time.sleep(5)


