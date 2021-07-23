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

upload_dir = config.download_dir
rsync = config.rsync

def md5dir(filename):
    m = hashlib.md5()
    if (os.path.isdir(filename)):
        for f in sorted(os.listdir(filename)):
            md5 = md5dir(filename + "/" + f)
            m.update(md5.encode('utf-8'))
    else:
        with open(filename, 'rb') as f:
            data = f.read(1048576)
            while(data):
                md5 = hashlib.md5(data).hexdigest()
                m.update(md5.encode('utf-8'))
                data = f.read(1048576)
    print(filename + " md5:" + m.hexdigest())
    return m.hexdigest()

def dirsize(filename):
    size = 0
    if (os.path.isdir(filename)):
        for f in os.listdir(filename):
            size += dirsize(filename + "/" + f)
    else:
        size = os.path.getsize(filename)
    return size


connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.synkler_server))
channel = connection.channel()

# message queues
channel.exchange_declare(exchange='synkler', exchange_type='direct')
channel.queue_declare(queue='new')
channel.queue_bind(exchange='synkler', queue='new')

while True:
# collect all the files that need to be uploaded
    for f in os.listdir(upload_dir):
        if (re.search("^\.", f)):
            continue
        md5 = minorimpact.md5dir(upload_dir + "/" + f)
        size = minorimpact.dirsize(upload_dir + "/" + f)
        upload_data = {"filename":f, "md5":md5, "size":size, "pickle_protocol":pickle.HIGHEST_PROTOCOL}
        print("sending about " + f + " to synkler")
        print(upload_data)
        # report the list of files to the central server.  It doesn't matter if
        #   we've sent them before, the server will keep track of duplicates.
        channel.basic_publish(exchange='synkler', routing_key='new', body=pickle.dumps(upload_data))

    channel.queue_declare(queue='upload', durable=True)
    channel.queue_bind(exchange='synkler', queue='upload')
#channel.basic_consume( queue='upload', on_message_callback=callback, auto_ack=True)
#channel.start_consuming()
# pull from the queue of files that the server wants us to start uploading.
# TODO: Figure out how to ... acknowledge that I got it, but not that it's done yet.  I want the server to know it's in process,
#   but that it could fail and may still need to be done again.  I mean... I guess it knows, because it's going to start seeing it show up
#   its download directory, but that's.... maybe if the upload script keeps reporting it as 'new', but never finishes it, that will
#   signal that something needs to happen.
    upload = channel.basic_get( 'upload', True)
    while upload[2] != None:
        file_data = pickle.loads(upload[2])
        filename = file_data['filename']
        dest_dir = file_data['dest_dir']
        md5 = file_data['md5']
        print("got upload request for '" + filename + "'")
        # eventually we should compare md5 to make sure i'm uploading the correct file, 
        #   for peace of mind or security or whatever.
        # upload the file that's being requested.
        rsync_command = [rsync, "--archive", "--partial", upload_dir + "/" + filename, config.synkler_server + ":" + dest_dir + "/"]
        print(' '.join(rsync_command))

        return_code = subprocess.call(rsync_command)
        print("Output: ", return_code)
        # get the next file from the queue
        upload = channel.basic_get('upload', True)

    channel.queue_declare(queue='archive', durable=True)
    channel.queue_bind(exchange='synkler', queue='archive')
# pull from the queue of files that are verified on the server and 
    archive = channel.basic_get('archive', True)
    while archive[2] != None:
        # archive the file that's being requested.
        filename = archive[2]
        print("got archive request for '" + filename + "'")
        # tell uTorrent to move the file to 'Media/Downloads'.  Not sure how that interface will work.
        # get the next file from the queue
        archive = channel.basic_get( 'archive', True)
    time.sleep(5)


# close the connection
connection.close()



