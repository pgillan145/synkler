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

upload_dir = config.file_dir
rsync = config.rsync

connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.synkler_server))
channel = connection.channel()

# message queues
channel.exchange_declare(exchange='synkler', exchange_type='direct')
channel.queue_declare(queue='new')
channel.queue_bind(exchange='synkler', queue='new')

files = {}

while True:
# collect all the files that need to be uploaded
    print("new files:")
    churn = True
    while churn:
        churn = False
        for f in os.listdir(upload_dir):
            if (re.search("^\.", f)):
                continue
            size = minorimpact.dirsize(upload_dir + "/" + f)
            mtime = os.path.getmtime(upload_dir + "/" + f) 
            if f in files:
                if mtime == files[f]['mtime'] and size == files[f]['size']:
                    # Don't md5 the file until we know the file has stopped being written to, just in case
                    if files[f]['md5'] is None:
                        md5 = minorimpact.md5dir(upload_dir + "/" + f)
                        files[f]['md5'] = md5

                else:
                    print(f"mtime({mtime}) and size({size}) aren't settled for {f}({files[f]['mtime']},{files[f]['size']})")
                    churn = True
                files[f]['mtime'] = mtime
                files[f]['size'] = size
            else:
                # TODO: Apparently the server is going to have to tell us what pickle protocols it knows.  This machine supports '5',
                #   but the server only supports '4'...
                files[f] = {'filename':f, 'pickle_protocol':4, 'mtime':mtime, 'size':size, 'state':'new', 'md5':None}
                churn = True
        time.sleep(1)

    for f in files:
        print(files[f])
        channel.basic_publish(exchange='synkler', routing_key='new', body=pickle.dumps(files[f]))

    # Pull the list of files on the middle upload server
    print("uploads:")
    channel.queue_declare(queue='upload')
    channel.queue_bind(exchange='synkler', queue='upload')
    upload = channel.basic_get( 'upload', True)
    while upload[2] != None:
        file_data = pickle.loads(upload[2])
        print(file_data)
        filename = file_data['filename']
        dest_dir = file_data['dir']
        md5 = file_data['md5']
        size = file_data['size']
        mtime = file_data['mtime']
        if filename in files: 
            if files[filename]['md5'] != md5 or files[filename]['size'] != size:
                print("  uploading " + filename)
                rsync_command = [rsync, "--archive", "--partial", upload_dir + "/" + filename, config.synkler_server + ":" + dest_dir + "/"]
                print(' '.join(rsync_command))

                return_code = subprocess.call(rsync_command)
                print("Output: ", return_code)

        # get the next file from the queue
        upload = channel.basic_get('upload', True)


    # pull from the queue of files that are verified on the server and 
    print("done:")
    channel.queue_declare(queue='done')
    channel.queue_bind(exchange='synkler', queue='done')
    done = channel.basic_get('done', True)
    while done[2] != None:
        # archive the file that's being requested.
        file_data = pickle.loads(done[2])
        print(file_data)
        filename = file_data["filename"]
        md5 = file_data['md5']
        size = file_data['size']
        mtime = file_data['mtime']
        if filename in files and files[filename]['md5'] == md5 and files[filename]['size'] == size:
            # TODO: This file is confirmed downloaded, do what we have to do to move it out of this directory
            #   and into its next phase of existence
            print("DONE " + filename)

        # tell uTorrent to move the file to 'Media/Downloads'.  Not sure how that interface will work.
        # get the next file from the queue
        done = channel.basic_get( 'done', True)
    print("\n")
    time.sleep(5)


# close the connection
connection.close()



