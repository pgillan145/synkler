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
channel.exchange_declare(exchange='synkler', exchange_type='topic')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='done')
channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='upload')

files = {}
#data_file = config.data_dir + "/files.pickle"
#if (os.path.exists(data_file)):
#   files = pickle.load(open(data_file, "rb"))

while True:
# collect all the files that need to be uploaded
    print("my files:")
    churn = True
    while churn:
        churn = False
        for f in os.listdir(upload_dir):
            if (re.search("^\.", f)):
                continue
            size = minorimpact.dirsize(upload_dir + "/" + f)
            mtime = os.path.getmtime(upload_dir + "/" + f) 
            if f in files:
                if files[f]['state'] != 'new':
                    continue
                if mtime == files[f]['mtime'] and size == files[f]['size']:
                    # Don't md5 the file until we know the file has stopped being written to, just in case
                    if files[f]['md5'] is None:
                        md5 = minorimpact.md5dir(upload_dir + "/" + f)
                        files[f]['md5'] = md5
                else:
                    print(f"mtime({mtime}) and size({size}) aren't settled for {f}({files[f]['mtime']},{files[f]['size']})")
                    files[f]['mtime'] = mtime
                    files[f]['size'] = size
                    churn = True
            else:
                # TODO: Apparently the server is going to have to tell us what pickle protocols it knows.  This machine supports '5',
                #   but the server only supports '4'...
                files[f] = {'filename':f, 'pickle_protocol':4, 'mtime':mtime, 'size':size, 'state':'new', 'md5':None, "dir":upload_dir}
                churn = True
        time.sleep(1)

    for f in files:
        if (files[f]["state"] == "new"):
            print(files[f])
            channel.basic_publish(exchange='synkler', routing_key=files[f]['state'], body=pickle.dumps(files[f]))

    # Pull the list of files on the middle upload server
    print("uploads:")
    method,properties,body = channel.basic_get( queue_name, True)
    while body != None:
        routing_key = method.routing_key
        file_data = pickle.loads(body)
        filename = file_data['filename']
        if filename not in files:
            continue
        md5 = file_data['md5']
        size = file_data['size']
        mtime = file_data['mtime']
        if (routing_key == "done"):
            if files[filename]['md5'] == md5 and files[filename]['size'] == size and files[filename]["mtime"] == mtime and files[filename]["state"] != "done":
                # TODO: Move or delete this file or whatever.
                print(f"DONE {filename}")
                files[filename]["state"] = "done"
            else:
                # TODO: start including the hostname in file_data so I know what server is actuall sending these messages.
                print(f"ERROR: {filename} on final destination doesn't match")
                files[filename]["state"] = "error"
        elif (routing_key == "upload"):
            if (files[filename]["state"] != "new"):
                #print(f"{filename} status is {files[filename]['state']}")
                continue
#        if (files[filename]['md5'] == md5 and files[filename]['size'] == size and files[filename]["mtime"] == mtime):
#                print(f"uploaded {filename}")
#                files[filename]["state"] = "uploaded"
#                continue
            dest_dir = None
            if ("dest_dir" in file_data):
                dest_dir = file_data['dest_dir']
            else:
                dest_dir = file_data['dir']

            if dest_dir is not None:
                print("  uploading " + filename)
                rsync_command = [rsync, "--archive", "--partial", upload_dir + "/" + filename, config.synkler_server + ":" + dest_dir + "/"]
                print(' '.join(rsync_command))
                return_code = subprocess.call(rsync_command)
                print("Output: ", return_code)

        # get the next file from the queue
        method,properties,body = channel.basic_get( queue_name, True)

    print("\n")
    #data_file = config.data_dir + "/files.pickle"
    #pickle.dump(files, open(data_file, "wb"))
    time.sleep(5)


# close the connection
connection.close()



