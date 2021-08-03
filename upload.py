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

upload_dir = config.file_dir
rsync = config.rsync

parser = argparse.ArgumentParser(description="Monitor directory and initiate synkler transfers")
#parser.add_argument('filename', nargs="?")
parser.add_argument('--verbose', help = "extra loud output", action='store_true')
#parser.add_argument('--logging', help = "turn on logging", action='store_true')
args = parser.parse_args()



connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.synkler_server))
channel = connection.channel()

# message queues
channel.exchange_declare(exchange='synkler', exchange_type='topic')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='done')
channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='upload')

files = {}

while True:
# collect all the files that need to be uploaded
    if (args.verbose): print(f"checking {upload_dir}")
    churn = True
    while churn:
        churn = False
        for f in os.listdir(upload_dir):
            if (re.search("^\.", f)):
                continue
            size = minorimpact.dirsize(upload_dir + "/" + f)
            mtime = os.path.getmtime(upload_dir + "/" + f) 
            if f in files:
                if files[f]['state'] == 'new':
                    if mtime == files[f]['mtime'] and size == files[f]['size']:
                        # Don't md5 the file until we know the file has stopped being written to, just in case
                        if files[f]['md5'] is None:
                            md5 = minorimpact.md5dir(upload_dir + "/" + f)
                            files[f]['md5'] = md5
                    else:
                        if (args.verbose): print(f"mtime({mtime}) and size({size}) aren't settled for {f}({files[f]['mtime']},{files[f]['size']})")
                        files[f]['mtime'] = mtime
                        files[f]['size'] = size
                        churn = True
            else:
                # TODO: Apparently the server is going to have to tell us what pickle protocols it knows.  This machine supports '5',
                #   but the server only supports '4'...
                if (args.verbose): print(f"found {f}")
                files[f] = {'filename':f, 'pickle_protocol':4, 'mtime':mtime, 'size':size, 'state':'new', 'md5':None, "dir":upload_dir}
                churn = True
        time.sleep(1)

    for f in files:
        if (files[f]["state"] == "new"):
            channel.basic_publish(exchange='synkler', routing_key=files[f]['state'], body=pickle.dumps(files[f]))

    # Pull the list of files on the middle upload server
    if (args.verbose): print("checking for synkler commands")
    method,properties,body = channel.basic_get( queue_name, True)
    while body != None:
        routing_key = method.routing_key
        file_data = pickle.loads(body)
        f = file_data['filename']
        if f in files:
            md5 = file_data['md5']
            size = file_data['size']
            mtime = file_data['mtime']
            if (routing_key == "done"):
                if (files[f]["state"] == "new"):
                    if files[f]['md5'] == md5 and files[f]['size'] == size and files[f]["mtime"] == mtime:
                        # TODO: Move or delete this file or whatever.
                        if (args.verbose): print(f"DONE {f}")
                        files[f]["state"] = "done"
                        if (config.cleanup_script is not None):
                            command = config.cleanup_script.split(" ")
                            for i in range(len(command)):
                                if command[i] == "%f":
                                    command[i] = f
                                    # Not sure if I need the quotes, subprocess might handle that for me.
#command[i] = "\"" + command[i] + "\""
                            if (args.verbose): print(' '.join(command))
                            return_code = subprocess.call(command)
                            if (args.verbose): print("Output: ", return_code)
                    else:
                        # TODO: start including the hostname in file_data so I know what server is actuall sending these messages.
                        if (args.verbose): print(f"ERROR: {f} on final destination doesn't match")
                        # TODO: definitely need to figure out a good way to restart the process to try and eliminate errors.  Setting 
                        #   it back to 'new' might do it, but the problem is that the end server doesn't know it's got a bad copy,
                        #   and if rsync bunged up someone along the way, i don't know why it wouldn't keep doing so...
                        files[f]["state"] = "error"
            elif (routing_key == "upload"):
                if (files[f]["state"] == "new"):
                    dest_dir = None
                    if ("dest_dir" in file_data):
                        dest_dir = file_data['dest_dir']
                    else:
                        dest_dir = file_data['dir']

                    if dest_dir is not None and (files[f]['md5'] != md5 or files[f]['size'] != size or files[f]["mtime"] != mtime):
                        if (args.verbose): print("  uploading " + f)
                        rsync_command = [rsync, "--archive", "--partial", upload_dir + "/" + f, config.synkler_server + ":" + dest_dir + "/"]
                        if (args.verbose): print(' '.join(rsync_command))
                        return_code = subprocess.call(rsync_command)
                        if (args.verbose): print("Output: ", return_code)

        # get the next file from the queue
        method,properties,body = channel.basic_get( queue_name, True)

    if (args.verbose): print("\n")
    time.sleep(5)


# close the connection
connection.close()



