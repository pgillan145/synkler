#!/usr/bin/env python3

import argparse
import csv
import hashlib
import minorimpact
import minorimpact.config
import os
import os.path
import pickle
import pika
import re
import subprocess
import sys
import time


parser = argparse.ArgumentParser(description="Monitor directory and initiate synkler transfers")
parser.add_argument('-c', '--config', help = "config file to use")
parser.add_argument('-v', '--verbose', help = "extra loud output", action='store_true')
parser.add_argument('-i', '--id', nargs='?', help = "id of a specific synkler group", default="default")
args = parser.parse_args()

config = minorimpact.config.getConfig(config=args.config, script_name='synkler', verbose = args.verbose)

cleanup_script = config['default']['cleanup_script']
file_dir = config['default']['file_dir']
pidfile = config['default']['pidfile'] if hasattr(config['default'], 'pidfile') and config['default']['pidfile'] is not None else '/tmp/synkler.pid'
rsync = config['default']['rsync']
rsync_opts = config['default']['rsync_opts'] if hasattr(config['default'], 'rsync_opts') else ''
rsync_opts = list(csv.reader([rsync_opts]))[0]
synkler_server = config['default']['synkler_server']


if (minorimpact.checkforduplicates(pidfile)):
    # TODO: if we run it from the command like, we want some indicator as to why it didn't run, but as a cron
    #   it fills up the log.
    if (args.verbose): sys.exit() #sys.exit('already running')
    else: sys.exit()

connection = pika.BlockingConnection(pika.ConnectionParameters(host=synkler_server))
channel = connection.channel()

# message queues
channel.exchange_declare(exchange='synkler', exchange_type='topic')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='done.' + args.id)
channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='upload.' + args.id)

files = {}
uploads = {}

while True:
    # collect all the files that need to be uploaded
    for f in os.listdir(file_dir):
        if (re.search("^\.", f)):
            continue
        size = minorimpact.dirsize(file_dir + "/" + f)
        mtime = os.path.getmtime(file_dir + "/" + f) 
        if f in files:
            if files[f]['state'] == 'churn':
                if mtime == files[f]['mtime'] and size == files[f]['size']:
                    # Don't md5 the file until we know the file has stopped being written to, just in case
                    if files[f]['md5'] is None:
                        md5 = minorimpact.md5dir(file_dir + "/" + f)
                        files[f]['md5'] = md5
                        if (args.verbose): minorimpact.fprint(f"{f} md5:{md5}")
                        files[f]["state"] = "new"
                else:
                    files[f]['mtime'] = mtime
                    files[f]['size'] = size
        else:
            # TODO: Apparently the server is going to have to tell us what pickle protocols it knows.  This machine supports '5',
            #   but the server only supports '4'...
            if (args.verbose): minorimpact.fprint(f"found {f}")
            files[f] = {'filename':f, 'pickle_protocol':4, 'mtime':mtime, 'size':size, 'state':'churn', 'md5':None, "dir":file_dir}

    # Pull the list of files on the middle upload server
    #if (args.verbose): minorimpact.fprint("checking for synkler commands")
    upload = False
    method,properties,body = channel.basic_get( queue_name, True)
    while body != None:
        routing_key = method.routing_key
        file_data = pickle.loads(body)
        f = file_data['filename']
        if f in files:
            md5 = file_data['md5']
            size = file_data['size']
            mtime = file_data['mtime']
            if (re.match("done", routing_key)):
                if (files[f]["state"] != "done"):
                    if files[f]['md5'] == md5 and files[f]['size'] == size and files[f]["mtime"] == mtime:
                        if (args.verbose): minorimpact.fprint(f"{f} done")
                        files[f]["state"] = "done"
                        if (cleanup_script is not None):
                            command = cleanup_script.split(" ")
                            for i in range(len(command)):
                                if command[i] == "%f":
                                    command[i] = f
                            if (args.verbose): minorimpact.fprint(' '.join(command))
                            return_code = subprocess.call(command)
                            if (return_code != 0):
                                if (args.verbose): minorimpact.fprint(f"Output: {return_code}")
                            else:
                                if f in files: del files[f]
                                if f in uploads: del uploads[f]
                        # DON'T delete from files: since it still exists in the upload directory, we need to maintain a record of what 
                        #   we've already sent so we don't keep trying to upload the same files.
                    else:
                        # TODO: start including the hostname in file_data so I know what server is actuall sending these messages.
                        if (args.verbose): minorimpact.fprint(f"ERROR: {f} on final destination doesn't match")
                        # TODO: definitely need to figure out a good way to restart the process to try and eliminate errors.  Setting 
                        #   it back to 'new' might do it, but the problem is that the end server doesn't know it's got a bad copy,
                        #   and if rsync bunged up someone along the way, i don't know why it wouldn't keep doing so...
                        files[f]["state"] = "new"
                        if f in uploads: del uploads[f]
            elif (re.match("upload", routing_key) and upload is False):
                # I don't want to do more than one upload per loop.  The other servers just keep pumping out signals, I don't want to
                #   too many of them bunching up.  And if things are done, I want them taken care of as soon as possible.
                # TODO: change state to "uploaded?"  don't we have a date on these things so we can limit uploads to no more than once every
                #   five minutes?
                if (files[f]["state"] == "new" and (f not in uploads or (time.time() - uploads[f] > 60))):
                    dest_dir = None
                    if ("dest_dir" in file_data):
                        dest_dir = file_data['dest_dir']
                    else:
                        dest_dir = file_data['dir']

                    if dest_dir is not None and (files[f]['md5'] != md5 or files[f]['size'] != size or files[f]["mtime"] != mtime):
                        upload = True
                        # TODO: really large files break this whole thing because in the time it takes to upload we lose connection to the rabbitmq server. We either need
                        #   to detect the disconnect and reconnect, or, better yet, spawn a separate thread to handle the rsync and wait until it completes before starting
                        #   the next one.
                        rsync_command = [rsync, "--archive", "--partial", *rsync_opts, file_dir + "/" + f, synkler_server + ":" + dest_dir + "/"]
                        if (args.verbose): minorimpact.fprint(' '.join(rsync_command))
                        return_code = subprocess.call(rsync_command)
                        if (return_code == 0):
                            uploads[f] = time.time()
                        if (args.verbose): minorimpact.fprint(f"Output: {return_code}")

        # get the next file from the queue
        method,properties,body = channel.basic_get( queue_name, True)

    for f in files:
        if (files[f]["state"] not in ["churn", "done"]):
            channel.basic_publish(exchange='synkler', routing_key="new." + args.id, body=pickle.dumps(files[f]))

    time.sleep(5)

# close the connection
connection.close()

# we don't strictly need to do this, but it's nice.
os.remove(pidfile)

