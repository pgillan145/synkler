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


parser = argparse.ArgumentParser(description="Synkler download script")
parser.add_argument('-c', '--config', help = "config file to use")
parser.add_argument('-d', '--debug', help = "extra extra loud output", action='store_true')
parser.add_argument('-v', '--verbose', help = "extra loud output", action='store_true')
parser.add_argument('--id', nargs='?', help = "id of a specific synkler group", default="default")
args = parser.parse_args()
if (args.debug): args.verbose = True

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
channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='download.' + args.id)

files = {}
while True:
    #if (args.verbose): minorimpact.fprint(f"checking {file_dir}")
    for f in os.listdir(file_dir):
        if (re.search('^\.', f)):
            continue
        size = minorimpact.dirsize(file_dir + '/' + f)
        mtime = os.path.getmtime(file_dir + '/' + f)
        if (f in files):
            if (size == files[f]['size'] and files[f]['mtime'] == mtime):
                if (files[f]['md5'] is None):
                    md5 = minorimpact.md5dir(file_dir + '/' + f)
                    files[f]['md5'] = md5
                    if (args.verbose): minorimpact.fprint(f"{f} md5:{md5}")
            else:
                files[f]['size'] = size
                files[f]['mtime'] = mtime

    
    #if (args.verbose): minorimpact.fprint("checking for synkler commands")
    # Just do one download per loop, so the rest of the messages don't pile up, and we can report
    #   files as done right away.
    download = False
    method, properies, body = channel.basic_get( queue_name, True)
    while body != None:
        file_data = pickle.loads(body)
        f = file_data['filename']
        dir = file_data['dir']
        md5 = file_data['md5']
        size = file_data['size']
        mtime = file_data['mtime']
        if (args.debug): print(f"file {f}: {md5},{size},{mtime}")
        if (f not in files):
            if (args.verbose): minorimpact.fprint(f"new file:{f}")
            files[f]  = {'filename':f, 'size':0, 'md5':None, 'mtime':0, 'dir':file_dir, 'state':'download'}

        if (files[f]['size'] != size or md5 != files[f]['md5'] or files[f]['mtime'] != mtime):
            if (args.debug): print(f"files[{f}]: {files[f]['md5']},{files[f]['size']},{files[f]['mtime']}")
            if (download is False):
                download = True
                # TODO: really large files break this whole thing because in the time it takes to upload we lose connection to the rabbitmq server. We either need
                #   to detect the disconnect and reconnect, or, better yet, spawn a separate thread to handle the rsync and monitor it for completion before 
                #   starting the next one.
                rsync_command = [rsync, '--archive', '--partial', *rsync_opts, synkler_server + ':\'' + dir + '/' + f + '\'', file_dir + '/']
                if (args.verbose): minorimpact.fprint(' '.join(rsync_command))
                return_code = subprocess.call(rsync_command)
                if (return_code == 0):
                    files[f]['size'] = minorimpact.dirsize(file_dir + '/' + f)
                    files[f]['mtime'] = os.path.getmtime(file_dir + '/' + f)
                    files[f]['md5'] = minorimpact.md5dir(file_dir + '/' + f)
                elif (args.verbose): minorimpact.fprint("Output: ", return_code)
        else:
            if (files[f]['state'] != 'done'):
                if (args.debug): print (f"{f}: doing done")
                files[f]['state'] = 'done'
                if (cleanup_script is not None):
                    if (args.debug): print (f"{f}: doing cleanup")
                    command = cleanup_script.split(' ')
                    for i in range(len(command)):
                        if command[i] == '%f':
                            command[i] = file_dir + '/' + f
                    if (args.verbose): minorimpact.fprint(' '.join(command))
                    return_code = subprocess.call(command)
                    if (return_code != 0):
                        if (args.verbose): minorimpact.fprint(f"Output: {return_code}")

                
        # get the next file from the queue
        method, properies, body = channel.basic_get( queue_name, True)

    filenames = [key for key in files]
    for f in filenames:
        if (files[f]['state'] == 'done'):
            if (args.verbose): minorimpact.fprint(f"{f} done")
            channel.basic_publish(exchange='synkler', routing_key='done.' + args.id, body=pickle.dumps(files[f]))
            # TODO: should we really only send a single message?  It seems like maybe we ought to spam this a few times, just in
            #    case.  Any clients or middlemen can just ignore it if it's not in their list of going concerns.
            del files[f]
    #if (args.verbose): minorimpact.fprint("\n")

    time.sleep(5)

# close the connection
connection.close()



