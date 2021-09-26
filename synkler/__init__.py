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
import shutil
import subprocess
import sys
import time

__version__ = "0.0.3"

def main():
    parser = argparse.ArgumentParser(description="Synkler")
    parser.add_argument('-c', '--config', help = "Read configuration options from CONFIG")
    parser.add_argument('-v', '--verbose', help = "extra loud output", action='store_true')
    #parser.add_argument('--id', nargs='?', help = "id of a specific synkler group", default='default')
    args = parser.parse_args()
    args.id = "default"

    config = minorimpact.config.getConfig(config = args.config)
    cleanup_script = config['default']['cleanup_script'] if ('cleanup_script' in config['default']) else None
    file_dir = config['default']['file_dir']
    keep_minutes = int(config['default']['keep_minutes']) if ('keep_minutes' in config['default']) else 30
    mode = config['default']['mode'] if ('mode' in config['default']) and config['default']['mode'] is not None else 'central'
    pidfile = config['default']['pidfile'] if ('pidfile' in config['default']) and config['default']['pidfile'] is not None else "/tmp/synkler.pid"
    rsync = config['default']['rsync'] if ('rsync' in config['default']) else None
    rsync_opts = config['default']['rsync_opts'] if ('rsync_opts' in config['default']) else ''
    rsync_opts = list(csv.reader([rsync_opts]))[0]
    synkler_server = config['default']['synkler_server'] if ('synkler_server' in config['default']) else None

    if (file_dir is None):
        sys.exit(f"'file_dir' is not set")
    if (os.path.exists(file_dir) is False):
        sys.exit(f"'{file_dir}' does not exist.")
    if (synkler_server is None):
        sys.exit(f"'synkler_server' is not set")
    if (rsync is None and mode != 'central'):
        sys.exit(f"'rsync' is not set")

    if (pidfile is not None and minorimpact.checkforduplicates(pidfile)):
        # TODO: if we run it from the command line, we want some indicator as to why it didn't run, but as a cron
        #   it fills up the log.  We really should use a logging module rather than STDOUT.
        if (args.verbose): sys.exit() #sys.exit('already running')
        else: sys.exit()

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=synkler_server))
    channel = connection.channel()

    channel.exchange_declare(exchange='synkler', exchange_type='topic')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    if mode == 'central':
        channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='done.' + args.id)
        channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='new.' + args.id)
    elif mode == 'download':
        channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='download.' + args.id)
    elif mode == 'upload':
        channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='done.' + args.id)
        channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='upload.' + args.id)
    else:
        sys.exit(f"'mode' must be 'upload','central', or 'download'")

    start_time = int(time.time())

    files = {}
    while (True):
        for f in os.listdir(file_dir):
            if (re.search('^\.', f)):
                continue

            mtime = os.path.getmtime(file_dir + '/' + f)
            size = minorimpact.dirsize(file_dir + '/' + f)
            if (f in files):
                if (size == files[f]['size'] and files[f]['mtime'] == mtime):
                    # The file has stopped changing, we can assume it's no longer being written to -- grab the md5sum.
                    if (files[f]['md5'] is None):
                        md5 = minorimpact.md5dir(f'{file_dir}/{f}')
                        files[f]['md5'] = md5
                        files[f]['mod_date'] = int(time.time())
                        if (args.verbose): minorimpact.fprint(f"{f} md5:{md5}")
                        if (mode == 'upload'):
                            files[f]['state'] = 'new'
                else:
                    files[f]['size'] = size
                    files[f]['mtime'] = mtime
                    files[f]['mod_date'] = int(time.time())
            else:
                # These files are more than 30 minutes old and haven't been reported in, they can be
                #   axed.
                if (mode == 'central'):
                    if (int(time.time()) - start_time > (keep_minutes * 60) and int(time.time()) - mtime > (keep_minutes * 60)):
                        if (args.verbose): minorimpact.fprint(f"deleting {file_dir}/{f}")
                        if (os.path.isdir(file_dir + '/' + f)):
                            shutil.rmtree(file_dir + '/' + f)
                        else:
                            os.remove(file_dir + '/' + f)
                elif (mode == 'upload'):
                    files[f] = {'filename':f, 'pickle_protocol':4, 'mtime':mtime, 'size':size, 'state':'churn', 'md5':None, 'dir':file_dir, 'mod_date':int(time.time()) }

        # This 'transfer' flag makes sure we don't to more than one upload or download during each main
        #   loop.  Otherwise the message queue gets backed up, and the other servers have to wait a long
        #   time before they can start processing the individual files.
        transfer = False
        uploads =  {}
        method, properties, body = channel.basic_get( queue=queue_name, auto_ack=True)
        while body != None:
            routing_key = method.routing_key
            file_data = pickle.loads(body)
            f = file_data['filename']
            md5 = file_data['md5']
            mtime = file_data['mtime']
            size = file_data['size']
            if (re.match('new', routing_key) and mode == 'central'):
                if (f not in files):
                    # TODO: Don't just blindly upload everything, set the state to 'new' then verify that we've got space for it
                    #   before setting the state to 'upload'.
                    if (args.verbose): minorimpact.fprint(f"receiving {f}")
                    files[f] = {'filename':f, 'dir':file_dir, 'size':0, 'mtime':None, 'md5':None, 'state':'upload'}
                elif (files[f]['size'] == size and files[f]['mtime'] == mtime and files[f]['md5'] == md5):
                    if (files[f]['state'] == 'upload'):
                        if (args.verbose): minorimpact.fprint(f"supplying {f}")
                        files[f]['state'] = 'download'
                files[f]['mod_date'] = int(time.time())
            elif (re.match('done', routing_key)):
                if (f in files):
                    if (files[f]['state'] != 'done'):
                        files[f]['state'] = 'done'
                        files[f]['mod_date'] = int(time.time())
                        if (mode == 'upload'):
                            if files[f]['md5'] == md5 and files[f]['size'] == size and files[f]["mtime"] == mtime:
                                if (cleanup_script is not None):
                                    command = cleanup_script.split(' ')
                                    for i in range(len(command)):
                                        if command[i] == '%f':
                                            command[i] = f
                                        elif command[i] == '%F':
                                            command[i] = file_dir + '/' + f
                                    if (args.verbose): minorimpact.fprint("running cleanup script:" + ' '.join(command))
                                    return_code = subprocess.call(command)
                                    if (return_code != 0):
                                        if (args.verbose): minorimpact.fprint(" ... FAILED (" + str(return_code) + ")")
                                    else:
                                        if (args.verbose): minorimpact.fprint(" ... DONE")
                                        # Since the file no longer lives in the download directory, delete it from the internal
                                        #   dict.
                                        del files[f]
                            else:
                                if (args.verbose): minorimpact.fprint(f"ERROR: {f} on final destination doesn't match")    
                                files[f]['state'] = 'new'
                            # Leave the file in the the internal dict, otherwise we'd just keep trying to upload it over
                            #   and over again -- but delete it from the 'uploads' dict, just in case it's requested again.
                            if f in uploads: del uploads[f]
                        if (args.verbose): minorimpact.fprint(f"{f} done")
            elif (re.match('upload', routing_key) and mode == 'upload' and transfer is False):
                # TODO: change state to "uploaded?"  don't we have a date on these things so we can limit uploads
                #   to no more than once ever five minutes?
                if (files[f]['state'] == 'new' and (f not in uploads or (time.time() - uploads[f] > 60))):
                    dest_dir = file_data['dir']
                    if (dest_dir is not None and (files[f]['md5'] != md5 or files[f]['size'] != size or files[f]['mtime'] != mtime)):
                        transfer = True
                        # TODO: really large files break this whole thing because in the time it takes to upload
                        #   we lose connection to the rabbitmq server.  We either need to detect the disconnect 
                        #   and reconnect or spawn a separate thread to handle the rsync and wait until it
                        #   completes before starting the next one.
                        rsync_command = [rsync, '--archive', '--partial', *rsync_opts, f'{file_dir}/{f}', f'{synkler_server}:{dest_dir}/']
                        if (args.verbose): minorimpact.fprint(' '.join(rsync_command))
                        return_code = subprocess.call(rsync_command)
                        if (return_code != 0):
                            if (args.verbose): minorimpact.fprint(f" ... FAILED ({return_code})")
                        else:
                            uploads[f] = int(time.time())
                            if (args.verbose): minorimpact.fprint(f" ... DONE")
            elif (re.match('download', routing_key) and mode == 'download'):
                file_data = pickle.loads(body)
                f = file_data['filename']
                dir = file_data['dir']
                md5 = file_data['md5']
                size = file_data['size']
                mtime = file_data['mtime']

                if (f not in files):
                    if (args.verbose): minorimpact.fprint(f"new file:{f}")
                    files[f]  = {'filename':f, 'size':0, 'md5':None, 'mtime':0, 'dir':file_dir, 'state':'download', 'mod_date':int(time.time()) }

                if (files[f]['size'] != size or md5 != files[f]['md5'] or files[f]['mtime'] != mtime):
                    if (transfer is False):
                        transfer = True
                        rsync_command = [rsync, '--archive', '--partial', *rsync_opts, f'{synkler_server}:"{dir}/{f}"', f'{file_dir}/']
                        if (args.verbose): minorimpact.fprint(' '.join(rsync_command))
                        return_code = subprocess.call(rsync_command)
                        if (return_code == 0):
                            if (args.verbose): minorimpact.fprint(f" ... DONE")
                            files[f]['size'] = minorimpact.dirsize(file_dir + '/' + f)
                            files[f]['mtime'] = os.path.getmtime(file_dir + '/' + f)
                            files[f]['md5'] = minorimpact.md5dir(file_dir + '/' + f)
                            files[f]['mod_date'] = int(time.time())
                        elif (args.verbose): minorimpact.fprint(f" ... FAIL ({return_code})")
                else:
                    if (files[f]['state'] != 'done'):
                        files[f]['state'] = 'done'
                        files[f]['mod_date'] = int(time.time())
                        if (cleanup_script is not None):
                            command = cleanup_script.split(' ')
                            for i in range(len(command)):
                                if command[i] == '%f':
                                    command[i] = f
                                elif command[i] == '%F':
                                    command[i] = file_dir + '/' + f
                            if (args.verbose): minorimpact.fprint("running cleanup script:" + ' '.join(command))
                            return_code = subprocess.call(command)
                            if (return_code == 0):
                                if (args.verbose): minorimpact.fprint(f" ... DONE")
                            else:
                                if (args.verbose): minorimpact.fprint(f" ... FAILED ({return_code})")
                        if (args.verbose): minorimpact.fprint(f"{f}: done")

            # Get the next item from queue.
            method, properties, body = channel.basic_get( queue=queue_name, auto_ack=True)

        filenames = [key for key in files]
        for f in filenames:
            if (mode == 'central'):
                if (files[f]['state'] == 'done' and (int(time.time()) - files[f]['mod_date'] > 60)):
                    if (args.verbose): minorimpact.fprint(f"clearing {f}")
                    del files[f]
                elif (files[f]['state'] == 'upload' and (int(time.time()) - files[f]['mod_date'] < 30)):
                    # Stop sending an 'upload' signal if we haven't gotten a 'new' message within the last 30 seconds.  Either the
                    #   file no longer exists, or 'upload' is blocking and isn't getting the messages anyway.
                    # TODO: Figure out when it's safe to delete zombie files from the array.
                    channel.basic_publish(exchange='synkler', routing_key='upload.' + args.id, body=pickle.dumps(files[f], protocol=4))
                elif (files[f]['state'] == 'download'):
                    channel.basic_publish(exchange='synkler', routing_key='download.' + args.id, body=pickle.dumps(files[f], protocol=4))
            elif (mode == 'upload'):
                if (files[f]['state'] not in ['churn', 'done']):
                    # TODO: Figure out if I need this.  Is there a fourth state this can be in?
                    if (files[f]['state'] != 'new'): minorimpact.fprint(f"{f}:{files[f]['state']}???")
                    channel.basic_publish(exchange='synkler', routing_key='new.' + args.id, body=pickle.dumps(files[f]))
            elif (mode == 'download'):
                if (files[f]['state'] == 'done'):
                    channel.basic_publish(exchange='synkler', routing_key='done.' + args.id, body=pickle.dumps(files[f]))
                    # TODO: should we really only send a single message?  It seems like maybe we ought to spam this a few times, just in
                    #    case.  Any clients or middlemen can just ignore it if it's not in their list of going concerns.
                    del files[f]


        time.sleep(5)

    # TODO: Figure out a way to make sure these get called, or get rid of them.
    connection.close()
    # We don't strictly need to do this, but it's nice.
    os.remove(pidfile)

if __name__ == '__main__':
    main()

