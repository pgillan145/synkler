#!/usr/bin/python3

import argparse
from datetime import datetime
import config
import hashlib
import minorimpact
import os
import os.path
import pickle
import pika
import re
import shutil
import sys
import time

def main():
    parser = argparse.ArgumentParser(description="Synkler middle manager")
    parser.add_argument('-v', '--verbose', help = "extra loud output", action='store_true')
    parser.add_argument('--id', nargs='?', help = "id of a specific synkler group", default="default")

    args = parser.parse_args()

    pidfile = config.pidfile if hasattr(config, 'pidfile') and config.pidfile is not None else "/tmp/synkler.pid"
    if (minorimpact.checkforduplicates(pidfile)):
        # TODO: if we run it from the command like, we want some indicator as to why it didn't run, but as a cron
        #   it fills up the log.
        if (args.verbose): sys.exit() #sys.exit('already running')
        else: sys.exit()


    connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.synkler_server))
    channel = connection.channel()

    channel.exchange_declare(exchange='synkler', exchange_type='topic')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='new.' + args.id)
    channel.queue_bind(exchange='synkler', queue=queue_name, routing_key='done.' + args.id)

    files = {}
    while (True):
        #if (args.verbose): minorimpact.fprint("checking for new files")
        method, properties, body = channel.basic_get( queue=queue_name, auto_ack=True)
        while body != None:
            routing_key = method.routing_key
            file_data = pickle.loads(body)
            f = file_data["filename"]
            if (re.match("new", routing_key)):
                if (f not in files):
                    # TODO: Don't just blindly upload everything, set the state to "new" then verify that we've got space for it
                    #   before setting the state to "upload".
                    if (args.verbose): minorimpact.fprint(f"receiving {f}")
                    files[f] = {"filename":f, "dir":config.download_dir, "size":0, "mtime":None, "md5":None, "state":"upload", "moddate":int(time.time())}
                elif (files[f]["size"] == file_data["size"] and files[f]["mtime"] == file_data["mtime"] and files[f]["md5"] == file_data["md5"]):
                    if (files[f]["state"] == "upload"):
                        if (args.verbose): minorimpact.fprint(f"supplying {f}")
                        files[f]["state"] = "download"
                        files[f]["moddate"] = int(time.time())
            elif (re.match("done", routing_key)):
                if (f in files):
                    if (files[f]["state"] != "done"):
                        if (args.verbose): minorimpact.fprint(f"{f} done")
                        files[f]["state"] = "done"
                        files[f]["moddate"] = int(time.time())
            method, properties, body = channel.basic_get( queue=queue_name, auto_ack=True)

        for f in os.listdir(config.download_dir):
            if (re.search("^\.", f)):
                continue

            size = minorimpact.dirsize(config.download_dir + "/" + f)
            mtime = os.path.getmtime(config.download_dir + "/" + f)
            if (f not in files):
                # These files are more than 30 minutes old and haven't been reported in, they can be
                #   axed.
                if (int(time.time()) - mtime > 1800):
                    if (args.verbose): minorimpact.fprint("deleting " + config.download_dir + "/" + f)
                    if (os.path.isdir(config.download_dir + "/" + f)):
                        shutil.rmtree(config.download_dir + "/" + f)
                    else:
                        os.remove(config.download_dir + "/" + f)
                continue

            if (f in files):
                if (files[f]["state"] == "upload"):
                    if (size == files[f]["size"] and files[f]["mtime"] == mtime):
                        # The file has stopped changing, we can assume it's no longer being written to -- grab the md5sum.
                        if (files[f]["md5"] == None):
                            md5 = minorimpact.md5dir(config.download_dir + "/" + f)
                            files[f]["md5"] = md5
                            files[f]["moddate"] = int(time.time())
                            if (args.verbose): minorimpact.fprint(f"{f} md5:{md5}")
                    else:
                        files[f]["size"] = size
                        files[f]["mtime"] = mtime
                        files[f]["moddate"] = time.time()

        filenames = [key for key in files]
        for f in filenames:
            if (files[f]["state"] == "done" and (int(time.time()) - files[f]["moddate"] > 60)):
                if (args.verbose): minorimpact.fprint(f"clearing {f}")
                del files[f]
            elif (files[f]["state"] == "upload"):
                channel.basic_publish(exchange='synkler', routing_key='upload.' + args.id, body=pickle.dumps(files[f], protocol=4))
            elif (files[f]["state"] == "download"):
                channel.basic_publish(exchange='synkler', routing_key='download.' + args.id, body=pickle.dumps(files[f], protocol=4))

        #if (args.verbose): minorimpact.fprint("\n")
        time.sleep(5)

if __name__ == "__main__":
    main()

