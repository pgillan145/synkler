#!/usr/bin/env python3

import hashlib
import minorimpact
import os
import os.path
import pickle
import pika
import re
import subprocess
import time

download_dir = config.download_dir
synkler_server = config.synkler_server
rsync = config.rsync

connection = pika.BlockingConnection(pika.ConnectionParameters(host=synkler_server))
channel = connection.channel()

# message queues
channel.exchange_declare(exchange='synkler', exchange_type='direct')
channel.queue_declare(queue='download', durable=True)
channel.queue_bind(exchange='synkler', queue='download')

while True:
# collect all the files that need to be uploaded
    #for f in os.listdir(download_dir):
    #    if (re.search("^\.", f)):
    #        continue
    #    md5 = md5dir(upload_dir + "/" + f)
    #    size = sizedir(upload_dir + "/" + f)
    #    upload_data = {"filename":f, "md5":md5, "size":size, "pickle_protocol":pickle.HIGHEST_PROTOCOL}
    #    print("sending about " + f + " to synkler")
    #    print(upload_data)
    #    # report the list of files to the central server.  It doesn't matter if
    #    #   we've sent them before, the server will keep track of duplicates.
    #    channel.basic_publish(exchange='synkler', routing_key='new', body=pickle.dumps(upload_data))

    download = channel.basic_get( 'download', True)
    while download[2] != None:
        file_data = pickle.loads(download[2])
        filename = file_data['filename']
        dl_dir = file_data['download_dir']
        md5 = file_data['md5']
        print("got download request for '" + filename + "'")
        rsync_command = [rsync, "--archive", "--partial", synkler_server + ":" + dl_dir + "/\"" + filename + "\"", download_dir + "/"]
        print(' '.join(rsync_command))

        return_code = subprocess.call(rsync_command)
        print("Output: ", return_code)
        # get the next file from the queue
        download = channel.basic_get('download', True)

    time.sleep(5)


# close the connection
connection.close()



