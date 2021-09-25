# synkler
Message queue based rsync wrangler for copying files across multiple servers.

## Overview
Synkler exists to solve the (probably ridiculous) problem of needing to copy from server A (**upload**) to server C(**download**) when neither can connect directly to each other, with the additional complication that the files *will not live at either the source nor the destination after the copy is complete*.

The basic workflow is as follows:  

- *file* arrives on the **upload** server, in the directory synkler is configured to monitor.  
- **upload** notifies **central** (via [rabbitmq](https://www.rabbitmq.com/)) that it has a new file or directory to transder
- once **central** is ready to receive it signals **upload** to begin the rsync
- when the transfer is complete, **central** will verify its local copy of *file* by comparing the md5 hash against what's reported by **upload** 
- **central** will then signal **download** to begin an rsync of *file* from **central** to its own local file system
- once completed, **download** verifies its copy of *file* before signalling to both **central** and **upload** that it has successfully received it
- **upload** and **download** then have the option to run "cleanup" scripts on *file*, which are free to  move it from its original location to wherever
- after a configurable number of minutes, **central** will delete its version of *file*


## Installation
On all three servers (**upload**, **central** and **download**):
```
    $ pip3 install synkler
```
On **central**, install [rabbitmq](https://www.rabbitmq.com/).

**upload** and **download** should both be able to connect to **central** on ports 22 (ssh) and 5672 (rabbitmq).


## Configuration
Modify [sample-config](https://github.com/pgillan145/synkler/blob/main/sample-config) and either copy it one of these locations:
```
$HOME/synkler.conf
$HOME/.config/synkler/synkler.conf
/etc/synkler.conf
```
... or call synkler with the configuration file as command line argument:
```
    $ synkler --config /location/of/synkler/config.file
```
... or set the $SYNKLER\_CONF environment variable:
```
$ export SYNKLER_CONF=/place/i/put/config.files
$ synkler
```

## Starting
As long as you set 'pidfile' in 'synkler.conf', you can call synkler from a cron without worrying about spawning multiple processes:
```
* * * * * /usr/bin/env synkler --verbose >> /tmp/synkler.log 2>&1
```

## Stopping
To stop synkler, just kill the process.  Assuming 'pidfile' is defined in *synkler.conf*:
```
    $ cat *pidfile* | xargs.kill
```

Also remember to disable the cron, of course, if that's how you were starting it:
```
#* * * * * /usr/bin/env synkler --verbose >> /tmp/synkler.log 2>&1
```

## To Do
Major pieces that still need to be added, fixed or investigated:
- rabbitmq doesn't need to be running on **central**, can run anywhere all three servers can access.
- probably need to be able to specify a port number for rabbitmq.
- very large files cause the connection to rabbitmq to timeout, and then the running synkler instance crashes.  Everything still works, since the messages are still in the queue and rsync resumes from where it left off, but it's still dumb.
- needs the option of running it as a service rather than a jenky-ass cron.
- documentashun shud be gooder
- no way to see the overall status of files in the system.
- I heard there might be more than two types of computers, some additional testing might be required.
- while daisy-chaining and having an arbitrary number of **upload** servers is theoretically possible, I haven't tried it.  I should.
- unit testing!
- need to be able to specify an arbitrary ID value so multiple instances can run on the same servers without clobbering each other's queues.

