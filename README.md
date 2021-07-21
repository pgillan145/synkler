# synkler
Message queue based rsync wrangler for backing up files across multiple servers.

## Installation
On all three servers (upload, central and download):
```
$ git clone https://github.com/pgillan145/synkler/
$ cd synkler
$ cp sample-config.py config.py
```
Alter the values in config.py accordingly.

## Start
On the upload server:
```
$ ./upload.py
```

On the central server:
```
$ ./synkler.py
```

On the download server:
```
$ ./download.py
```
