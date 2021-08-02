#!/usr/bin/env python3

file_dir = "./downloads"
rsync = "/usr/bin/rsync"
synkler_server = "localhost"

# command to run after a complete transfer.
# options:
#   %f - full path/filename of the completed file.
#cleanup_script = "mv -a %f /tmp/trash"
