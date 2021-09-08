#!/usr/bin/env python3

file_dir = "./downloads"
# rsync command
rsync = "/usr/bin/env rsync"
# Additional options to pass to rsync, if necessary
#rsync_opts = [ "--rsh=/usr/bin/ssh" ]
rsync_opts = []
synkler_server = "localhost"

# command to run after a complete transfer.
# options:
#   %f - filename of the completed file.
#cleanup_script = "mv -a %f /tmp/trash"
