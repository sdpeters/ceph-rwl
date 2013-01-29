#!/bin/sh

set -e

# first stage a bunch of files
echo "mkaing dirs..."
find /usr/bin -type d -exec mkdir -p .\{\} \;
echo "touchign files..."
find /usr/bin -type f -exec touch .\{\} \;

# try to drop caches
echo "dropping caches..."
echo 2 > /proc/sys/vm/drop_caches || true

# try to abort a readdir
for f in `seq 1 10`; do
    echo "iteration $f..."
    find . &
    CH=$!
    sleep .1
    kill -9 $CH || true
    wait
done

echo OK
