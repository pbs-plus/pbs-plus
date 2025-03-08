#!/bin/bash

ls "$BINARY_PATH"
ls -al
chmod 755 ./build/package/server/debian/DEBIAN/postinst
mkdir -p ./build/package/server/debian/usr/bin
cp "$BINARY_PATH"/pbs-plus ./build/package/server/debian/usr/bin/pbs-plus
