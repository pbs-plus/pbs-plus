#!/bin/bash

ls "$BINARY_PATH"
ls -al
chmod 755 ./build/package/debian/DEBIAN/postinst
mkdir -p ./build/package/debian/usr/bin
cp "$BINARY_PATH"/pbs-plus-overlay ./build/package/debian/usr/bin/pbs-plus-overlay
