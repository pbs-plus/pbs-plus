#!/bin/bash

ls "$BINARY_PATH"
ls -al
chmod 755 ./build/package/linux_agent/debian/DEBIAN/postinst
mkdir -p ./build/package/linux_agent/debian/usr/bin
cp "$BINARY_PATH"/pbs-plus-agent ./build/package/linux_agent/debian/usr/bin/pbs-plus-agent
