#!/bin/sh
# Pre-install: stop and remove legacy pxar-direct-mount / pxar-socket-api

# Unmount any FUSE mounts from old pxar-direct-mount
mount | grep 'fuse.pxar.direct.mount\|pxar-direct-mount' | awk '{print $3}' | while read -r mnt; do
    echo "Unmounting legacy pxar-direct-mount at $mnt"
    fusermount -u "$mnt" 2>/dev/null || umount -l "$mnt" 2>/dev/null || true
done

# Kill running instances
killall pxar-direct-mount 2>/dev/null || true
killall pxar-socket-api 2>/dev/null || true

# Remove old systemd units if present
for unit in pxar-direct-mount pxar-socket-api; do
    if [ -f "/lib/systemd/system/${unit}.service" ] || [ -f "/etc/systemd/system/${unit}.service" ]; then
        echo "Stopping and disabling legacy ${unit} service"
        systemctl stop "${unit}.service" 2>/dev/null || true
        systemctl disable "${unit}.service" 2>/dev/null || true
        rm -f "/lib/systemd/system/${unit}.service" "/etc/systemd/system/${unit}.service"
        systemctl daemon-reload 2>/dev/null || true
    fi
done

exit 0
