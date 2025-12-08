#!/usr/bin/execlineb -P
# execline script run by s6-overlay before services start

# Ensure dirs exist even if volumes override them
if { mkdir -p /var/lib/pbs-plus-agent }
if { mkdir -p /var/log/pbs-plus-agent }
if { mkdir -p /run/pbs-plus-agent }
if { mkdir -p /etc/pbs-plus-agent }

# Fix ownership and perms (idempotent)
if { chown -R pbsplus:pbsplus /var/lib/pbs-plus-agent /var/log/pbs-plus-agent /run/pbs-plus-agent /etc/pbs-plus-agent }
if { chmod 0750 /var/lib/pbs-plus-agent }
if { chmod 0750 /var/log/pbs-plus-agent }
if { chmod 0755 /run/pbs-plus-agent }
if { chmod 0750 /etc/pbs-plus-agent }

