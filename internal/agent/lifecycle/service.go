package lifecycle

const SYSTEMD_SCRIPT = `[Unit]
Description={{.DisplayName}}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pbsplus
Group=pbsplus
WorkingDirectory=/var/lib/pbs-plus-agent
ExecStart={{.Path}}
Restart=on-failure
EnvironmentFile=-/etc/pbs-plus-agent/agent.env

CapabilityBoundingSet=CAP_DAC_READ_SEARCH CAP_DAC_OVERRIDE
AmbientCapabilities=CAP_DAC_READ_SEARCH CAP_DAC_OVERRIDE
NoNewPrivileges=yes

ProtectSystem=true
ProtectHome=no
PrivateTmp=no

ReadWritePaths=/usr/bin /var/lib/pbs-plus-agent /var/log/pbs-plus-agent /etc/pbs-plus-agent /run/pbs-plus-agent

[Install]
WantedBy=multi-user.target
`

const OPENRC_SCRIPT = `#!/sbin/openrc-run
name="{{.DisplayName}}"
description="{{.Description}}"

command="{{.Path}}"
command_user="pbsplus:pbsplus"
pidfile="/run/pbs-plus-agent/pbs-plus-agent.pid"
directory="/var/lib/pbs-plus-agent"
agent_env="/etc/pbs-plus-agent/agent.env"

output_log="/var/log/pbs-plus-agent/agent.log"
error_log="/var/log/pbs-plus-agent/agent.error.log"

command_background="yes"
start_stop_daemon_args="--nicelevel 19 --ionice idle"

depend() {
    need net
    after firewall
}

start_pre() {
    checkpath --directory --owner pbsplus:pbsplus --mode 0755 /run/pbs-plus-agent
    checkpath --directory --owner pbsplus:pbsplus --mode 0750 /var/log/pbs-plus-agent
    checkpath --directory --owner pbsplus:pbsplus --mode 0750 /var/lib/pbs-plus-agent
    
    if [ -f "$agent_env" ]; then
        while IFS= read -r line || [ -n "$line" ]; do
            case "$line" in
                ''|#*) continue ;;
                *=*) 
                    key=$(echo "${line%%=*}" | tr -d ' ')
                    val=$(echo "${line#*=}" | sed "s/^['\"]//;s/['\"]$//")
                    export "$key=$val"
                    ;;
            esac
        done < "$agent_env"
    fi

    if command -v getcap >/dev/null 2>&1; then
        if ! getcap "$command" | grep -q "cap_dac_read_search"; then
            ewarn "Warning: Binary lacks CAP_DAC_READ_SEARCH. Backups may fail."
        fi
    fi
}

stop_post() {
    [ -f "${pidfile}" ] && rm -f "${pidfile}"
}
`
