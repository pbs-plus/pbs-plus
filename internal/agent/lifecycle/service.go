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
Restart=always
RestartSec=3
EnvironmentFile=-/etc/pbs-plus-agent/agent.env

RuntimeDirectory=pbs-plus-agent
RuntimeDirectoryMode=0755

CapabilityBoundingSet=CAP_DAC_READ_SEARCH CAP_DAC_OVERRIDE
AmbientCapabilities=CAP_DAC_READ_SEARCH CAP_DAC_OVERRIDE
NoNewPrivileges=yes
LimitNOFILE=1048576

ProtectSystem=full
ProtectHome=no
PrivateTmp=yes

ReadWritePaths=/opt/pbs-plus-agent /var/lib/pbs-plus-agent /var/log/pbs-plus-agent /etc/pbs-plus-agent

[Install]
WantedBy=multi-user.target
`

const OPENRC_SCRIPT = `#!/sbin/openrc-run
name="{{.DisplayName}}"
description="{{.Description}}"

command="{{.Path}}"
command_user="pbsplus:pbsplus"
directory="/var/lib/pbs-plus-agent"
agent_env="/etc/pbs-plus-agent/agent.env"

output_log="/var/log/pbs-plus-agent/agent.log"
error_log="/var/log/pbs-plus-agent/agent.error.log"

supervisor=supervise-daemon
respawn_delay=3
respawn_max=0

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
`
