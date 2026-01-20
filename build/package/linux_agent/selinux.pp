#!/bin/bash

policy_module(pbs_plus_agent, 1.0.0)

require {
    type bin_t;
    type systemd_unit_file_t;
    type initrc_t;
}

# Allow agent to write to its own binary
allow pbs_plus_agent_t bin_t:file { write create rename unlink };

# Allow systemd service restarts
allow pbs_plus_agent_t systemd_unit_file_t:service { start stop restart reload };
allow pbs_plus_agent_t initrc_t:unix_stream_socket connectto;
