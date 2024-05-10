#!/bin/bash

# The command we want to monitor
PROCESS_CMD="sudo -u ec2-user python3 -u /home/ec2-user/agent/agent.py"
# The rerun command
RERUN_CMD="sudo -u ec2-user python3 -u /home/ec2-user/agent/agent.py >> /home/ec2-user/agent/run.log  2>&1 &"

# Grep Python process
GREP_PYTHON_CMD="ps aux | grep --line-buffered 'python3 -u /home/ec2-user/agent/agent.py' | grep -v grep | awk '{print \$2}' > /home/ec2-user/agent/previous_process_ids"
GREP_TSHARK_CMD="ps aux | grep --line-buffered 'tshark -i capture0' | grep -v grep | awk '{print \$2}' >> /home/ec2-user/agent/previous_process_ids"

# Kill command
KILL_PREVIOUS_PROCESS_CMD="cat /home/ec2-user/agent/previous_process_ids | xargs kill"

# get process IDs
eval "$GREP_PYTHON_CMD"
eval "$GREP_TSHARK_CMD"

# PROCESS_ID=$(pgrep -f "$PROCESS_CMD")

eval "$RERUN_CMD"
echo "$(date) Process rerun." >> /home/ec2-user/agent/monitor.log

eval "$KILL_PREVIOUS_PROCESS_CMD"
echo "$(date) Previous process killed." >> /home/ec2-user/agent/monitor.log
