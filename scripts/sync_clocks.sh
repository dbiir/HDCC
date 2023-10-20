#!/bin/bash
HOSTS="$1"
if [ -z "$HOSTS" ]
then
    HOSTS=$(cat ../vcloud_ifconfig.txt)
fi
#CMD="ntpdate -q clock-1.cs."
CMD="ntpdate -b clock-1.cs."
./vcloud_cmd.sh "$HOSTS" "$CMD"
