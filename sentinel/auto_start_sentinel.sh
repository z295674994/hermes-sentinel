#!/bin/bash
LOG=/home/ubuntu/smart-money-scanner/sentinel/logs/auto_start.log
STATUS=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 5 'https://fapi.binance.com/fapi/v1/ping' 2>/dev/null)
if [ "$STATUS" = "200" ]; then
    if ! systemctl is-active --quiet sentinel; then
        echo "$(date): UP - starting sentinel" >> $LOG
        sudo systemctl unmask sentinel 2>/dev/null; sudo systemctl start sentinel
    fi
else
    echo "$(date): banned ($STATUS)" >> $LOG
fi