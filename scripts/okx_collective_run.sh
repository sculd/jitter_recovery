cd /home/sculd3/projects/jitter_recovery/
LOG="logs/log_okx_collective_$(date +%Y-%m-%dT%H:%M:%S).txt"
# echo $LOG
# date >> $LOG
FILENAME="/home/sculd3/projects/jitter_recovery/main_live_okx_collective.py"
# echo $FILENAME >> $LOG
PYTHONBIN="/home/sculd3/venvs/mlb/bin/python"
RUN_COMMAND="echo $LOG && touch $LOG && date >> $LOG && echo $FILENAME >> $LOG && $PYTHONBIN $FILENAME >> $LOG 2>&1"
pgrep -f $FILENAME || eval $RUN_COMMAND
