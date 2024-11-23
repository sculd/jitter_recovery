PROJECT_DIR="/home/$(whoami)/projects/jitter_recovery"
cd $PROJECT_DIR || exit
LOG="${PROJECT_DIR}/logs/log_okx_collective_$(date +%Y-%m-%dT%H:%M:%S).txt"
LOG_PING="${PROJECT_DIR}/logs/ping.txt"
touch $LOG_PING
FILENAME_LOCAL="main_live_okx_collective.py"
FILENAME_FULL="${PROJECT_DIR}/${FILENAME_LOCAL}"
PYTHONBIN="/home/sculd3/venvs/mlb/bin/python"
RUN_COMMAND="echo $LOG && touch $LOG && date >> $LOG && echo $FILENAME_LOCAL >> $LOG && $PYTHONBIN $FILENAME_FULL >> $LOG 2>&1"
while true; do
  pgrep -f $FILENAME_LOCAL || eval $RUN_COMMAND &
  date >> $LOG_PING
  echo $FILENAME_LOCAL >> $LOG_PING
  sleep 60s
done
