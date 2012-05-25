cd /home/nourl/github/hadoop-common-1.0/build/hadoop-1.0.4-SNAPSHOT/logs/
find ./* -size 0 -delete
cat ./*.log|grep -i act-hadoop
