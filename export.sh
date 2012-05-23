export HADOOP_HOME=/home/nourl/github/hadoop-common-1.0/build/hadoop-1.0.4-SNAPSHOT
export JAVA_HOME=/usr/lib/jvm/java-6-sun

cp ./myconf/* ./build/hadoop-1.0.4-SNAPSHOT/conf/

cd ./build/hadoop-1.0.4-SNAPSHOT/

./bin/hadoop namenode -format

./bin/hadoop namenode 

./bin/hadoop datanode

./bin/hadoop jobtracker

./bin/hadoop tasktracker
