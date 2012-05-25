export HADOOP_HOME=/home/nourl/github/hadoop-common-1.0/build/hadoop-1.0.4-SNAPSHOT
export JAVA_HOME=/usr/lib/jvm/java-6-sun

current=`pwd`

cp ./myconf/* ./build/hadoop-1.0.4-SNAPSHOT/conf/

cd ./build/hadoop-1.0.4-SNAPSHOT/

rm /home/hadoop/hdfs/name -r
rm /home/hadoop/hdfs/data -r

./bin/hadoop namenode -format

./bin/start-all.sh

./bin/hadoop jar hadoop-examples-1.0.4-SNAPSHOT.jar pi 1 1

cd $current

echo "./bin/hadoop jar hadoop-examples-1.0.4-SNAPSHOT.jar wordcount /input/in /output/1"
#./bin/hadoop namenode 
#./bin/hadoop datanode
#./bin/hadoop jobtracker
#./bin/hadoop tasktracker
