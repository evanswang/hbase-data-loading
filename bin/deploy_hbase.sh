# deploy HBase 0.98.0 hadoop2 and start a standalone instance
DIR=`pwd`
mkdir $DIR/INSTALL_HBase
cd $DIR/INSTALL_HBase
DIR=`pwd`
VALID=$((wget -P ${DIR} http://archive.apache.org/dist/hbase/hbase-0.98.0/hbase-0.98.0-hadoop2-bin.tar.gz) 1>&2 )
if [ -n "$VALID" ]; then
	echo $VALID
	return
fi
tar xzf hbase-0.98.0-hadoop2-bin.tar.gz
cd hbase-0.98.0-hadoop2/conf
sed 's/\# export JAVA_HOME=\/usr\/java\/jdk1.6.0\//export JAVA_HOME=\/usr\/lib\/jvm\/java-7-openjdk-amd64\//g' hbase-env.sh > tmp
mv tmp hbase-env.sh
HBASE_HOME=$DIR/hbase-0.98.0-hadoop2
echo "HBASE_HOME=$DIR/hbase-0.98.0-hadoop2" >> $DIR/env.sh
export HBASE_HOME=$HBASE_HOME
echo "export HBASE_HOME=$HBASE_HOME" >> $DIR/env.sh
export PATH=$PATH:$HBASE_HOME/bin
echo "export PATH=$PATH:$HBASE_HOME/bin" >> $DIR/env.sh
start-hbase.sh
