rm -f src/dsi/kvmodel/microarray/*.class
javac -cp `hbase classpath`:lib/mongo-java-driver-3.2.2.jar src/dsi/kvmodel/microarray/*.java
cd src
java -Xmx6666m -cp `hbase classpath`: dsi.kvmodel.microarray.HBaseSQL $1 $2 $3 $4 $5 $6 $7 $8 $9
