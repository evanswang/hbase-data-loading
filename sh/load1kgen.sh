javac -cp `hbase classpath` src/dsi/kvmodel/vcf/*.java
cd src
java -Xmx6666m -cp `hbase classpath`: dsi.kvmodel.vcf.Load1KGenome $1 $2 $3 $4 $5 $6
