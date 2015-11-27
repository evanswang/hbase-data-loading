. INSTALL_HBase/env.sh
javac -cp `hbase classpath` src/dsi/kvmodel/vcf/*.java
cd src
java -Xmx6666m -cp `hbase classpath`:../lib/postgresql-9.1-903.jdbc4.jar dsi.kvmodel.vcf.ETLCellLine vcf-subject