javac -cp /data/hbase-0.96.0-hadoop1/bin/../conf:/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.33.x86_64/lib/tools.jar:/data/hbase-0.96.0-hadoop1/bin/..:/data/hbase-0.96.0-hadoop1/bin/../lib/activation-1.1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/asm-3.1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-beanutils-1.7.0.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-beanutils-core-1.8.0.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-cli-1.2.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-codec-1.7.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-collections-3.2.1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-configuration-1.6.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-digester-1.8.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-el-1.0.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-httpclient-3.1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-io-2.4.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-lang-2.6.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-logging-1.1.1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-math-2.2.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/commons-net-1.4.1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/core-3.1.1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/findbugs-annotations-1.3.9-1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/guava-12.0.1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hadoop-core-1.1.2.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-client-0.96.0-hadoop1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-common-0.96.0-hadoop1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-common-0.96.0-hadoop1-tests.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-examples-0.96.0-hadoop1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-hadoop1-compat-0.96.0-hadoop1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-hadoop-compat-0.96.0-hadoop1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-it-0.96.0-hadoop1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-it-0.96.0-hadoop1-tests.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-prefix-tree-0.96.0-hadoop1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-protocol-0.96.0-hadoop1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-server-0.96.0-hadoop1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-server-0.96.0-hadoop1-tests.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-shell-0.96.0-hadoop1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-testing-util-0.96.0-hadoop1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/hbase-thrift-0.96.0-hadoop1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/high-scale-lib-1.1.1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/htrace-core-2.01.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/httpclient-4.1.3.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/httpcore-4.1.3.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jackson-core-asl-1.8.8.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jackson-jaxrs-1.8.8.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jackson-mapper-asl-1.8.8.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jackson-xc-1.8.8.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jamon-runtime-2.3.1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jasper-compiler-5.5.23.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jasper-runtime-5.5.23.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jaxb-api-2.2.2.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jaxb-impl-2.2.3-1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jersey-core-1.8.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jersey-json-1.8.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jersey-server-1.8.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jettison-1.3.1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jetty-6.1.26.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jetty-sslengine-6.1.26.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jetty-util-6.1.26.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jruby-complete-1.6.8.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jsp-2.1-6.1.14.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jsp-api-2.1-6.1.14.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/jsr305-1.3.9.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/junit-4.11.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/libthrift-0.9.0.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/log4j-1.2.17.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/metrics-core-2.1.2.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/netty-3.6.6.Final.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/protobuf-java-2.5.0.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/servlet-api-2.5-6.1.14.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/slf4j-api-1.6.4.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/slf4j-log4j12-1.6.4.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/stax-api-1.0.1.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/xmlenc-0.52.jar:/data/hbase-0.96.0-hadoop1/bin/../lib/zookeeper-3.4.5.jar:/data/hadoop-1.0.3/libexec/../conf:/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.33.x86_64/lib/tools.jar:/data/hadoop-1.0.3/libexec/..:/data/hadoop-1.0.3/libexec/../hadoop-core-1.0.3.jar:/data/hadoop-1.0.3/libexec/../lib/asm-3.2.jar:/data/hadoop-1.0.3/libexec/../lib/aspectjrt-1.6.5.jar:/data/hadoop-1.0.3/libexec/../lib/aspectjtools-1.6.5.jar:/data/hadoop-1.0.3/libexec/../lib/commons-beanutils-1.7.0.jar:/data/hadoop-1.0.3/libexec/../lib/commons-beanutils-core-1.8.0.jar:/data/hadoop-1.0.3/libexec/../lib/commons-cli-1.2.jar:/data/hadoop-1.0.3/libexec/../lib/commons-codec-1.4.jar:/data/hadoop-1.0.3/libexec/../lib/commons-collections-3.2.1.jar:/data/hadoop-1.0.3/libexec/../lib/commons-configuration-1.6.jar:/data/hadoop-1.0.3/libexec/../lib/commons-daemon-1.0.1.jar:/data/hadoop-1.0.3/libexec/../lib/commons-digester-1.8.jar:/data/hadoop-1.0.3/libexec/../lib/commons-el-1.0.jar:/data/hadoop-1.0.3/libexec/../lib/commons-httpclient-3.0.1.jar:/data/hadoop-1.0.3/libexec/../lib/commons-io-2.1.jar:/data/hadoop-1.0.3/libexec/../lib/commons-lang-2.4.jar:/data/hadoop-1.0.3/libexec/../lib/commons-logging-1.1.1.jar:/data/hadoop-1.0.3/libexec/../lib/commons-logging-api-1.0.4.jar:/data/hadoop-1.0.3/libexec/../lib/commons-math-2.1.jar:/data/hadoop-1.0.3/libexec/../lib/commons-net-1.4.1.jar:/data/hadoop-1.0.3/libexec/../lib/core-3.1.1.jar:/data/hadoop-1.0.3/libexec/../lib/hadoop-capacity-scheduler-1.0.3.jar:/data/hadoop-1.0.3/libexec/../lib/hadoop-fairscheduler-1.0.3.jar:/data/hadoop-1.0.3/libexec/../lib/hadoop-thriftfs-1.0.3.jar:/data/hadoop-1.0.3/libexec/../lib/hsqldb-1.8.0.10.jar:/data/hadoop-1.0.3/libexec/../lib/jackson-core-asl-1.8.8.jar:/data/hadoop-1.0.3/libexec/../lib/jackson-mapper-asl-1.8.8.jar:/data/hadoop-1.0.3/libexec/../lib/jasper-compiler-5.5.12.jar:/data/hadoop-1.0.3/libexec/../lib/jasper-runtime-5.5.12.jar:/data/hadoop-1.0.3/libexec/../lib/jdeb-0.8.jar:/data/hadoop-1.0.3/libexec/../lib/jersey-core-1.8.jar:/data/hadoop-1.0.3/libexec/../lib/jersey-json-1.8.jar:/data/hadoop-1.0.3/libexec/../lib/jersey-server-1.8.jar:/data/hadoop-1.0.3/libexec/../lib/jets3t-0.6.1.jar:/data/hadoop-1.0.3/libexec/../lib/jetty-6.1.26.jar:/data/hadoop-1.0.3/libexec/../lib/jetty-util-6.1.26.jar:/data/hadoop-1.0.3/libexec/../lib/jsch-0.1.42.jar:/data/hadoop-1.0.3/libexec/../lib/junit-4.5.jar:/data/hadoop-1.0.3/libexec/../lib/kfs-0.2.2.jar:/data/hadoop-1.0.3/libexec/../lib/log4j-1.2.15.jar:/data/hadoop-1.0.3/libexec/../lib/mockito-all-1.8.5.jar:/data/hadoop-1.0.3/libexec/../lib/oro-2.0.8.jar:/data/hadoop-1.0.3/libexec/../lib/servlet-api-2.5-20081211.jar:/data/hadoop-1.0.3/libexec/../lib/slf4j-api-1.4.3.jar:/data/hadoop-1.0.3/libexec/../lib/slf4j-log4j12-1.4.3.jar:/data/hadoop-1.0.3/libexec/../lib/xmlenc-0.52.jar:/data/hadoop-1.0.3/libexec/../lib/jsp-2.1/jsp-2.1.jar:/data/hadoop-1.0.3/libexec/../lib/jsp-2.1/jsp-api-2.1.jar HBaseTM.java

