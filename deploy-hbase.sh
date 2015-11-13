if [ -n $((sh/install_jdk.sh) 1>&2)"" ]; then
	echo "***FINAL DECISION***: please install jdk-7 manually and run this script again"
	exit
fi

if [ -n $((sh/deploy_hbase.sh) 1>&2)"" ]; then
	echo "***FINAL DECISION***: please install hbase 0.98.0 hadoop2 manually and run this script again"
	exit
fi
