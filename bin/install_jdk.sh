# test jdk install and install jdk if not installed
if [ -n $((java -version) 1>&2 )"" ]; then
        if [ -n $((sudo apt-get update) 1>&2 )"" ]; then
                echo "***FINAL DECISION***: cannot install openjdk-7, please install it manually and re-run this script"
		exit
	fi
	if [ -n $((sudo apt-get -y install openjdk-7-jdk) 1>&2 )"" ]; then
                echo "***FINAL DECISION***: cannot install openjdk-7, please install it manually and re-run this script"
		exit
        fi
fi
