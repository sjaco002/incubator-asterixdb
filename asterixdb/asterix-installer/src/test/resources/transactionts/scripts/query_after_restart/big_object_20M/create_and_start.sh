jps | awk '{if ($2 == "NCDriver" || $2 == "CCDriver") print $1;}' | xargs -n 1 kill -9;
$MANAGIX_HOME/bin/managix stop -n asterix 1>/dev/null 2>&1;
$MANAGIX_HOME/bin/managix delete -n asterix 1>/dev/null 2>&1;
$MANAGIX_HOME/bin/managix create -n asterix -c $MANAGIX_HOME/clusters/local/local.xml;