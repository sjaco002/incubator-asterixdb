#!/bin/bash
#/*
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

export JAVA_HOME=/usr/local/java/vms/java

LOGSDIR=/home/onose/hyracks-asterix/logs
HYRACKS_HOME=/home/onose/src/hyracks

IPADDR=`/sbin/ifconfig eth0 | grep "inet addr" | awk '{print $2}' | cut -f 2 -d ':'`
NODEID=`ypcat hosts | grep asterix | grep -w $IPADDR | awk '{print $2}'`

export JAVA_OPTS="-Xmx10g -Djava.net.preferIPv4Stack=true -Djava.io.tmpdir=/mnt/data/sdd/space/onose/tmp"

echo $HYRACKS_HOME/hyracks-server/target/hyracks-server-0.1.3.1-binary-assembly/bin/hyracksnc -cluster-address 10.1.0.1 -cluster-port 2222 -data-listen-address $IPADDR -node-id $NODEID
$HYRACKS_HOME/hyracks-server/target/hyracks-server-0.1.3.1-binary-assembly/bin/hyracksnc -cluster-address 10.1.0.1 -cluster-port 2222 -data-listen-address $IPADDR -node-id $NODEID &> $LOGSDIR/$NODEID.log &
