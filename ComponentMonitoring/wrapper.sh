#!/bin/bash

#
# Copyright 2016 Athanassios Kintsakis.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Author: Athanassios Kintsakis
# contact: akintsakis@issel.ee.auth.gr athanassios.kintsakis@gmail.com
# 

screen -S "$2" -X quit 1>/dev/null
screen -d -m -S "$2"

touch "$2".commandTmp
echo $1 > "$2".commandTmp
path="$2".commandTmp
####screen -S "$2" -X stuff "bash "$2".commandTmp"`echo -ne '\015'`
#screen -S "$2" -X stuff "read -d $'\x04' commandName < "$2".commandTmp"`echo -ne '\015'`
#screen -S "$2" -X stuff 'eval \$commandName'`echo -ne '\015'`
###
screen -S "$2" -X stuff 'bash '$path''`echo -ne '\015'`
screen -S "$2" -X stuff 'exit'`echo -ne '\015'`

#screen -S "$2" -X "$1" 
#screen -R "$2" -X exec "$1"
#sleep 1


#echo "System CPUs:"$(grep -c ^processor /proc/cpuinfo)  
#echo "System Memory in KB:"$(grep MemTotal /proc/meminfo | awk '{print $2}')  
rm -f "$2".tmp 
screen -list | grep $2 | cut -f1 -d'.' | sed 's/\W//g' >> "$2".tmp
B=`cat "$2".tmp`

#echo $B
#echo "after B"
screenPid=$B
if [ $screenPid>0 ]; then
	while (ps -p $screenPid >> /dev/null) #
	do
	  #ls
	  bash /home/user/plist $B >> $3
	  #echo "stop"		  
	  sleep 0.5
	done
fi

#rm -f "$4"

#bash "$3" "$2" >> "$4"
#screen -S "$2" -X quit

rm "$2".tmp
rm "$2".commandTmp

