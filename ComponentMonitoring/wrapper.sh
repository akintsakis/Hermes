#!/bin/bash

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

