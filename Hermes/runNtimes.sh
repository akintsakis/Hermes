for i in $(seq 1 $1)
do
echo $i
java -jar ./dist/Hermes.jar &> $i"manyrunslog"
sleep 30
done
