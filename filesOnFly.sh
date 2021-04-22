!/bin/bash
FILES=../../ilesForWordcount/*
for f in $FILES
do
       	kafka-console-producer --broker-list localhost:9092 --topic wordCount-topic < "$f"
        rm "$f"
done

