#!/bin/bash
FILES=../../filesForWordcount/*
for f in $FILES
do
		if test -f "$f"; then
			bin/kafka-console-producer.sh --broker-list localhost:9092 --topic wordCount-topic < "$f"
			rm "$f"
		fi

done

