#!/bin/sh

rm -f ./*.txt ./*.html ./*.png && \
mvn exec:java -Dexec.mainClass="com.dsp.ass1.Worker"
