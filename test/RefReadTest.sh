#!/bin/sh

rm -f *.root *.xml *.xml.BAK

cp ../../Output/test/*.root .
cp ../../Output/test/*.xml .
cp *.root a.root


cmsRun --parameter-set RefReadTest.cfg
