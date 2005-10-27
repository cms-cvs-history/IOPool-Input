#!/bin/sh

rm -f *.root
rm -f PoolFileCatalog.*

ln -s ../../OutputService/test/RefTest.root a.root
cp a.root b.root

cmsRun --parameter-set RefReadTest.cfg
