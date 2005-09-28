#!/bin/sh

rm -f RefTest.root
rm -f PoolFileCatalog.xml

ln -s ../../OutputService/test/PoolFileCatalog.xml PoolFileCatalog.xml
ln -s ../../OutputService/test/RefTest.root RefTest.root

cmsRun --parameter-set RefReadTest.cfg
