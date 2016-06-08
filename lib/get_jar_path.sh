#!/bin/bash

subdir=`ls -d */`
echo "$dirs"
for directory in $subdir
do
  echo "========================"
  echo "$directory jars"
  echo "========================"
  result=""
  for x in `ls $directory`
  do
   echo $x
   result=$result:\$\(STRESSLIB\)\/$directory$x
   done
   echo ""
   echo "$result"
done
