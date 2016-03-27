#!/bin/sh

INPUT_DIR="../parser"

PROG_PATH="./dist/Compiler.jar"

ls ${INPUT_DIR} |
while read INPUT_FILE
do 
    ERROR=`java -jar ${PROG_PATH} -target parser -debug ${INPUT_DIR}/${INPUT_FILE} 2>&1`
    if [ "$ERROR" = "" ] && echo $INPUT_FILE | grep "^legal-\d\{2\}$" > /dev/null 
    then
        echo "${INPUT_FILE} is correct."
    elif ! [ "$ERROR" = "" ] && echo $INPUT_FILE | grep "^illegal-\d\{2\}$" > /dev/null
    then
        echo "${INPUT_FILE} is correct."
    else
        echo "\033[31m${INPUT_FILE} is wrong.\033[0m"
    fi
done 
