#!/bin/sh

PREFIX=""
DEBUG="false"

while [ $# -ne 0 ]
do 
    case $1 in
    -p ) 
        shift
        PREFIX="$1" 
        ;;
    -d )
        DEBUG="true"
        ;;
    esac
    shift
done

INPUT_DIR="../scanner"

OUTPUT_DIR="../scanner/output"

PROG_PATH="./dist/Compiler.jar"

ls ${INPUT_DIR} | grep "${PREFIX}.*" |
while read INPUT_FILE
do 
    if [ -f "${INPUT_DIR}/${INPUT_FILE}" ]
    then
        java -jar ${PROG_PATH} -target scan -debug ${INPUT_DIR}/${INPUT_FILE} |
        sed "s=${INPUT_DIR}/${INPUT_FILE}=${INPUT_FILE}=g" > $$ 
        DIFF="diff -u $$ ${OUTPUT_DIR}/${INPUT_FILE}.out"
        if `$DIFF > /dev/null`
        then
            echo "${INPUT_FILE} is correct."
        else
            echo "\033[31m${INPUT_FILE} is wrong.\033[0m"
        fi
        if [ $DEBUG = "true" ]
        then
            $DIFF 
        fi
        rm $$
    fi
done 
