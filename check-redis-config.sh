#!/usr/bin/sh

REDISCMD="redis-bin/src/redis-cli"
MYHOSTS=myhosts
PORT=$(awk 'NR==1 {print $2}' $MYHOSTS)

if [ "$#" -eq 0 ]; then
  echo "please provide a config string"
  exit 1
fi

AUTHOPT=""
if [ "$#" -ge 2 ]; then
  AUTHOPT="-a $2"
fi

awk '{print $1}' $MYHOSTS | xargs -I{} $REDISCMD --no-auth-warning $AUTHOPT -p $PORT -h {} config get $1