#!/bin/bash

containers=$(sudo docker ps -q | tail -n +1)

for item in ${containers//\\n/}
do
  env=$(sudo docker inspect -f '{{range $index, $value := .Config.Env}}{{$value}} {{end}}' $item);
  if [[ $env == *"PYAMQP_INTEGRATION_INSTANCE=1"* ]]; then
    grep -m1 'Server startup complete' <(sudo docker logs -f $item)
    sudo docker logs $item
  fi
done;
