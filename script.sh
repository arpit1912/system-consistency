#!/bin/bash

# Basic while loop
counter=1

ports=()
replica_ports=""
port=8081
while [ $counter -le $1 ]; do
    ports[$counter]=$port 
    replica_ports="$replica_ports $port"
    ((port++))
    ((counter++))
done
echo $replica_ports

PRIMARY_PORT=8080
gnome-terminal -e "go run primary.go $PRIMARY_PORT $1 $replica_ports" &

for i in "${ports[@]}"; do
    other_ports="$PRIMARY_PORT"
    for j in "${ports[@]}"; do
        if [ "$j" -ne "$i" ]; then
            other_ports="$other_ports $j"
        fi
    done  
    gnome-terminal -e "go run replica.go $i $PRIMARY_PORT $other_ports" &
done

sleep 3
prob=$3
all_ports="$PRIMARY_PORT $replica_ports"
counter=1
Client_port=8090
while [ $counter -le $2 ]; do
    gnome-terminal -e "go run client.go $Client_port $prob $all_ports" &
    ((Client_port++))
    ((counter++))
done

# script.sh 2 <number of cleints> <prob>