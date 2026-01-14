#!/usr/bin/env bash

if [[ -z $1 ]];
then
    echo "No parameters passed. One parameter needed: name of file for OSM region"
    exit 1
fi

echo "Preprocessing OSM file: $1"
echo "Step 1/3: Extracting $1 data"
osrm-extract --profile /profiles/car.lua /data/$1.osm.pbf

echo "Step 2/3: Extracting $1 data"
osrm-partition /data/$1

echo "Step 2/3: Customizing $1 data"
osrm-customize /data/$1