#!/bin/bash

s3cmd ls --recursive s3://fatty.zillabyte.com/data/homepages/2014/0620/ | awk '{print $4}' > manifest
