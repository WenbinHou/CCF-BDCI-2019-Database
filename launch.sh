#!/bin/bash

set -e

cd "$(dirname "$0")"

time ./BDCI19.out \
    data/customer.txt data/orders.txt data/lineitem.txt \
    7 \
    BUILDING 1995-03-29 1995-03-27 5 \
    BUILDING 1995-02-29 1995-04-27 10 \
    BUILDING 1995-03-28 1995-04-27 2 \
    `# Manually added by Wenbin Hou` \
    AUTOMOBILE 1989-12-31 2000-01-01 10 \
    FURNITURE 1989-12-31 2000-01-01 10 \
    HOUSEHOLD 1989-12-31 2000-01-01 10 \
    MACHINERY 1989-12-31 2000-01-01 10 \
    | tee out.txt

diff out.txt data/results.txt

