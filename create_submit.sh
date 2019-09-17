#!/bin/bash

set -v
set -e

cd "$(dirname "$0")"

rm -f submit.zip
rm -rf submit
mkdir submit
cd submit


# Copy source code
cp -a ../src sourcecode
rm -f sourcecode/*.o sourcecode/*.out

# Copy README
cp -a ../README.md readme.txt

# Generate run.sh
echo '#!/bin/bash -e' >run.sh
echo 'cd "$(dirname "$0")"' >>run.sh
echo 'exec ./BDCI19.out "$@"' >>run.sh
chmod +x run.sh

# Generate compile.sh
echo '#!/bin/bash -e' >compile.sh
echo 'cd "$(dirname "$0")"' >>compile.sh
echo 'make fastest -j -C sourcecode' >>compile.sh
chmod +x compile.sh

# Compile and copy binary file
./compile.sh
mv sourcecode/BDCI19.out .


# Create zip file
tar -cf ../submit.zip *

cd ..
rm -rf submit
