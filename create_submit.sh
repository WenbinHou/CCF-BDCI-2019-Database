#!/bin/bash

set -v
set -e

cd "$(dirname "$0")"

rm -f submit.zip
rm -rf submit
mkdir submit
cd submit


# Copy source code
cp -a ../src ./src
cp -a ../CMakeLists.txt ./
rm -f src/*.o src/*.out

# Copy README
cp -a ../README.md readme.txt

# Generate run.sh
echo '#!/bin/bash -e' >run.sh
echo 'cd "$(dirname "$0")"' >>run.sh
echo 'exec ./BDCI19 "$@"' >>run.sh
chmod +x run.sh

# Generate compile.sh
echo '#!/bin/bash -ve' >compile.sh
echo 'cd "$(dirname "$0")"' >>compile.sh
echo 'rm -rf build; mkdir build' >>compile.sh
echo 'cd build' >>compile.sh
echo 'cmake .. -DCMAKE_BUILD_TYPE=Release -DMAKE_FASTEST=ON' >>compile.sh
echo 'make -j' >>compile.sh
echo 'mv BDCI19 ../' >>compile.sh
echo 'cd ..; rm -rf build' >>compile.sh
chmod +x compile.sh

# Compile and copy binary file
./compile.sh


# Create zip file
tar -cf ../submit.tar.gz *

cd ..
rm -rf submit
