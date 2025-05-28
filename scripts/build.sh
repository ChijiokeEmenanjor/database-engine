#!/bin/bash

echo " Building Database Engine (Eclipse Project)..."

mkdir -p bin

echo " Compiling sources..."
find src -name "*.java" -type f > sources.txt

if [ -s sources.txt ]; then
    javac -d bin @sources.txt
    
    if [ $? -eq 0 ]; then
        echo " Build successful!"
        echo " Classes compiled to bin/"
        echo ""
        echo " To run example: java -cp bin company.db.Company"
    else
        echo " Build failed!"
        exit 1
    fi
else
    echo "⚠️  No source files found!"
    exit 1
fi

rm sources.txt
