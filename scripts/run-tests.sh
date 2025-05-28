#!/bin/bash

echo "🧪 Starting test script..."

if [ ! -d "bin" ]; then
    echo " Please run ./scripts/build.sh first"
    exit 1
fi

JUNIT_JAR="junit-platform-console-standalone-1.8.2.jar"
if [ ! -f "$JUNIT_JAR" ]; then
    echo " JUnit not found! Download with:"
    echo "   wget https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/1.8.2/junit-platform-console-standalone-1.8.2.jar"
    exit 1
fi

echo " Found JUnit: $JUNIT_JAR"
echo "🧪 Running Database Engine Tests..."
echo "==================================="

java -cp "bin:$JUNIT_JAR" org.junit.platform.console.ConsoleLauncher --classpath bin --select-class company.db.UnitTests

echo " Tests completed!"
