#!/bin/bash

# Wait for neo4j to start
NEO4J_HOST="neo4j"
NEO4J_PORT="7687"

until $(nc -zv $NEO4J_HOST $NEO4J_PORT); do
    echo "Waiting for neo4j service at $NEO4J_HOST:$NEO4J_PORT to start..."
    sleep 5
done

# Check if the marker file exists → if not it means that the data has not been imported yet
if [ ! -f /tmp/data_imported ]; then
    # Run your Python script
    python neo4j.py

    # If the Python script ran successfully, create the marker file
    if [ $? -eq 0 ]; then
        touch /tmp/data_imported
    else
        echo "Python script failed, not creating marker file."
        exit 1
    fi
fi

# Here you can put the command to keep the container alive, if needed.
# For example:
tail -f /dev/null
