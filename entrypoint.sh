#!/bin/bash

echo "Starting entrypoint.sh..."

# Wait for neo4j to start
NEO4J_HOST="neo4j"
NEO4J_PORT="7687"

echo "Waiting for Neo4j service at $NEO4J_HOST:$NEO4J_PORT to start..."

until $(nc -zv $NEO4J_HOST $NEO4J_PORT); do
    echo "Neo4j not yet available, retrying..."
    sleep 5
done

echo "Neo4j service is available, proceeding with data import check..."

# Check if the marker file exists → if not it means that the data has not been imported yet
if [ ! -f /tmp/data_imported ]; then
    echo "Marker file not found, starting data import..."
    
    # Run your Python script and log the output to a file
    python main.py

    # If the Python script ran successfully, create the marker file
    if [ $? -eq 0 ]; then
        echo "Python script ran successfully, creating marker file..."
        touch /tmp/data_imported
    else
        echo "Python script failed, not creating marker file."
        exit 1
    fi
else
    echo "Marker file found, data import is not required."
fi

# Here you can put the command to keep the container alive, if needed.
echo "Keeping the container alive..."
tail -f /dev/null
