#!/bin/sh

echo "Starting entrypoint.sh..."

# Use environment variables for Neo4j host and port, with defaults
NEO4J_HOST="${NEO4J_HOST:-neo4j}"
NEO4J_PORT="${NEO4J_PORT:-7687}"

echo "Waiting for Neo4j service at $NEO4J_HOST:$NEO4J_PORT to start..."

# Wait for Neo4j to start
until $(nc -zv $NEO4J_HOST $NEO4J_PORT); do
    echo "Neo4j not yet available, retrying..."
    sleep 5
done

echo "Neo4j service is available, starting data import..."

# Run your Python script
python main.py

# Here you can put the command to keep the container alive, if needed.
echo "Keeping the container alive..."
sleep infinity
