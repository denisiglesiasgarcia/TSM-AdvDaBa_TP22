# Use an official Python runtime as the parent image
FROM python:3.10-slim

# Set the working directory in the container to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install netcat
RUN apt-get update && apt-get install -y netcat-openbsd

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Define environment variable for Neo4j (change accordingly if necessary)
ENV NEO4J_URI=bolt://neo4j:7687
ENV NEO4J_USER=neo4j
ENV NEO4J_PASSWORD=testtest

# Import the data into Neo4j
# COPY entrypoint.sh /entrypoint.sh
# ENTRYPOINT ["/entrypoint.sh"]

# Launch the script
CMD ["python", "/app/neo4j.py"]