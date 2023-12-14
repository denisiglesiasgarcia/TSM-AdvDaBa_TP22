# Use an official Python runtime as the parent image
FROM python:3.12.1-slim

# Set the working directory in the container to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install netcat
RUN apt-get update && apt-get install -y netcat-openbsd libyajl2

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Import the data into Neo4j
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]