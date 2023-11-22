import ijson
from neo4j import GraphDatabase
import datetime
from tqdm_loggable.auto import tqdm
import logging
from tqdm_loggable.tqdm_logging import tqdm_logging
import os
from collections import deque
from itertools import islice
import gc
import requests
import time
import requests
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
tqdm_logging.set_level(logging.INFO)


# JSON
def preprocess_line(line):
    """
    Preprocesses a line by replacing 'NaN' with 'null' and removing 'NumberInt(' and ')'.

    Args:
        line (str): The line to be preprocessed.

    Returns:
        str: The preprocessed line.
    """
    return line.replace('NaN', 'null').replace('NumberInt(', '').replace(')', '')


def preprocess_json(url, buffer_size, max_retries=3, timeout=10):
    """
    Preprocesses a JSON file from a given URL with retry logic and improved error handling.

    Args:
        url (str): The URL of the JSON file.
        buffer_size (int): The size of the buffer for reading the file.
        max_retries (int): Maximum number of retries for the request.
        timeout (int): Timeout for the request in seconds.

    Yields:
        str: Preprocessed line from the JSON file.

    Raises:
        requests.HTTPError: If there is an error while retrieving the file.
    """
    attempts = 0
    while attempts < max_retries:
        try:
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                buffer = bytearray()
                for chunk in r.iter_content(chunk_size=buffer_size):
                    buffer.extend(chunk)
                    while buffer:
                        last_newline_pos = buffer.find(b'\n')
                        if last_newline_pos == -1:
                            break  # Incomplete line, wait for more data
                        line = buffer[:last_newline_pos + 1].decode()
                        buffer = buffer[last_newline_pos + 1:]
                        yield preprocess_line(line)
            break  # Successful retrieval, exit the loop
        except (requests.RequestException, requests.Timeout) as e:
            attempts += 1
            print("Attempt {}/{} failed: {}. Retrying...".format(attempts, max_retries, e))
            time.sleep(1)  # Wait for a second before retrying
    if attempts == max_retries:
        raise requests.HTTPError("Failed to retrieve data after {} attempts.".format(max_retries))

class PreprocessedFile:
    def __init__(self, url, buffer_size=4096):  # buffer_size is in bytes
        """
        Initializes a PreprocessedFile object.

        Args:
            url (str): The name of the file to preprocess.
            buffer_size (int, optional): The size of the buffer in bytes. Defaults to 4096.
        """
        self.generator = preprocess_json(url, buffer_size)
        self.buffer = deque()
        self.buffer_length = 0  # Track the length of strings in the buffer

    def append_to_buffer(self, line):
        """
        Appends a line to the buffer.

        Args:
            line (str): The line to append to the buffer.
        """
        if line is not None:
            self.buffer.append(line)
            self.buffer_length += len(line)

    def pop_from_buffer(self):
        """
        Pops a line from the buffer.

        Returns:
            str: The popped line from the buffer.
        """
        line = self.buffer.popleft()
        self.buffer_length -= len(line)
        return line

    def read(self, size=-1):
        """
        Reads and returns the specified number of characters from the file.

        Args:
            size (int, optional): The number of characters to read. Defaults to -1, which means read all.

        Returns:
            str: The read characters from the file.
        """
        while size < 0 or self.buffer_length < size:
            try:
                self.append_to_buffer(next(self.generator))
            except StopIteration:
                break  # End of generator, return what's left in the buffer
        if size < 0:
            result = ''.join(self.buffer)
            self.buffer.clear()
            self.buffer_length = 0
        else:
            result_list = []
            remaining_size = size
            while self.buffer and remaining_size > 0:
                line = self.pop_from_buffer()
                result_list.append(line[:remaining_size])
                remaining_size -= len(line)
                if remaining_size < 0:
                    # If the last line was too large, put the remaining part back into the buffer
                    leftover = line[remaining_size:]
                    self.buffer.appendleft(leftover)
                    self.buffer_length += len(leftover)
            result = ''.join(result_list)
        return result



def get_cleaned_data(url, buffer_size):
    """
    Retrieves cleaned data from a preprocessed file.

    Args:
        url (str): The path to the preprocessed file.

    Returns:
        iterator: An iterator over the cleaned data items.
    """
    preprocessed_file = PreprocessedFile(url, buffer_size)
    return ijson.items(preprocessed_file, 'item')

def parse_ijson_object(cleaned_data, batch_size):
    def chunked_iterable(iterable, size):
        it = iter(iterable)
        while True:
            try:
                chunk = list(islice(it, size))
                if not chunk:
                    break
                yield chunk
            except ijson.common.IncompleteJSONError as e:
                logging.error(f"Error parsing JSON: {e}")
                continue

    def trim_article(article):
        trimmed_article = {key: article[key] for key in ['_id', 'title', 'references'] if key in article}
        authors = article.get('authors')
        
        if authors and isinstance(authors, list):
            trimmed_authors = [{'_id': author['_id'], 'name': author['name']} for author in authors if '_id' in author and 'name' in author]
            if trimmed_authors:  # Check if the list is not empty
                trimmed_article['authors'] = trimmed_authors
                    
        return trimmed_article

    def process_articles_chunk(articles_chunk):
        articles_chunk = list(map(trim_article, articles_chunk))
        authors_batch_chunk = []
        references_batch_chunk = []
        for article in articles_chunk:
            article_id = article.get('_id')
            article_title = article.get('title')
            authors = article.get('authors', [])
            if authors and article_id and article_title and article_id != 'null':
                for author in authors:
                    author_id = author.get('_id')
                    author_name = author.get('name')
                    if author_id and author_name and article_id and article_title \
                        and author_id != 'null' and author_name != 'null' \
                        and article_id != 'null' and article_title != 'null':
                        entry = {
                            'article': {
                                'article_id': article_id,
                                'article_title': article_title
                            },
                            'authors': [{
                                '_id': author_id,
                                'name': author_name
                            }]
                        }
                        authors_batch_chunk.append(entry)

            # Only process the article for references if there are references
            references = article.get('references', [])
            if references and article_id and article_title and article_id != 'null':
                references_data = {
                    'article_id': article_id,
                    'article_title': article_title,
                    'references': references
                }
                references_batch_chunk.append(references_data)

        return authors_batch_chunk, references_batch_chunk

    for articles_chunk in chunked_iterable(cleaned_data, batch_size):
        articles_authors_batch, articles_references_batch = process_articles_chunk(articles_chunk)

        if len(articles_authors_batch) >= batch_size:
            yield articles_authors_batch, articles_references_batch
            articles_authors_batch = []
            articles_references_batch = []

    if articles_authors_batch or articles_references_batch:
        yield articles_authors_batch, articles_references_batch

# Neo4j
def neo4j_startup(uri, username, password):
    """
    Connects to a Neo4j database using the provided URI, username, and password.
    Drops all constraints and indexes in the database for debugging purposes.
    Creates new indexes and constraints for optimized Neo4j performance.
    Closes the database connection at the end.
    
    Parameters:
    - uri (str): The URI of the Neo4j database.
    - username (str): The username for authentication.
    - password (str): The password for authentication.
    """
    def drop_all_constraints_and_indexes(tx):
        """ 
        Debugging function to drop all indexes and constraints in the database 
        """
        # Get the list of all constraints
        result = tx.run("SHOW CONSTRAINTS")
        for record in result:
            # Get the constraint name
            constraint_name = record['name']
            # Drop the constraint
            tx.run(f"DROP CONSTRAINT {constraint_name} IF EXISTS")

        # Now that constraints are dropped, get the list of all indexes
        result = tx.run("SHOW INDEXES")
        for record in result:
            # Get the index name
            index_name = record['name']
            # Drop the index
            tx.run(f"DROP INDEX {index_name} IF EXISTS")

    def neo4j_index_constraints(session):
        with session.begin_transaction() as tx:
            # index
            tx.run("CREATE INDEX article_title_index FOR (n:Article) ON (n.title)")
            tx.run("CREATE INDEX author_name_index FOR (a:Author) ON (a.name)")
            # unique
            tx.run("CREATE CONSTRAINT article_id_uniqueness FOR (a:Article) REQUIRE (a._id) IS UNIQUE")
            tx.run("CREATE CONSTRAINT author_id_uniqueness FOR (a:Author) REQUIRE (a._id) IS UNIQUE")
            # Commit the transaction at the end of the batch
            tx.commit()
    
    # Connect to Neo4j
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    # Start a session and process the data in batches
    with driver.session() as session:
        # debug drop all indexes
        #session.run("MATCH (n) DETACH DELETE n")
        session.execute_write(drop_all_constraints_and_indexes)
        # optimize Neo4j
        neo4j_index_constraints(session)
    
    # Close the driver
    driver.close()

def send_data_to_neo4j(uri, username, password, author_lists, references_lists, batch_size_apoc):
    # Function to send a single batch to the database
    def send_batch_author(tx, authors_batch, batch_size_apoc):
        query = """
        CALL apoc.periodic.iterate(
            'UNWIND $authors_batch AS row RETURN row',
            'WITH row.article AS article, row.authors AS authors
            UNWIND authors AS authorData
            WITH article, authorData
            WHERE article.article_id IS NOT NULL AND article.article_title IS NOT NULL
            AND authorData._id IS NOT NULL AND authorData.name IS NOT NULL
            CALL apoc.merge.node(["Article"], {_id: article.article_id}, {title: article.article_title}) YIELD node AS a
            CALL apoc.merge.node(["Author"], {_id: authorData._id}, {name: authorData.name}) YIELD node AS author
            CALL apoc.create.relationship(author, "AUTHORED", {}, a) YIELD rel
            RETURN rel',
            {batchSize: $batch_size_apoc, parallel:false, params:{authors_batch: $authors_batch}})
        """
        tx.run(query, authors_batch=authors_batch, batch_size_apoc=batch_size_apoc)
    
    def send_batch_ref(tx, refs_batch, batch_size_apoc):
        query = """
        CALL apoc.periodic.iterate(
            'UNWIND $refs_batch AS row RETURN row',
            'WITH row.article AS article, row.refs AS refs
            UNWIND refs AS refData
            WITH article, refData
            WHERE article.article_id IS NOT NULL AND article.article_title IS NOT NULL
            AND refData._id IS NOT NULL AND refData.title IS NOT NULL
            CALL apoc.merge.node(["Article"], {_id: article.article_id}, {title: article.article_title}) YIELD node AS a
            CALL apoc.merge.node(["Article"], {_id: refData._id}, {title: refData.title}) YIELD node AS ref
            CALL apoc.create.relationship(a, "REFERENCES", {}, ref) YIELD rel
            RETURN rel',
            {batchSize: $batch_size_apoc, parallel:false, params:{refs_batch: $refs_batch}})
        """
        tx.run(query, refs_batch=refs_batch, batch_size_apoc=batch_size_apoc)

    # Connect to Neo4j
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    # Start a session and process the data in batches
    with driver.session() as session:
        if author_lists:
            session.execute_write(send_batch_author, author_lists, batch_size_apoc)
        if references_lists:
            session.execute_write(send_batch_ref, references_lists, batch_size_apoc)

    # Close the driver
    driver.close()

def main(neo4j_uri, neo4j_user, neo4j_password, url, buffer_size, BATCH_SIZE, TOTAL_ARTICLES, batch_size_apoc):
    # Neo4j cleanup and optimization
    neo4j_startup(neo4j_uri, neo4j_user, neo4j_password)

    # Parse JSON file and get a generator of cleaned data
    cleaned_data_generator = get_cleaned_data(url, buffer_size)
    
    # Create the generator
    article_batches_generator = parse_ijson_object(cleaned_data_generator, BATCH_SIZE)

    # Initialize tqdm with the total count of articles
    t = tqdm(total=TOTAL_ARTICLES, unit=' article')

    # Loop through all the batches from the generator with a progress bar
    for articles_authors_batch, articles_references_batch in article_batches_generator:
        # Update the tqdm progress bar with the number of articles processed in this batch
        t.update(len(articles_authors_batch)+len(articles_references_batch))
        # Process the current batch of articles
        send_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, articles_authors_batch, articles_references_batch, batch_size_apoc)
        gc.collect()

    # Optional: Close the tqdm progress bar once processing is complete
    t.close()

# Usage
url = os.environ['JSON_FILE']
neo4j_uri = os.environ['NEO4J_URI']
neo4j_user = os.environ['NEO4J_USER']
neo4j_password = os.environ['NEO4J_PASSWORD']
BATCH_SIZE = int(os.environ['BATCH_SIZE_ARTICLES'])
buffer_size = int(os.environ['BUFFER_SIZE_PARSER'])
batch_size_apoc = int(os.environ['BATCH_SIZE_APOC'])
TOTAL_ARTICLES = 17_100_000

# start
start_time = datetime.datetime.now()
print(f"Processing started at {start_time}")

# process articles
main(neo4j_uri, neo4j_user, neo4j_password, url, buffer_size, BATCH_SIZE, TOTAL_ARTICLES, batch_size_apoc)

# end
end_time = datetime.datetime.now()
elapsed_time = end_time - start_time
print(f"Processing finished at {end_time}. Total time taken: {elapsed_time}")