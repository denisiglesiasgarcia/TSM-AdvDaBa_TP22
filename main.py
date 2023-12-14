import ijson.backends.yajl2_c as ijson
from neo4j import GraphDatabase
import datetime
from tqdm_loggable.auto import tqdm
import logging
from tqdm_loggable.tqdm_logging import tqdm_logging
from collections import deque
from itertools import islice
import time
import httpx
import os
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
tqdm_logging.set_level(logging.INFO)

# JSON
def preprocess_json(url, chunk_size_httpx, max_retries=3, timeout=10):
    """
    Preprocesses a JSON file from a given URL.

    Args:
        url (str): The URL of the JSON file.
        chunk_size_httpx (int): The chunk size for streaming the HTTP response.
        max_retries (int, optional): The maximum number of retry attempts. Defaults to 3.
        timeout (int, optional): The timeout duration for the HTTP request. Defaults to 10.

    Yields:
        str: Processed lines from the JSON file.

    Raises:
        httpx.HTTPError: If the data retrieval fails after the maximum number of retry attempts.
    """
    attempts = 0
    while attempts < max_retries:
        try:
            with httpx.stream('GET', url, timeout=timeout) as response:
                response.raise_for_status()
                buffer = bytearray()
                for chunk in response.iter_bytes(chunk_size=chunk_size_httpx):
                    buffer.extend(chunk)
                    while buffer:
                        last_newline_pos = buffer.find(b'\n')
                        if last_newline_pos == -1:
                            break  # Incomplete line, wait for more data
                        line = buffer[:last_newline_pos + 1].decode()
                        buffer = buffer[last_newline_pos + 1:]
                        line = re.sub(r'NaN', 'null', line)
                        line = re.sub(r'NumberInt\((.*?)\)', r'\1', line)
                        yield line
            break  # Successful retrieval, exit the loop
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            attempts += 1
            print(f"Attempt {attempts}/{max_retries} failed: {e}. Retrying...")
            time.sleep(1)  # Wait for a second before retrying
    if attempts == max_retries:
        raise httpx.HTTPError(f"Failed to retrieve data after {max_retries} attempts.")

class PreprocessedFile:
    def __init__(self, url, chunk_size_httpx):
        """
        Initialize the class with the given parameters.

        Args:
            url (str): The URL to fetch the JSON data from.
            chunk_size_httpx (int): The chunk size to use for HTTPX requests.

        Returns:
            None
        """
        self.generator = preprocess_json(url, chunk_size_httpx)
        self.buffer = deque()
        self.buffer_length = 0  # Track the length of strings in the buffer

    def append_to_buffer(self, line):
        """
        Appends a line to the buffer.

        Args:
            line (str): The line to be appended to the buffer.

        Returns:
            None
        """
        if line is not None:
            self.buffer.append(line)
            self.buffer_length += len(line)

    def pop_from_buffer(self):
            """
            Removes and returns the first line from the buffer.

            Returns:
                str: The first line from the buffer.
            """
            line = self.buffer.popleft()
            self.buffer_length -= len(line)
            return line

    def read(self, size=-1):
        """
        Reads and returns a specified number of characters from the buffer.

        Args:
            size (int, optional): The number of characters to read from the buffer. 
                                  If not specified or set to -1, reads all characters.

        Returns:
            str: The characters read from the buffer.

        """
        while size < 0 or self.buffer_length < size:
            try:
                line = next(self.generator)
                self.append_to_buffer(line)
            except StopIteration:
                break

        if size < 0:
            result = ''.join(self.buffer)
            self.buffer.clear()
            self.buffer_length = 0
            return result

        result_list = []
        remaining_size = size

        while self.buffer and remaining_size > 0:
            line = self.pop_from_buffer()
            line_length = len(line)

            if line_length <= remaining_size:
                result_list.append(line)
                remaining_size -= line_length
            else:
                # Split line if it's too long and put the remainder back in the buffer
                result_list.append(line[:remaining_size])
                self.buffer.appendleft(line[remaining_size:])
                self.buffer_length += line_length - remaining_size
                break

        return ''.join(result_list)

def get_cleaned_data(url, chunk_size_httpx):
    """
    Retrieves cleaned data from a given URL using a specified chunk size.

    Args:
        url (str): The URL to retrieve the data from.
        chunk_size_httpx (int): The chunk size to use for processing the data.

    Returns:
        objects: The cleaned data objects.
    """
    preprocessed_file = PreprocessedFile(url, chunk_size_httpx)
    objects = ijson.items(preprocessed_file, 'item')
    return objects

def parse_ijson_object(cleaned_data, batch_size):
    """
    Parses a JSON object containing articles and extracts valid authors and references.

    Args:
        cleaned_data (list): A list of JSON objects representing articles.
        batch_size (int): The size of each batch of articles to process.

    Yields:
        tuple: A tuple containing two lists - authors_batch_chunk and references_batch_chunk.
            authors_batch_chunk (list): A list of JSON objects representing articles with valid authors.
            references_batch_chunk (list): A list of JSON objects representing articles with references.
    """
    def chunked_iterable(iterable, size):
        """
        Splits an iterable into chunks of a specified size.

        Args:
            iterable (iterable): The iterable to be split into chunks.
            size (int): The size of each chunk.

        Yields:
            list: A chunk of the specified size from the iterable.
        """
        it = iter(iterable)
        while True:
            chunk = list(islice(it, size))
            if not chunk:
                break
            yield chunk

    def process_articles_chunk(articles_chunk):
        article_id_article_title_list = []
        authors_id_author_name_list = []
        article_id_author_id_list = []
        article_id_reference_id_list = []

        # Sets to keep track of seen identifiers
        seen_pair_article_id_article_title = set()
        seen_pair_authors_id_author_name = set()
        seen_pair_article_id_author_id = set()
        seen_pair_article_id_reference_id = set()

        for item in articles_chunk:
            article_id = item.get('_id')
            article_title = item.get('title')

            # article_list
            if article_id and article_title:
                if ((article_id, article_title) not in seen_pair_article_id_article_title):
                    seen_pair_article_id_article_title.add((article_id, article_title))
                    article_id_article_title_list.append({'_id': article_id, 'title': article_title})

            # authors
            authors = item.get('authors', [])
            for author in authors:
                if 'name' in author and '_id' in author and len(author) == 2:
                    author_id = author['_id']
                    author_name = author['name']
                    # Check if this author hasn't been processed before
                    if ((author_id, author_name) not in seen_pair_authors_id_author_name):
                        seen_pair_authors_id_author_name.add((author_id, author_name))
                        authors_id_author_name_list.append({'_id': author_id, 'name': author_name})

                    # Check if this article-author pair hasn't been processed before
                    if ((article_id, author_id) not in seen_pair_article_id_author_id):
                        seen_pair_article_id_author_id.add((article_id, author_id))
                        article_id_author_id_list.append({'article_id': article_id, 'author_id': author_id})

            # references
            references = item.get('references', [])
            if references:
                for ref in references:
                    if ((article_id, ref) not in seen_pair_article_id_reference_id):
                        seen_pair_article_id_reference_id.add((article_id, ref))
                        article_id_reference_id_list.append({'mainArticleId': article_id, 'refId': ref})

        return article_id_article_title_list, authors_id_author_name_list, article_id_author_id_list, article_id_reference_id_list

    for articles_chunk in chunked_iterable(cleaned_data, batch_size):
        yield process_articles_chunk(articles_chunk)

# Neo4j
def neo4j_startup(uri, username, password):
    """
    Connects to a Neo4j database using the provided URI, username, and password.
    Sets up index constraints for the Neo4j database.
    
    Parameters:
    - uri (str): The URI of the Neo4j database.
    - username (str): The username for authentication.
    - password (str): The password for authentication.
    """
    def neo4j_index_constraints(session):
        with session.begin_transaction() as tx:
            # index
            tx.run("CREATE INDEX composite_range_rel_authored IF NOT EXISTS FOR ()-[a:AUTHORED]-() ON (a._id)")
            tx.run("CREATE INDEX composite_range_rel_references IF NOT EXISTS FOR ()-[r:REFERENCES]-() ON (r._id)")
            tx.run("CREATE INDEX article_index IF NOT EXISTS FOR (a:Article) ON (a.title)")
            tx.run("CREATE INDEX author_index IF NOT EXISTS FOR (a:Author) ON (a.name)")
            # unique
            tx.run("CREATE CONSTRAINT article_id_uniqueness IF NOT EXISTS FOR (a:Article) REQUIRE (a._id) IS UNIQUE")
            tx.run("CREATE CONSTRAINT author_id_uniqueness IF NOT EXISTS FOR (a:Author) REQUIRE (a._id) IS UNIQUE")
            # Commit the transaction at the end of the batch
            tx.commit()

    driver = GraphDatabase.driver(uri, auth=(username, password))
    with driver.session() as session:
        neo4j_index_constraints(session)
    
    # Close the driver
    driver.close()

def send_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password,\
                       article_id_article_title_list,\
                        authors_id_author_name_list,\
                        article_id_author_id_list,\
                        article_id_reference_id_list,\
                        batch_size_apoc):
    """
    Sends data to Neo4j database using the provided parameters.

    Parameters:
    - neo4j_uri (str): The URI of the Neo4j database.
    - neo4j_user (str): The username for authentication.
    - neo4j_password (str): The password for authentication.
    - article_id_article_title_list (list): A list of dictionaries containing article IDs and titles.
    - authors_id_author_name_list (list): A list of dictionaries containing author IDs and names.
    - article_id_author_id_list (list): A list of dictionaries containing article IDs and author IDs.
    - article_id_reference_id_list (list): A list of dictionaries containing article IDs and reference IDs.
    - batch_size_apoc (int): The batch size for processing data in Neo4j.

    Returns:
    None
    """
    def send_article_id_article_title_batch(session, article_id_article_title_list, batch_size_apoc):
        query = """
            CALL apoc.periodic.iterate(
                'UNWIND $batch AS article RETURN article',
                'MERGE (a:Article {_id: article._id}) ON CREATE SET a.title = article.title',
                {batchSize: $batch_size_apoc, parallel: false, params: {batch: $batch}}
            )
        """
        session.run(query, batch=article_id_article_title_list, batch_size_apoc=batch_size_apoc)

    def send_authors_id_author_name_batch(session, authors_id_author_name_list, batch_size_apoc):
        query = """
            CALL apoc.periodic.iterate(
                'UNWIND $batch AS author RETURN author',
                'MERGE (a:Author {_id: author._id}) ON CREATE SET a.name = author.name',
                {batchSize: $batch_size_apoc, parallel: false, params: {batch: $batch}}
            )
        """
        session.run(query, batch=authors_id_author_name_list, batch_size_apoc=batch_size_apoc)

    def send_article_id_author_id_batch(session, article_id_author_id_list, batch_size_apoc):
        query = """
            CALL apoc.periodic.iterate(
                'UNWIND $batch AS relation RETURN relation',
                'MATCH (a:Article {_id: relation.article_id}) MATCH (auth:Author {_id: relation.author_id}) MERGE (auth)-[:AUTHORED]->(a)',
                {batchSize: $batch_size_apoc, parallel: false, params: {batch: $batch}}
            )
        """
        session.run(query, batch=article_id_author_id_list, batch_size_apoc=batch_size_apoc)

    def send_article_id_reference_id_batch(session, article_id_reference_id_list, batch_size_apoc):
        query = """
            CALL apoc.periodic.iterate(
                'UNWIND $batch AS relation RETURN relation',
                'MATCH (mainArticle:Article {_id: relation.mainArticleId}) MERGE (refArticle:Article {_id: relation.refId}) MERGE (mainArticle)-[:REFERENCES]->(refArticle)',
                {batchSize: $batch_size_apoc, parallel: false, params: {batch: $batch}}
            )
        """
        session.run(query, batch=article_id_reference_id_list, batch_size_apoc=batch_size_apoc)
    
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    
    with driver.session() as session:
        if article_id_article_title_list:
            send_article_id_article_title_batch(session, article_id_article_title_list, batch_size_apoc)
        if authors_id_author_name_list:
            send_authors_id_author_name_batch(session, authors_id_author_name_list, batch_size_apoc)
        if article_id_author_id_list:
            send_article_id_author_id_batch(session, article_id_author_id_list, batch_size_apoc)
        if article_id_reference_id_list:
            send_article_id_reference_id_batch(session, article_id_reference_id_list, batch_size_apoc)

    driver.close()

def main(neo4j_uri, neo4j_user, neo4j_password, url,\
        BATCH_SIZE, TOTAL_ARTICLES, batch_size_apoc,\
        chunk_size_httpx):
    # Neo4j optimization
    neo4j_startup(neo4j_uri, neo4j_user, neo4j_password)

    # Parse JSON file and get a generator of cleaned data
    cleaned_data_generator = get_cleaned_data(url, chunk_size_httpx)

    # Create the generator
    article_batches_generator = parse_ijson_object(cleaned_data_generator, BATCH_SIZE)

    # Initialize tqdm with the total count of articles
    t = tqdm(total=TOTAL_ARTICLES, unit=' article_id')
    for article_id_article_title_list,\
        authors_id_author_name_list,\
        article_id_author_id_list,\
        article_id_reference_id_list in article_batches_generator:
        
        # Update the tqdm progress bar with the number of articles processed in this batch
        t.update(len(article_id_article_title_list))
        
        # Process the current batch of articles
        send_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password,\
                                article_id_article_title_list,\
                                authors_id_author_name_list,\
                                article_id_author_id_list,\
                                article_id_reference_id_list,\
                                batch_size_apoc)

    # Close tqdm progress bar
    t.close()

# Usage
url = os.environ['JSON_FILE']
neo4j_uri = os.environ['NEO4J_URI']
neo4j_user = os.environ['NEO4J_USER']
neo4j_password = os.environ['NEO4J_PASSWORD']
BATCH_SIZE = int(os.environ['BATCH_SIZE_ARTICLES'])
batch_size_apoc = int(os.environ['BATCH_SIZE_APOC'])
chunk_size_httpx = int(os.environ['CHUNK_SIZE_HTTPX'])
TOTAL_ARTICLES = 5_810_000

# start
start_time = datetime.datetime.now()
print(f"Processing started at {start_time}")

# process articles
main(neo4j_uri, neo4j_user, neo4j_password, url, BATCH_SIZE, TOTAL_ARTICLES, batch_size_apoc, chunk_size_httpx)

# end
end_time = datetime.datetime.now()
elapsed_time = end_time - start_time
print(f"Processing finished at {end_time}. Total time taken: {elapsed_time}")