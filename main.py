import ijson
from neo4j import GraphDatabase
import datetime
from tqdm_loggable.auto import tqdm
import logging
from tqdm_loggable.tqdm_logging import tqdm_logging
import os
from collections import deque
from itertools import islice
import time
from multiprocessing import Pool
import httpx

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

def preprocess_json(url, max_retries=3, timeout=10):
    """
    Preprocesses a JSON file from a given URL with retry logic and improved error handling.

    Args:
        url (str): The URL of the JSON file.
        max_retries (int): Maximum number of retries for the request.
        timeout (int): Timeout for the request in seconds.

    Yields:
        str: Preprocessed line from the JSON file.

    Raises:
        httpx.HTTPError: If there is an error while retrieving the file.
    """
    attempts = 0
    while attempts < max_retries:
        try:
            with httpx.stream('GET', url, timeout=timeout) as response:
                response.raise_for_status()
                buffer = bytearray()
                for chunk in response.iter_bytes():
                    buffer.extend(chunk)
                    while buffer:
                        last_newline_pos = buffer.find(b'\n')
                        if last_newline_pos == -1:
                            break  # Incomplete line, wait for more data
                        line = buffer[:last_newline_pos + 1].decode()
                        buffer = buffer[last_newline_pos + 1:]
                        yield preprocess_line(line)
            break  # Successful retrieval, exit the loop
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            attempts += 1
            print(f"Attempt {attempts}/{max_retries} failed: {e}. Retrying...")
            time.sleep(1)  # Wait for a second before retrying
    if attempts == max_retries:
        raise httpx.HTTPError(f"Failed to retrieve data after {max_retries} attempts.")

class PreprocessedFile:
    def __init__(self, url):
        """
        Initializes a PreprocessedFile object.

        Args:
            url (str): The name of the file to preprocess.
        """
        self.generator = preprocess_json(url)
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

def get_cleaned_data(url):
    """
    Retrieves cleaned data from a preprocessed file.

    Args:
        url (str): The path to the preprocessed file.

    Returns:
        iterator: An iterator over the cleaned data items.
    """
    preprocessed_file = PreprocessedFile(url)
    return ijson.items(preprocessed_file, 'item')

def parse_ijson_object(cleaned_data, batch_size):
    """
    Parses the cleaned data in chunks and extracts information about authors and references from each article.

    Args:
        cleaned_data (iterable): The cleaned data containing articles.
        batch_size (int): The size of each chunk to process.

    Yields:
        tuple: A tuple containing two lists - authors_batch_chunk and references_batch_chunk.
            authors_batch_chunk (list): A list of dictionaries containing information about authors and their respective articles.
            references_batch_chunk (list): A list of dictionaries containing information about article references.
    """
    def chunked_iterable(iterable, size):
        it = iter(iterable)
        while True:
            chunk = list(islice(it, size))
            if not chunk:
                break
            yield chunk

    def process_articles_chunk(articles_chunk):
        authors_batch_chunk = []
        references_batch_chunk = []

        for article in articles_chunk:
            article_id = article.get('_id')
            article_title = article.get('title')

            if not (article_id and article_title and article_id != 'null'):
                continue

            for author in article.get('authors', []):
                author_id = author.get('_id')
                author_name = author.get('name')
                if author_id and author_name:
                    authors_batch_chunk.append({
                        'article': {'article_id': article_id, 'article_title': article_title},
                        'authors': [{'_id': author_id, 'name': author_name}]
                    })

            references = article.get('references', [])
            if references:
                references_batch_chunk.append({
                    'article_id': article_id,
                    'article_title': article_title,
                    'references': references
                })

        return authors_batch_chunk, references_batch_chunk

    for articles_chunk in chunked_iterable(cleaned_data, batch_size):
        yield process_articles_chunk(articles_chunk)

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
            tx.run("CREATE INDEX article_index FOR (a:Article) ON (a._id, a.title)")
            tx.run("CREATE INDEX author_index FOR (a:Author) ON (a._id, a.name)")
            tx.run("CREATE INDEX composite_range_rel_authored FOR ()-[a:AUTHORED]-() ON (a._id, a.title, a.name)")
            tx.run("CREATE INDEX composite_range_rel_references FOR ()-[r:REFERENCES]-() ON (r._id, r.title)")
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

def execute_task(task):
    """
    Execute a task by calling the specified function with the given arguments.

    Parameters:
    task (tuple): A tuple containing the function to be called and its arguments.

    Returns:
    None
    """
    function, *args = task
    function(*args)

def send_batch_author(authors_batch, batch_size_apoc, uri, username, password):
    """
    Sends a batch of authors to a Neo4j database.

    Args:
        authors_batch (list): List of dictionaries representing authors.
        batch_size_apoc (int): Size of each batch for APOC periodic iterate.
        uri (str): URI of the Neo4j database.
        username (str): Username for authentication.
        password (str): Password for authentication.
    """
    driver = GraphDatabase.driver(uri, auth=(username, password))
    with driver.session() as session:
        query = """
        CALL apoc.periodic.iterate(
            'UNWIND $authors_batch AS row RETURN row',
            'WITH row.article AS article, row.authors AS authors
            UNWIND authors AS authorData
            WITH article, authorData
            CALL apoc.merge.node(["Article"], {_id: article.article_id}, {title: article.article_title}) YIELD node AS a
            CALL apoc.merge.node(["Author"], {_id: authorData._id}, {name: authorData.name}) YIELD node AS author
            CALL apoc.create.relationship(author, "AUTHORED", {}, a) YIELD rel
            RETURN rel',
            {batchSize: $batch_size_apoc, parallel:false, params:{authors_batch: $authors_batch}})
        """
        session.run(query, authors_batch=authors_batch, batch_size_apoc=batch_size_apoc)
    driver.close()

def send_batch_ref(refs_batch, batch_size_apoc, uri, username, password):
    driver = GraphDatabase.driver(uri, auth=(username, password))
    with driver.session() as session:
        query = """
        CALL apoc.periodic.iterate(
            'UNWIND $refs_batch AS row RETURN row',
            'WITH row.article_id AS mainArticleId, row.article_title AS mainArticleTitle, row.references AS refIds
            CALL apoc.merge.node(["Article"], {_id: mainArticleId}, {title: mainArticleTitle}) YIELD node AS mainArticle
            WITH mainArticle, refIds
            UNWIND refIds AS refId
            CALL apoc.merge.node(["Article"], {_id: refId}) YIELD node AS refArticle
            MERGE (mainArticle)-[:REFERENCES]->(refArticle)',
            {batchSize: $batch_size_apoc, parallel:false, params:{refs_batch: $refs_batch}})
        """
        session.run(query, refs_batch=refs_batch, batch_size_apoc=batch_size_apoc)
    driver.close()

def send_data_to_neo4j(uri, username, password, author_lists, references_lists, batch_size_apoc):
    def split_data_author(data_list, num_parts):
        """
        Splits a list of data into multiple parts based on the authors of each item.

        Args:
            data_list (list): The list of data to be split.
            num_parts (int): The number of parts to split the data into.

        Returns:
            list: A list of lists, where each inner list contains the data items with the same authors.
        """
        # Initialize result lists
        result_lists = [[] for _ in range(num_parts)]

        # Dictionary to track which list an article_id has been added to
        id_list_map = {}

        for item in data_list:
            # Check if 'article' key exists and has 'article_id' and 'article_title'
            if 'article' in item and 'article_id' in item['article'] and 'article_title' in item['article']:
                article_id = item['article']['article_id']
                valid_authors = []

                # Check if 'authors' key exists and is a list
                if 'authors' in item and isinstance(item['authors'], list):
                    # Check each author for '_id' and 'name'
                    for author in item['authors']:
                        if '_id' in author and 'name' in author:
                            valid_authors.append(author)
                
                # Only proceed if there are valid authors
                if valid_authors:
                    item['authors'] = valid_authors  # Update the authors list with only valid authors
                    
                    # Check if this article_id is already in one of the lists
                    if article_id in id_list_map:
                        # Add the item to the same list as previous items with this article_id
                        result_lists[id_list_map[article_id]].append(item)
                    else:
                        # Choose a list for this article_id and remember the choice
                        chosen_list = len(id_list_map) % num_parts
                        id_list_map[article_id] = chosen_list
                        result_lists[chosen_list].append(item)
                # If the item doesn't meet the criteria, it will be ignored (skipped)

        return result_lists

    def split_data_ref(data_list, num_parts):
        """
        Splits a list of data into multiple parts based on the 'article_id' and 'references' criteria.

        Args:
            data_list (list): The list of data to be split.
            num_parts (int): The number of parts to split the data into.

        Returns:
            list: A list of result lists, where each result list contains data items that have the same 'article_id'.
        """
        # Initialize result lists
        result_lists = [[] for _ in range(num_parts)]

        # Dictionary to track which list an article_id has been added to
        id_list_map = {}

        for item in data_list:
            # Check if 'article_id', 'article_title' exist and 'references' is not empty and contains valid data
            if ('article_id' in item and 'article_title' in item and 
                'references' in item and item['references']):
                article_id = item['article_id']
                
                # Filter out invalid references (e.g., empty strings, nulls)
                valid_references = [ref for ref in item['references'] if ref]
                
                # Only proceed if there are valid references
                if valid_references:
                    item['references'] = valid_references  # Update the references list with only valid references
                    
                    # Check if this article_id is already in one of the lists
                    if article_id in id_list_map:
                        # Add the item to the same list as previous items with this article_id
                        result_lists[id_list_map[article_id]].append(item)
                    else:
                        # Choose a list for this article_id and remember the choice
                        chosen_list = len(id_list_map) % num_parts
                        id_list_map[article_id] = chosen_list
                        result_lists[chosen_list].append(item)
                # If the item doesn't meet the criteria, it will be ignored (skipped)

        return result_lists

    # Initialize the variables to empty lists to ensure they are always defined
    author_parts = []
    ref_parts = []

    num_parts = 2 # Number of parts to split the data into

    # Split the data into num_parts parts
    if author_lists:
        author_parts = split_data_author(author_lists, num_parts)
    if references_lists:
        ref_parts = split_data_ref(references_lists, num_parts)

    # # Start a session and process the data in batches
    tasks = []
    if author_parts:
        tasks.extend([(send_batch_author, part, batch_size_apoc, uri, username, password) for part in author_parts])
    if ref_parts:
        tasks.extend([(send_batch_ref, part, batch_size_apoc, uri, username, password) for part in ref_parts])

    with Pool() as p:
        p.map(execute_task, tasks)

def main(neo4j_uri, neo4j_user, neo4j_password, url, BATCH_SIZE, TOTAL_ARTICLES, batch_size_apoc):
    # Neo4j cleanup and optimization
    neo4j_startup(neo4j_uri, neo4j_user, neo4j_password)

    # Parse JSON file and get a generator of cleaned data
    cleaned_data_generator = get_cleaned_data(url)

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

    # Optional: Close the tqdm progress bar once processing is complete
    t.close()

# Usage
url = os.environ['JSON_FILE']
neo4j_uri = os.environ['NEO4J_URI']
neo4j_user = os.environ['NEO4J_USER']
neo4j_password = os.environ['NEO4J_PASSWORD']
BATCH_SIZE = int(os.environ['BATCH_SIZE_ARTICLES'])
batch_size_apoc = int(os.environ['BATCH_SIZE_APOC'])
TOTAL_ARTICLES = 17_100_000

# start
start_time = datetime.datetime.now()
print(f"Processing started at {start_time}")

# process articles
main(neo4j_uri, neo4j_user, neo4j_password, url, BATCH_SIZE, TOTAL_ARTICLES, batch_size_apoc)

# end
end_time = datetime.datetime.now()
elapsed_time = end_time - start_time
print(f"Processing finished at {end_time}. Total time taken: {elapsed_time}")