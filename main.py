import ijson
from neo4j import GraphDatabase
import datetime
from tqdm_loggable.auto import tqdm
import logging
from tqdm_loggable.tqdm_logging import tqdm_logging
import os
from collections import deque
from itertools import islice

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
tqdm_logging.set_level(logging.INFO)


# JSON
def preprocess_line(line):
    return line.replace('NaN', 'null').replace('NumberInt(', '').replace(')', '')

def preprocess_json(filename, buffer_size):
    with open(filename, 'r', encoding='utf-8') as file:
        buffer = ''
        while True:
            chunk = file.read(buffer_size)
            if not chunk:
                if buffer:
                    yield preprocess_line(buffer)
                break  # End of file
            buffer += chunk
            while '\n' in buffer:
                line, buffer = buffer.split('\n', 1)
                yield preprocess_line(line)

class PreprocessedFile:
    def __init__(self, filename, buffer_size=65536):  # buffer_size is in bytes
        self.generator = preprocess_json(filename, buffer_size)
        self.buffer = deque()

    def read(self, size=-1):
        while size < 0 or sum(len(line) for line in self.buffer) < size:
            try:
                self.buffer.append(next(self.generator))
            except StopIteration:
                break  # End of generator, return what's left in the buffer
        if size < 0:
            result = ''.join(self.buffer)
            self.buffer.clear()
        else:
            result_list = []
            remaining_size = size
            while self.buffer and remaining_size > 0:
                line = self.buffer.popleft()
                result_list.append(line[:remaining_size])
                remaining_size -= len(line)
                if remaining_size < 0:
                    self.buffer.appendleft(line[remaining_size:])
            result = ''.join(result_list)
        return result

def get_cleaned_data(filename):
    preprocessed_file = PreprocessedFile(filename)
    return ijson.items(preprocessed_file, 'item')

def parse_ijson_object(cleaned_data, batch_size):
    def chunked_iterable(iterable, size):
        it = iter(iterable)
        while chunk := list(islice(it, size)):
            yield chunk

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
            if not (article_id and article_title):
                continue  # Skip this article if it doesn't have an id or title

            authors = article.get('authors', [])
            if authors:
                for author in authors:
                    author_id = author.get('_id')
                    author_name = author.get('name')
                    if author_id and author_name:
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
            if references:
                references_data = {
                    'article_id': article['_id'],
                    'article_title': article['title'],
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
        session.run("MATCH (n) DETACH DELETE n")
        session.execute_write(drop_all_constraints_and_indexes)
        # optimize Neo4j
        neo4j_index_constraints(session)
    
    # Close the driver
    driver.close()

def send_data_to_neo4j(uri, username, password, author_lists, references_lists):
    # Function to send a single batch to the database
    def send_batch_author(tx, authors_batch):
        query = """
        UNWIND $authors_batch AS row
        MERGE (a:Article {_id: row.article.article_id})
            ON CREATE SET a.title = row.article.article_title
        WITH a, row
        UNWIND row.authors AS authorData
        MERGE (author:Author {_id: authorData._id})
            ON CREATE SET author.name = authorData.name
        MERGE (author)-[:AUTHORED]->(a)
        """

        tx.run(query, authors_batch=authors_batch)

    def send_batch_ref(tx, references_batch):
        query = """
        UNWIND $references_batch AS refRow
        MERGE (refArticle:Article {_id: refRow.article_id})
            ON CREATE SET refArticle.title = refRow.article_title
        WITH refArticle, refRow
        UNWIND refRow.references AS reference
        MERGE (referredArticle:Article {_id: reference})
        MERGE (refArticle)-[:CITES]->(referredArticle)
        """

        tx.run(query, references_batch=references_batch)

    # Connect to Neo4j
    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    # Start a session and process the data in batches
    with driver.session() as session:
        if author_lists:
            session.execute_write(send_batch_author, author_lists)
        if references_lists:
            session.execute_write(send_batch_ref, references_lists)

    # Close the driver
    driver.close()

def main(neo4j_uri, neo4j_user, neo4j_password, filename, BATCH_SIZE, TOTAL_ARTICLES):
    # Neo4j cleanup and optimization
    neo4j_startup(neo4j_uri, neo4j_user, neo4j_password)

    # Parse JSON file and get a generator of cleaned data
    cleaned_data_generator = get_cleaned_data(filename)
    
    # Create the generator
    article_batches_generator = parse_ijson_object(cleaned_data_generator, BATCH_SIZE)

    # Initialize tqdm with the total count of articles
    t = tqdm(total=TOTAL_ARTICLES, unit=' article')

    # Loop through all the batches from the generator with a progress bar
    for articles_authors_batch, articles_references_batch in article_batches_generator:
        # Update the tqdm progress bar with the number of articles processed in this batch
        t.update(len(articles_authors_batch))  # Assuming each batch represents multiple articles
        
        # Process the current batch of articles
        send_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, articles_authors_batch, articles_references_batch)

    # Optional: Close the tqdm progress bar once processing is complete
    t.close()

# Usage
filename = os.environ['JSON_FILE']
neo4j_uri = os.environ['NEO4J_URI']
neo4j_user = os.environ['NEO4J_USER']
neo4j_password = os.environ['NEO4J_PASSWORD']
BATCH_SIZE = 10000
TOTAL_ARTICLES = 5354309

# start
start_time = datetime.datetime.now()
print(f"Processing started at {start_time}")

# process articles
main(neo4j_uri, neo4j_user, neo4j_password, filename, BATCH_SIZE, TOTAL_ARTICLES)

# end
end_time = datetime.datetime.now()  # <-- 3. Get the current time again
elapsed_time = end_time - start_time
print(f"Processing finished at {end_time}. Total time taken: {elapsed_time}")