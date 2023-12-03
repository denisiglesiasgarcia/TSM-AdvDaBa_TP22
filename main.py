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
import re
import os
from multiprocessing import Pool

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
tqdm_logging.set_level(logging.INFO)

# JSON
def preprocess_json(url, pattern, chunk_size_httpx, max_retries=3, timeout=10):
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
                        line = pattern.sub(lambda x: 'null' if x.group() == 'NaN' else x.group(1), line)
                        yield line
            break  # Successful retrieval, exit the loop
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            attempts += 1
            print(f"Attempt {attempts}/{max_retries} failed: {e}. Retrying...")
            time.sleep(1)  # Wait for a second before retrying
    if attempts == max_retries:
        raise httpx.HTTPError(f"Failed to retrieve data after {max_retries} attempts.")

class PreprocessedFile:
    def __init__(self, url, pattern, chunk_size_httpx):
        self.generator = preprocess_json(url, pattern, chunk_size_httpx)
        self.buffer = deque()
        self.buffer_length = 0  # Track the length of strings in the buffer

    def append_to_buffer(self, line):
        if line is not None:
            self.buffer.append(line)
            self.buffer_length += len(line)

    def pop_from_buffer(self):
        line = self.buffer.popleft()
        self.buffer_length -= len(line)
        return line

    def read(self, size=-1):
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

def get_cleaned_data(url, pattern, chunk_size_httpx):
    preprocessed_file = PreprocessedFile(url, pattern, chunk_size_httpx)
    objects = ijson.items(preprocessed_file, 'item')
    return objects

def parse_ijson_object(cleaned_data, batch_size):
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

        def is_valid(item):
            # Check for presence of _id and title
            has_id_and_title = item.get('_id') not in [None, 'null', ''] and item.get('title') not in [None, 'null', '']
            # Check for presence of authors or references
            has_authors_or_references = bool(item.get('authors')) or bool(item.get('references'))
            return has_id_and_title and has_authors_or_references

        def transform_item(item):
            valid_authors = [author for author in item.get('authors', []) 
                            if author.get('_id') and author.get('name') and len(author) == 2]
            references = item.get('references', [])

            if valid_authors:
                json_author = {'_id': item['_id'], 'title': item['title'], 'authors': valid_authors}
                authors_batch_chunk.append(json_author)

            if references:
                json_ref = {'_id': item['_id'], 'title': item['title'], 'references': references}
                references_batch_chunk.append(json_ref)

        for item in articles_chunk:
            if is_valid(item):
                transform_item(item)

        authors_batch_chunk = sort_list_of_dicts(authors_batch_chunk, '_id')
        references_batch_chunk = sort_list_of_dicts(references_batch_chunk, '_id')
        return authors_batch_chunk, references_batch_chunk

    for articles_chunk in chunked_iterable(cleaned_data, batch_size):
        yield process_articles_chunk(articles_chunk)

def sort_list_of_dicts(list_of_dicts, key):
    def get_sort_key(dict_item):
        return dict_item.get(key)

    return sorted(list_of_dicts, key=get_sort_key)

# Neo4j
def neo4j_startup(uri, username, password):
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

def prepare_batches(raw_data):
    seen_authors = set()
    seen_articles = set()
    seen_relations = set()

    articles_batch = [{
        '_id': item['_id'],
        'title': item['title']
    } for item in raw_data if item['_id'] not in seen_articles and item['_id'] and item.get('title') and seen_articles.add(item['_id']) is None]

    authors_batch = []
    relations_batch = []

    for item in raw_data:
        article_id = item.get('_id')
        if not article_id:
            continue
        for author in item.get('authors', []):
            author_id = author.get('_id')
            author_name = author.get('name')
            author_key = (author_id, author_name)
            if author_key not in seen_authors and all(author_key):
                seen_authors.add(author_key)
                authors_batch.append({'_id': author_id, 'name': author_name})

            relation = (article_id, author_id)
            if relation not in seen_relations and all(relation):
                seen_relations.add(relation)
                relations_batch.append({'article_id': article_id, 'author_id': author_id})

    relations_batch = sort_list_of_dicts(relations_batch, 'article_id')

    # print(f"authors_batch: {len(authors_batch)}")
    # print(f"articles_batch: {len(articles_batch)}")
    # print(f"relations_batch: {len(relations_batch)}")
    return authors_batch, articles_batch, relations_batch

def prepare_relations_refs(raw_refs_list):
    unique_relations = set()
    for item in raw_refs_list:
        main_article_id = item['_id']
        for ref_id in item['references']:
            unique_relations.add((main_article_id, ref_id))

    refs_batch = [{'mainArticleId': main_article, 'refId': ref} for main_article, ref in unique_relations]
    return refs_batch

def send_batch(articles_batch_raw, refs_batch_raw, batch_size_apoc, uri, username, password):
    driver = GraphDatabase.driver(uri, auth=(username, password))

    # Prepare the batches
    authors_batch, articles_batch, relations_batch = prepare_batches(articles_batch_raw)
    refs_batch = prepare_relations_refs(refs_batch_raw) if refs_batch_raw else None
    # if authors_batch:
    #     print(f"authors_batch: {len(authors_batch)}")
    # if articles_batch:
    #     print(f"articles_batch: {len(articles_batch)}")
    # if relations_batch:
    #     print(f"relations_batch: {len(relations_batch)}")
    # if refs_batch:
    #     print(f"refs_batch: {len(refs_batch)}")

    with driver.session() as session:
        # Define your queries
        queries = {
            'authors': {
                'batch': authors_batch,
                'cypher': """
                    CALL apoc.periodic.iterate(
                        'UNWIND $batch AS author RETURN author',
                        'MERGE (a:Author {_id: author._id}) ON CREATE SET a.name = author.name',
                        {batchSize: $batch_size_apoc, parallel: false, params: {batch: $batch}}
                    )
                """ if authors_batch else None
            },
            'articles': {
                'batch': articles_batch,
                'cypher': """
                    CALL apoc.periodic.iterate(
                        'UNWIND $batch AS article RETURN article',
                        'MERGE (a:Article {_id: article._id}) ON CREATE SET a.title = article.title',
                        {batchSize: $batch_size_apoc, parallel: false, params: {batch: $batch}}
                    )
                """ if articles_batch else None
            },
            'authored_relations': {
                'batch': relations_batch,
                'cypher': """
                    CALL apoc.periodic.iterate(
                        'UNWIND $batch AS relation RETURN relation',
                        'MATCH (a:Article {_id: relation.article_id}) MATCH (auth:Author {_id: relation.author_id}) MERGE (auth)-[:AUTHORED]->(a)',
                        {batchSize: $batch_size_apoc, parallel: false, params: {batch: $batch}}
                    )
                """ if relations_batch else None
            },
            'references': {
                'batch': refs_batch,
                'cypher': """
                    CALL apoc.periodic.iterate(
                        'UNWIND $batch AS relation RETURN relation',
                        'MATCH (mainArticle:Article {_id: relation.mainArticleId}) MERGE (refArticle:Article {_id: relation.refId}) MERGE (mainArticle)-[:REFERENCES]->(refArticle)',
                        {batchSize: $batch_size_apoc, parallel: false, params: {batch: $batch}}
                    )
                """ if refs_batch else None
            }
        }

        # Execute each query
        for key in queries:
            query_info = queries[key]
            if query_info['batch'] and query_info['cypher']:
                session.run(query_info['cypher'], batch=query_info['batch'], batch_size_apoc=batch_size_apoc)

    driver.close()

def execute_task(task):
    """
    Execute a task by calling the specified function with the given arguments.
    """
    function, *args = task
    function(*args)

def send_data_to_neo4j_pool(uri, username, password, authors_batch_chunk, references_batch_chunk, batch_size_apoc, worker_count_neo4j, batch_size_neo4j):
    def split_data(batch_chunk, batch_size_neo4j):
        id_to_list_index = {}
        result_parts = []

        for item in batch_chunk:
            item_id = item['_id']
            if item_id not in id_to_list_index:
                if not result_parts or len(result_parts[-1]) >= batch_size_neo4j:
                    # Create a new list if the last one is full or doesn't exist
                    result_parts.append([])
                id_to_list_index[item_id] = len(result_parts) - 1
            list_index = id_to_list_index[item_id]
            result_parts[list_index].append(item)
        # print(f"result_parts: {len(result_parts)}")
        return result_parts

    author_parts_raw = split_data(authors_batch_chunk, batch_size_neo4j)
    ref_parts_raw = split_data(references_batch_chunk, batch_size_neo4j/25)

    tasks = []
    for i in range(worker_count_neo4j):
        author_part = author_parts_raw[i] if i < len(author_parts_raw) else []
        ref_part = ref_parts_raw[i] if i < len(ref_parts_raw) else []
        tasks.append((send_batch, author_part, ref_part, batch_size_apoc, uri, username, password))

    with Pool(worker_count_neo4j) as p:
        p.map(execute_task, tasks)

def main(neo4j_uri, neo4j_user, neo4j_password, url, BATCH_SIZE, TOTAL_ARTICLES, batch_size_apoc, pattern, chunk_size_httpx, worker_count_neo4j, batch_size_neo4j):
    # Neo4j optimization
    neo4j_startup(neo4j_uri, neo4j_user, neo4j_password)

    # Parse JSON file and get a generator of cleaned data
    cleaned_data_generator = get_cleaned_data(url, pattern, chunk_size_httpx)

    # Create the generator
    article_batches_generator = parse_ijson_object(cleaned_data_generator, BATCH_SIZE)

    # Initialize tqdm with the total count of articles
    t = tqdm(total=TOTAL_ARTICLES, unit=' article')
    for authors_batch_chunk, references_batch_chunk in article_batches_generator:
        # Update the tqdm progress bar with the number of articles processed in this batch
        t.update(len(authors_batch_chunk) + len(references_batch_chunk))
        # Process the current batch of articles
        send_data_to_neo4j_pool(neo4j_uri, neo4j_user, neo4j_password, authors_batch_chunk, references_batch_chunk, batch_size_apoc, worker_count_neo4j, batch_size_neo4j)

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
worker_count_neo4j = int(os.environ['WORKER_COUNT_NEO4J'])
batch_size_neo4j = int(os.environ['BATCH_SIZE_NEO4J'])

TOTAL_ARTICLES = 5_810_000
pattern = re.compile(r'NaN|NumberInt\((.*?)\)')

# start
start_time = datetime.datetime.now()
print(f"Processing started at {start_time}")

# process articles
main(neo4j_uri, neo4j_user, neo4j_password, url, BATCH_SIZE, TOTAL_ARTICLES, batch_size_apoc, pattern, chunk_size_httpx, worker_count_neo4j, batch_size_neo4j)

# end
end_time = datetime.datetime.now()
elapsed_time = end_time - start_time
print(f"Processing finished at {end_time}. Total time taken: {elapsed_time}")