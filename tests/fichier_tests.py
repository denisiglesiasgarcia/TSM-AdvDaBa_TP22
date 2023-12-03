import ijson
from neo4j import GraphDatabase
import datetime
from tqdm_loggable.auto import tqdm
import logging
from tqdm_loggable.tqdm_logging import tqdm_logging
from collections import deque
from itertools import islice
import time
from multiprocessing import Pool
import httpx
import cProfile
import re

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
            WHERE article.article_id IS NOT NULL AND article.article_title IS NOT NULL AND authorData._id IS NOT NULL AND authorData.name IS NOT NULL
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
            WHERE mainArticleId IS NOT NULL AND mainArticleTitle IS NOT NULL
            CALL apoc.merge.node(["Article"], {_id: mainArticleId}, {title: mainArticleTitle}) YIELD node AS mainArticle
            WITH mainArticle, refIds
            UNWIND refIds AS refId
            WITH mainArticle, refId
            WHERE refId IS NOT NULL
            CALL apoc.merge.node(["Article"], {_id: refId}) YIELD node AS refArticle
            MERGE (mainArticle)-[:REFERENCES]->(refArticle)',
            {batchSize: $batch_size_apoc, parallel:false, params:{refs_batch: $refs_batch}})
        """
        session.run(query, refs_batch=refs_batch, batch_size_apoc=batch_size_apoc)
    driver.close()

# def main():
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
tqdm_logging.set_level(logging.INFO)

# JSON
# def process_articles_chunk(articles_chunk):
#     authors_batch_chunk = []
#     references_batch_chunk = []

#     for article in articles_chunk:
#         article_id = article.get('_id')
#         article_title = article.get('title')

#         if not (article_id and article_title and article_id != 'null'):
#             continue

#         for author in article.get('authors', []):
#             author_id = author.get('_id')
#             author_name = author.get('name')
#             if author_id and author_name:
#                 authors_batch_chunk.append({
#                     'article': {'article_id': article_id, 'article_title': article_title},
#                     'authors': [{'_id': author_id, 'name': author_name}]
#                 })

#         references = article.get('references', [])
#         if references:
#             references_batch_chunk.append({
#                 'article_id': article_id,
#                 'article_title': article_title,
#                 'references': references
#             })

#     return authors_batch_chunk, references_batch_chunk

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

    return authors_batch_chunk, references_batch_chunk

# Worker function for processing a chunk
def process_chunk(chunk):
    return process_articles_chunk(chunk)

def main():
    # def preprocess_line(line, pattern):
    #     return pattern.sub(lambda x: 'null' if x.group() == 'NaN' else x.group(1), line)

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
        # json_clean = (\
        #     {'_id': o['_id'], 'title': o['title'], 'authors': o.get('authors', []), 'references': o.get('references', [])}\
        #     for o in objects)
        return objects

    def parse_ijson_object_parallel(cleaned_data, batch_size, worker_count_parser=4):
        def chunked_iterable(iterable, size):
            it = iter(iterable)
            while True:
                chunk = list(islice(it, size))
                if not chunk:
                    break
                yield chunk

        batch_size = batch_size // worker_count_parser
        # Chunk the data
        chunks = chunked_iterable(cleaned_data, batch_size)

        # Create a pool of workers to process these chunks
        with Pool(worker_count_parser) as pool:
            for processed_chunk in pool.imap(process_chunk, chunks):
                yield processed_chunk

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

            return authors_batch_chunk, references_batch_chunk

        for articles_chunk in chunked_iterable(cleaned_data, batch_size):
            yield process_articles_chunk(articles_chunk)

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

    def send_data_to_neo4j_pool(uri, username, password, authors_batch_chunk, references_batch_chunk, batch_size_apoc, worker_count_neo4j):
        def split_data(batch_chunk, worker_count_neo4j):
            # sort
            batch_chunk = sorted(batch_chunk, key=lambda x: x['_id'])

            # Initialize result lists
            result_parts = [[] for _ in range(worker_count_neo4j)]

            # Get the total number of items and calculate the target size for each part
            total_items = len(batch_chunk)
            target_size_per_part = total_items // worker_count_neo4j

            # Variables to keep track of current part and its size
            current_part_index = 0
            current_part_size = 0

            for item in batch_chunk:
                # If the current part is full and the _id changes, move to the next part
                if (current_part_size >= target_size_per_part and 
                    current_part_index < worker_count_neo4j - 1 and 
                    item['_id'] != batch_chunk[total_items - 1]['_id']):  # Check for the last _id to avoid splitting
                    current_part_index += 1
                    current_part_size = 0

                # Add the item to the current part
                result_parts[current_part_index].append(item)
                current_part_size += 1

            return result_parts

        # Initialize the variables to empty lists to ensure they are always defined
        author_parts = []
        ref_parts = []

        # Split the data into num_parts parts
        author_parts = split_data(authors_batch_chunk, worker_count_neo4j)
        ref_parts = split_data(references_batch_chunk, worker_count_neo4j)

        # # Start a session and process the data in batches
        tasks = []
        if author_parts:
            tasks.extend([(send_batch_author, part, batch_size_apoc, uri, username, password) for part in author_parts])
        if ref_parts:
            tasks.extend([(send_batch_ref, part, batch_size_apoc, uri, username, password) for part in ref_parts])

        with Pool(worker_count_neo4j) as p:
            p.map(execute_task, tasks)

    def send_data_to_neo4j(uri, username, password, authors_batch_chunk, references_batch_chunk, batch_size_apoc):
        if authors_batch_chunk:
            send_batch_author(authors_batch_chunk, batch_size_apoc, uri, username, password)
        if references_batch_chunk:
            send_batch_ref(references_batch_chunk, batch_size_apoc, uri, username, password)

    def main1(neo4j_uri, neo4j_user, neo4j_password, url, BATCH_SIZE, TOTAL_ARTICLES, batch_size_apoc, pattern, worker_count_neo4j, worker_count_parser, chunk_size_httpx):
        # Neo4j cleanup and optimization
        neo4j_startup(neo4j_uri, neo4j_user, neo4j_password)

        # Parse JSON file and get a generator of cleaned data
        cleaned_data_generator = get_cleaned_data(url, pattern, chunk_size_httpx)

        # Create the generator
        # article_batches_generator = parse_ijson_object(cleaned_data_generator, BATCH_SIZE)
        article_batches_generator = parse_ijson_object_parallel(cleaned_data_generator, BATCH_SIZE, worker_count_parser)

        # Initialize tqdm with the total count of articles
        t = tqdm(total=TOTAL_ARTICLES, unit=' article')
        for authors_batch_chunk, references_batch_chunk in article_batches_generator:
            # Update the tqdm progress bar with the number of articles processed in this batch
            t.update(len(authors_batch_chunk) + len(references_batch_chunk))
            # Process the current batch of articles
            # send_data_to_neo4j_pool(neo4j_uri, neo4j_user, neo4j_password, authors_batch_chunk, references_batch_chunk, batch_size_apoc, worker_count_neo4j)
            # send_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, authors_batch_chunk, references_batch_chunk, batch_size_apoc)

        # Optional: Close the tqdm progress bar once processing is complete
        t.close()

    # Usage
    url = "http://vmrum.isc.heia-fr.ch/dblpv13.json"
    # url = "http://vmrum.isc.heia-fr.ch/biggertest.json"
    # url = "https://filesender.switch.ch/filesender2/download.php?token=7c7c2b1b-b7c9-43bf-94e1-a5131efcc710&archive_format=undefined&files_ids=552921"
    neo4j_uri = "bolt://localhost:7687"
    neo4j_user = "neo4j"
    neo4j_password = "testtest"
    BATCH_SIZE = 5000
    batch_size_apoc = 2000
    TOTAL_ARTICLES = 5_810_000
    worker_count_neo4j = 4
    worker_count_parser = 8
    chunk_size_httpx = 10240

    pattern = re.compile(r'NaN|NumberInt\((.*?)\)')

    # start
    start_time = datetime.datetime.now()
    print(f"Processing started at {start_time}")

    # process articles
    main1(neo4j_uri, neo4j_user, neo4j_password, url, BATCH_SIZE, TOTAL_ARTICLES, batch_size_apoc, pattern, worker_count_neo4j, worker_count_parser, chunk_size_httpx)

    # end
    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time
    print(f"Processing finished at {end_time}. Total time taken: {elapsed_time}")

if __name__ == "__main__":
    cProfile.run('main()', 'output.prof')

   # def parse_ijson_object(cleaned_data, batch_size):
    #     def chunked_iterable(iterable, size):
    #         it = iter(iterable)
    #         while True:
    #             chunk = list(islice(it, size))
    #             if not chunk:
    #                 break
    #             yield chunk

    #     # def process_articles_chunk(articles_chunk):
    #     #     authors_batch_chunk = []
    #     #     references_batch_chunk = []

    #     #     for article in articles_chunk:
    #     #         article_id = article.get('_id')
    #     #         article_title = article.get('title')

    #     #         if not (article_id and article_title and article_id != 'null'):
    #     #             continue

    #     #         for author in article.get('authors', []):
    #     #             author_id = author.get('_id')
    #     #             author_name = author.get('name')
    #     #             if author_id and author_name:
    #     #                 authors_batch_chunk.append({
    #     #                     'article': {'article_id': article_id, 'article_title': article_title},
    #     #                     'authors': [{'_id': author_id, 'name': author_name}]
    #     #                 })

    #     #         references = article.get('references', [])
    #     #         if references:
    #     #             references_batch_chunk.append({
    #     #                 'article_id': article_id,
    #     #                 'article_title': article_title,
    #     #                 'references': references
    #     #             })

    #     #     return authors_batch_chunk, references_batch_chunk

    #     for articles_chunk in chunked_iterable(cleaned_data, batch_size):
    #         yield process_articles_chunk(articles_chunk)



#     def preprocess_line(line):
#         """
#         Preprocesses a line by replacing 'NaN' with 'null' and removing 'NumberInt(' and ')'.

#         Args:
#             line (str): The line to be preprocessed.

#         Returns:
#             str: The preprocessed line.
#         """
#         return line.replace('NaN', 'null').replace('NumberInt(', '').replace(')', '')

#     def preprocess_json(url, max_retries=3, timeout=10):
#         """
#         Preprocesses a JSON file from a given URL with retry logic and improved error handling.

#         Args:
#             url (str): The URL of the JSON file.
#             max_retries (int): Maximum number of retries for the request.
#             timeout (int): Timeout for the request in seconds.

#         Yields:
#             str: Preprocessed line from the JSON file.

#         Raises:
#             httpx.HTTPError: If there is an error while retrieving the file.
#         """
#         attempts = 0
#         while attempts < max_retries:
#             try:
#                 with httpx.stream('GET', url, timeout=timeout) as response:
#                     response.raise_for_status()
#                     buffer = bytearray()
#                     for chunk in response.iter_bytes():
#                         buffer.extend(chunk)
#                         while buffer:
#                             last_newline_pos = buffer.find(b'\n')
#                             if last_newline_pos == -1:
#                                 break  # Incomplete line, wait for more data
#                             line = buffer[:last_newline_pos + 1].decode()
#                             buffer = buffer[last_newline_pos + 1:]
#                             yield preprocess_line(line)
#                 break  # Successful retrieval, exit the loop
#             except (httpx.HTTPError, httpx.TimeoutException) as e:
#                 attempts += 1
#                 print(f"Attempt {attempts}/{max_retries} failed: {e}. Retrying...")
#                 time.sleep(1)  # Wait for a second before retrying
#         if attempts == max_retries:
#             raise httpx.HTTPError(f"Failed to retrieve data after {max_retries} attempts.")







#     class PreprocessedFile:
#         def __init__(self, url):
#             """
#             Initializes a PreprocessedFile object.

#             Args:
#                 url (str): The name of the file to preprocess.
#             """
#             self.generator = preprocess_json(url)
#             self.buffer = deque()
#             self.buffer_length = 0  # Track the length of strings in the buffer

#         def append_to_buffer(self, line):
#             """
#             Appends a line to the buffer.

#             Args:
#                 line (str): The line to append to the buffer.
#             """
#             if line is not None:
#                 self.buffer.append(line)
#                 self.buffer_length += len(line)

#         def pop_from_buffer(self):
#             """
#             Pops a line from the buffer.

#             Returns:
#                 str: The popped line from the buffer.
#             """
#             line = self.buffer.popleft()
#             self.buffer_length -= len(line)
#             return line

#         def read(self, size=-1):
#             """
#             Reads and returns the specified number of characters from the file.

#             Args:
#                 size (int, optional): The number of characters to read. Defaults to -1, which means read all.

#             Returns:
#                 str: The read characters from the file.
#             """
#             while size < 0 or self.buffer_length < size:
#                 try:
#                     self.append_to_buffer(next(self.generator))
#                 except StopIteration:
#                     break  # End of generator, return what's left in the buffer
#             if size < 0:
#                 result = ''.join(self.buffer)
#                 self.buffer.clear()
#                 self.buffer_length = 0
#             else:
#                 result_list = []
#                 remaining_size = size
#                 while self.buffer and remaining_size > 0:
#                     line = self.pop_from_buffer()
#                     result_list.append(line[:remaining_size])
#                     remaining_size -= len(line)
#                     if remaining_size < 0:
#                         # If the last line was too large, put the remaining part back into the buffer
#                         leftover = line[remaining_size:]
#                         self.buffer.appendleft(leftover)
#                         self.buffer_length += len(leftover)
#                 result = ''.join(result_list)
#             return result

#     def get_cleaned_data(url):
#         """
#         Retrieves cleaned data from a preprocessed file.

#         Args:
#             url (str): The path to the preprocessed file.

#         Returns:
#             iterator: An iterator over the cleaned data items.
#         """
#         preprocessed_file = PreprocessedFile(url)
#         return ijson.items(preprocessed_file, 'item')

#     def parse_ijson_object(cleaned_data, batch_size):
#         def chunked_iterable(iterable, size):
#             iterator = iter(iterable)
#             return iter(lambda: list(islice(iterator, size)), [])

#         def process_articles_chunk(articles_chunk):
#             # Convert list of articles to DataFrame
#             df = pl.DataFrame(articles_chunk)

#             # authors
#             # TODO: use pure polars
#             def filter_valid_authors(authors_list):
#                 return [
#                     {'_id': str(author['_id']), 'name': str(author['name'])}
#                     for author in authors_list
#                     if author.get('_id') is not None and author.get('name') is not None
#                 ]
#             df_authors = df.select(pl.col("_id", "title", "authors"))\
#                 .fill_nan(None)\
#                 .filter((df['_id'].is_not_null()) & (df['title'].is_not_null()) & (df['_id'] != 'null') & (df['title'] != 'null'))\
#                 .with_columns(pl.col("_id").alias("article_id"), pl.col("title").alias("article_title"))\
#                 .filter(pl.col('authors').is_not_null())\
#                 .with_columns([pl.col('authors').map_elements(filter_valid_authors).alias('valid_authors')])\
#                 .filter(pl.col('valid_authors').list.lengths() > 0)\
#                 .to_dicts()

#             # Transforming each row into the desired format
#             df_authors_dict = [
#                 {
#                     'article': {'article_id': row['article_id'], 'article_title': row['article_title']},
#                     'authors': row['valid_authors']  # Assuming valid_authors is a list of author dicts
#                 }
#                 for row in df_authors
#             ]

#             # references
#             # Initialize df_references
#             df_references = None
#             if 'references' in df.columns:
#                 df_references = df.select(pl.col("_id", "title", "references"))\
#                     .fill_nan(None)\
#                     .filter((df['_id'].is_not_null()) & (df['title'].is_not_null()) & (df['_id'] != 'null') & (df['title'] != 'null'))\
#                     .with_columns(pl.col("_id").alias("article_id"), pl.col("title").alias("article_title"))

#             return df_authors_dict, df_references

#         for articles_chunk in chunked_iterable(cleaned_data, batch_size):
#             yield process_articles_chunk(articles_chunk)

#     # Neo4j
#     def neo4j_startup(uri, username, password):
#         """
#         Connects to a Neo4j database using the provided URI, username, and password.
#         Drops all constraints and indexes in the database for debugging purposes.
#         Creates new indexes and constraints for optimized Neo4j performance.
#         Closes the database connection at the end.
        
#         Parameters:
#         - uri (str): The URI of the Neo4j database.
#         - username (str): The username for authentication.
#         - password (str): The password for authentication.
#         """
#         def drop_all_constraints_and_indexes(tx):
#             """ 
#             Debugging function to drop all indexes and constraints in the database 
#             """
#             # Get the list of all constraints
#             result = tx.run("SHOW CONSTRAINTS")
#             for record in result:
#                 # Get the constraint name
#                 constraint_name = record['name']
#                 # Drop the constraint
#                 tx.run(f"DROP CONSTRAINT {constraint_name} IF EXISTS")

#             # Now that constraints are dropped, get the list of all indexes
#             result = tx.run("SHOW INDEXES")
#             for record in result:
#                 # Get the index name
#                 index_name = record['name']
#                 # Drop the index
#                 tx.run(f"DROP INDEX {index_name} IF EXISTS")

#         def neo4j_index_constraints(session):
#             with session.begin_transaction() as tx:
#                 # index
#                 tx.run("CREATE INDEX article_index FOR (a:Article) ON (a._id, a.title)")
#                 tx.run("CREATE INDEX author_index FOR (a:Author) ON (a._id, a.name)")
#                 tx.run("CREATE INDEX composite_range_rel_authored FOR ()-[a:AUTHORED]-() ON (a._id, a.title, a.name)")
#                 tx.run("CREATE INDEX composite_range_rel_references FOR ()-[r:REFERENCES]-() ON (r._id, r.title)")
#                 # unique
#                 tx.run("CREATE CONSTRAINT article_id_uniqueness FOR (a:Article) REQUIRE (a._id) IS UNIQUE")
#                 tx.run("CREATE CONSTRAINT author_id_uniqueness FOR (a:Author) REQUIRE (a._id) IS UNIQUE")
#                 # Commit the transaction at the end of the batch
#                 tx.commit()
        
#         # Connect to Neo4j
#         driver = GraphDatabase.driver(uri, auth=(username, password))
        
#         # Start a session and process the data in batches
#         with driver.session() as session:
#             # debug drop all indexes
#             #session.run("MATCH (n) DETACH DELETE n")
#             session.execute_write(drop_all_constraints_and_indexes)
#             # optimize Neo4j
#             neo4j_index_constraints(session)
        
#         # Close the driver
#         driver.close()

#     def send_data_to_neo4j(uri, username, password, df_authors_dict, df_references, batch_size_apoc):
#         def split_data_author(df, num_parts):
#         # Ensure the DataFrame contains the necessary columns
#             required_columns = ["article_id", "article_title", "authors"]
#             for col in required_columns:
#                 if col not in df.columns:
#                     raise ValueError(f"Column '{col}' not found in DataFrame")

#             # Group by 'article_id' and assign each group to a part
#             groups = df.group_by('article_id').agg(pl.count('article_id').alias('count'))
#             groups = groups.with_columns(
#                 (groups['article_id'].rank(method='dense') % num_parts).cast(pl.UInt32).alias('part')
#             )
#             df = df.join(groups, on='article_id', how='left')

#             # Convert each part to the desired list of dictionaries format
#             result_parts = []
#             for i in range(num_parts):
#                 part_df = df.filter(pl.col('part') == i).drop('part')
#                 part_dicts = part_df.select([
#                     pl.struct({'article_id': 'article_id', 'article_title': 'article_title'}).alias('article'),
#                     'valid_authors'
#                 ]).to_dicts()

#                 # Transforming 'valid_authors' to 'authors' in the dictionaries
#                 formatted_dicts = [{'article': item['article'], 'authors': item['valid_authors']} for item in part_dicts]
#                 result_parts.append(formatted_dicts)

#             return result_parts

#         def split_data_ref(data_list, num_parts):
#             """
#             Splits a list of data into multiple parts based on the 'article_id' and 'references' criteria.

#             Args:
#                 data_list (list): The list of data to be split.
#                 num_parts (int): The number of parts to split the data into.

#             Returns:
#                 list: A list of result lists, where each result list contains data items that have the same 'article_id'.
#             """
#             # Initialize result lists
#             result_lists = [[] for _ in range(num_parts)]

#             # Dictionary to track which list an article_id has been added to
#             id_list_map = {}

#             for item in data_list:
#                 # Check if 'article_id', 'article_title' exist and 'references' is not empty and contains valid data
#                 if ('article_id' in item and 'article_title' in item and 
#                     'references' in item and item['references']):
#                     article_id = item['article_id']
                    
#                     # Filter out invalid references (e.g., empty strings, nulls)
#                     valid_references = [ref for ref in item['references'] if ref]
                    
#                     # Only proceed if there are valid references
#                     if valid_references:
#                         item['references'] = valid_references  # Update the references list with only valid references
                        
#                         # Check if this article_id is already in one of the lists
#                         if article_id in id_list_map:
#                             # Add the item to the same list as previous items with this article_id
#                             result_lists[id_list_map[article_id]].append(item)
#                         else:
#                             # Choose a list for this article_id and remember the choice
#                             chosen_list = len(id_list_map) % num_parts
#                             id_list_map[article_id] = chosen_list
#                             result_lists[chosen_list].append(item)
#                     # If the item doesn't meet the criteria, it will be ignored (skipped)

#             return result_lists

#         # Initialize the variables to empty lists to ensure they are always defined
#         # author_parts = []
#         # # ref_parts = []

#         # num_parts = 4 # Number of parts to split the data into

#         # # Split the data into num_parts parts
#         # author_parts = split_data_author(df_authors, num_parts)
#         #ref_parts = split_data_ref(df_references, num_parts)

#         # # # Start a session and process the data in batches
#         # tasks = []
#         # if author_parts:
#         #     tasks.extend([(send_batch_author, part, batch_size_apoc, uri, username, password) for part in author_parts])
#         # # if ref_parts:
#         # #     tasks.extend([(send_batch_ref, part, batch_size_apoc, uri, username, password) for part in ref_parts])

#         # with Pool() as p:
#         #     p.map(execute_task, tasks)

#         #send_batch_author(df_authors_dict, batch_size_apoc, uri, username, password)

#     def main1(neo4j_uri, neo4j_user, neo4j_password, url, BATCH_SIZE, TOTAL_ARTICLES, batch_size_apoc):
#         # Neo4j cleanup and optimization
#         neo4j_startup(neo4j_uri, neo4j_user, neo4j_password)

#         # Parse JSON file and get a generator of cleaned data
#         cleaned_data_generator = get_cleaned_data(url)

#         # Create the generator
#         # article_batches_generator = parse_ijson_object(cleaned_data_generator, BATCH_SIZE)
#         article_batches_generator = parse_ijson_object(cleaned_data_generator, BATCH_SIZE)

#         # Initialize tqdm with the total count of articles
#         t = tqdm(total=TOTAL_ARTICLES, unit=' article')

#         # Loop through all the batches from the generator with a progress bar
#         # for articles_authors_batch, articles_references_batch in article_batches_generator:
#         #     # Update the tqdm progress bar with the number of articles processed in this batch
#         #     t.update(len(articles_authors_batch)+len(articles_references_batch))
#         #     # Process the current batch of articles
#         #     send_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, articles_authors_batch, articles_references_batch, batch_size_apoc)

#         for df_authors_dict, df_references in article_batches_generator:
#             # Update the tqdm progress bar with the number of articles processed in this batch
#             t.update(len(df_authors_dict) + (len(df_references) if df_references is not None else 0))
#             # Process the current batch of articles
#             send_data_to_neo4j(neo4j_uri, neo4j_user, neo4j_password, df_authors_dict, df_references, batch_size_apoc)

#         # Optional: Close the tqdm progress bar once processing is complete
#         t.close()

#     # Usage
#     url = "http://vmrum.isc.heia-fr.ch/dblpv13.json"
#     # url = "http://vmrum.isc.heia-fr.ch/biggertest.json"
#     neo4j_uri = "bolt://localhost:7687"
#     neo4j_user = "neo4j"
#     neo4j_password = "testtest"
#     BATCH_SIZE = 5000
#     batch_size_apoc = 5000
#     TOTAL_ARTICLES = 17_100_000

#     # start
#     start_time = datetime.datetime.now()
#     print(f"Processing started at {start_time}")

#     # process articles
#     main1(neo4j_uri, neo4j_user, neo4j_password, url, BATCH_SIZE, TOTAL_ARTICLES, batch_size_apoc)

#     # end
#     end_time = datetime.datetime.now()
#     elapsed_time = end_time - start_time
#     print(f"Processing finished at {end_time}. Total time taken: {elapsed_time}")

# if __name__ == "__main__":
#     cProfile.run('main()', 'output.prof')