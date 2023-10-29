import ijson
import re
from neo4j import GraphDatabase
import datetime
from tqdm_loggable.auto import tqdm
import logging
from tqdm_loggable.tqdm_logging import tqdm_logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
tqdm_logging.set_level(logging.INFO)

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

def send_articles_to_neo4j(session, article_lists):
    with session.begin_transaction() as tx:
        query = """
                MERGE (a:Article {_id: $article_id})
                ON CREATE SET a.title = $article_title
                ON MATCH SET a.title = $article_title
                """
        for article in article_lists:
            article_id = article[0]
            article_title = article[1]
            tx.run(
                query,
                article_id=article_id,
                article_title=article_title
                )
        tx.commit()  # Commit the transaction at the end of the batch

def send_authors_to_neo4j(session, author_lists):
    with session.begin_transaction() as tx:
        query = """
                MERGE (a:Article {_id: $article_id})
                ON CREATE SET a.title = $article_title
                ON MATCH SET a.title = $article_title
                MERGE (authorNode:Author {_id: $author_id})
                SET authorNode.name = $author_name
                MERGE (authorNode)-[:AUTHORED]->(a)
                """
        for author in author_lists:
            article_id = author[0]
            article_title = author[1]
            author_id = author[2]
            author_name = author[3]
            
            if author_id and author_name:
                tx.run(
                    query,
                    author_id=author_id,
                    author_name=author_name,
                    article_title=article_title,
                    article_id=article_id
                    )
        tx.commit()  # Commit the transaction at the end of the batch

def send_references_to_neo4j(session, reference_lists):
    with session.begin_transaction() as tx:
        query = """
                MERGE (a:Article {_id: $article_id})
                MERGE (c:Article {_id: $reference_article_id})
                MERGE (a)-[:CITES]->(c)
                """
        for reference in reference_lists:
            article_id = reference[0]
            reference_article_id = reference[1]
            
            if reference_article_id:
                tx.run(
                    query,
                    article_id=article_id,
                    reference_article_id=reference_article_id)
        tx.commit()  # Commit the transaction at the end of the batch    

def articles_generator(filename):
    with open(filename, 'r') as file:
        for article in ijson.items(file, 'item', use_float=True):
            yield article

def prepare_author_lists(article):
    """
    Prepare a list of lists for authors
    """
    # Check if 'authors' key exists in the article dictionary
    if 'authors' not in article or not article['authors']:
        return []  # Return an empty list if there are no authors
    try:
        # Master list to collect the flattened data
        author_lists = []

        # Iterate through each author in the authors list
        for author in article['authors']:
            # Create a new list with _id, title, authors._id, and authors.name
            author_data = [
                article['_id'],
                article['title'],
                author['_id'],
                author['name']
            ]
            # Append this list to the master list
            author_lists.append(author_data)
        return author_lists
    except:
        pass

def prepare_article_lists(article):
    """
    Create a new list with _id and title
    """
    article_data = [
        article['_id'],
        article['title'],
    ]
    return [article_data]

def prepare_article_no_title_lists(article):
    """
    Create a new list with _id and title
    """
    article_no_title_data = [
        article['_id'],
        "Unknown title",
    ]
    return [article_no_title_data]

def prepare_references_lists(article):
    """
    Prepare a list of lists of references
    """
    # Check if 'authors' key exists in the article dictionary
    if 'references' not in article or not article['references']:
        return []  # Return an empty list if there are no authors
    try:
        # Master list to collect the flattened data
        reference_lists = []

        # Iterate through each reference in the references list
        for references in article['references']:
            # Create a new list with _id and references._id
            reference_data = [
                article['_id'],
                references['references']  # Assume each reference has an _id key
            ]
            # Append this list to the master list
            reference_lists.append(reference_data)
        return reference_lists
    except:
        pass

def preprocess_json(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        for line in file:
            line = re.sub(r'NumberInt\((\d+)\)', r'\1', line)
            line = line.replace('NaN', 'null')
            yield line

class PreprocessedFile:
    def __init__(self, filename):
        self.generator = preprocess_json(filename)
        self.buffer = ''

    def read(self, size=-1):
        while size < 0 or len(self.buffer) < size:
            try:
                self.buffer += next(self.generator)
            except StopIteration:
                # End of generator, return what's left in the buffer
                break
        if size < 0:
            result, self.buffer = self.buffer, ''
        else:
            result, self.buffer = self.buffer[:size], self.buffer[size:]
        return result

def get_cleaned_data(filename):
    """
    Remove NumberInt and NaN from the JSON file and return a generator of objects
    """
    preprocessed_file = PreprocessedFile(filename)
    return ijson.parse(preprocessed_file)  # pass the custom file-like object to ijson.parse

def process_object(objects):
    """
    Process a single object from the JSON file
    """
    current_item = {}
    authors_list = []  # Initialize an empty list to collect authors
    references_list = []  # Initialize an empty list to collect references
    inside_authors = False  # A flag to check if we are processing the authors field
    inside_references = False  # A flag to check if we are processing the references field
    for _, event, value in objects:
        if event == 'map_key':
            current_key = value
            if current_key == 'authors':
                inside_authors = True  # Set flag to True when entering authors field
            elif current_key == 'references':
                inside_references = True  # Set flag to True when entering references field
            else:
                # Reset flags and assign lists to current_item when exiting authors or references fields
                if inside_authors:
                    inside_authors = False
                    current_item['authors'] = authors_list
                    authors_list = []  # Reset authors_list for next use
                if inside_references:
                    inside_references = False
                    current_item['references'] = references_list
                    references_list = []  # Reset references_list for next use
        elif event in ('string', 'number'):
            if inside_authors:
                # If inside authors, append a new author dictionary to authors_list
                authors_list.append({current_key: value})
            elif inside_references:
                # If inside references, append a new reference dictionary to references_list
                references_list.append({current_key: value})
            else:
                current_item[current_key] = value
        elif event == 'start_map':
            # If inside authors or references, process the next dictionary
            if inside_authors:
                authors_list.append(process_object(objects))
            elif inside_references:
                references_list.append(process_object(objects))
            else:
                current_item[current_key] = process_object(objects)
        elif event == 'end_map':
            # Ensure authors and references lists are assigned to current_item if they are the last fields
            if inside_authors:
                current_item['authors'] = authors_list
            if inside_references:
                current_item['references'] = references_list
            return current_item

def main(filename, neo4j_uri, neo4j_user, neo4j_password):
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    
    with driver.session() as session:
        # debug drop all indexes
        session.execute_write(drop_all_constraints_and_indexes)
        # optimize neo4j
        neo4j_index_constraints(session)
        # process articles
        with open(filename, 'r', encoding='utf-8') as file:
            one_article = get_cleaned_data(filename)
            # Wrap your loop with tqdm, and specify the total count of articles
            t = tqdm(total=5354309, unit=' article')
            for _, event, _ in one_article:
                t.update()
                if event == 'start_map':
                    one_article_dict = process_object(one_article)
                    # add authors/articles to neo4j
                    author_lists = prepare_author_lists(one_article_dict)
                    if author_lists:
                        send_authors_to_neo4j(session, author_lists)
                    else:
                        try:
                            # articles avec titre
                            article_lists = prepare_article_lists(one_article_dict)
                            if article_lists:
                                send_articles_to_neo4j(session, article_lists)
                        except:
                            # articles sans titre
                            article_no_title_lists = prepare_article_no_title_lists(one_article_dict)
                            if article_no_title_lists:
                                send_articles_to_neo4j(session, article_no_title_lists)
                    # add references to neo4j
                    references_lists = prepare_references_lists(one_article_dict)
                    if references_lists:
                        send_references_to_neo4j(session, references_lists)

    driver.close()

# Usage
filename = os.environ['JSON_FILE']
neo4j_uri = os.environ['NEO4J_URI']
neo4j_user = os.environ['NEO4J_USER']
neo4j_password = os.environ['NEO4J_PASSWORD']

# start
start_time = datetime.datetime.now()
print(f"Processing started at {start_time}")

# process articles
main(filename, neo4j_uri, neo4j_user, neo4j_password)

# end
end_time = datetime.datetime.now()  # <-- 3. Get the current time again
elapsed_time = end_time - start_time
print(f"Processing finished at {end_time}. Total time taken: {elapsed_time}")