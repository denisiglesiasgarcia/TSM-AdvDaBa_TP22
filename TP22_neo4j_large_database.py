import ijson
import re
from neo4j import GraphDatabase
import datetime
from tqdm import tqdm

filename = 'dblpv13.json'
uri = "bolt://neo4j:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "testtest"))
BATCH_SIZE = 25

# Function to correct JSON lines on-the-fly
def corrected_json_lines(file):
    for line in file:
        yield re.sub(r'NumberInt\((\d+)\)', r'\1', line)

# Function to process an individual article from the JSON file
def process_article(article):
    data_list = []
    article_id = article['_id']
    article_title = article['title']
    authors = article.get('authors', [])
    references = article.get('references', [])

    for author in authors:
        author_name = author.get('name', None)
        author_id = author.get('_id', None)

        # If no references, append article and author details only
        if not references:
            data_list.append({
                'article_id': article_id,
                'article_title': article_title,
                'author_id': author_id,
                'author_name': author_name,
                'cited_article_id': None
            })
        else:
            for reference in references:
                data_list.append({
                    'article_id': article_id,
                    'article_title': article_title,
                    'author_id': author_id,
                    'author_name': author_name,
                    'cited_article_id': reference
                })

    # Handle case where there are no authors but there are references
    if not authors and references:
        for reference in references:
            data_list.append({
                'article_id': article_id,
                'article_title': article_title,
                'author_id': None,
                'author_name': None,
                'cited_article_id': reference
            })

    return data_list

def add_articles_batch(tx, data_list):
    for data in data_list:
        if data['article_title']:
            tx.run("MERGE (a:Article {_id: $id}) SET a.title = $title", id=data['article_id'], title=data['article_title'])
        else:
            tx.run("MERGE (a:Article {_id: $id})", id=data['article_id'])
    
        if data['author_name'] and data['author_id']:
            tx.run("MERGE (a:Author {_id: $author_id, name: $name})", author_id=data['author_id'], name=data['author_name'])
            tx.run("""
                MATCH (a:Author {_id: $author_id}), (b:Article {_id: $article_id})
                MERGE (a)-[:AUTHORED]->(b)
            """, author_id=data['author_id'], article_id=data['article_id'])
        
        if data['cited_article_id']:
            tx.run("""
                MERGE (a:Article {_id: $article_id})
                MERGE (b:Article {_id: $cited_article_id})
                MERGE (a)-[:CITES]->(b)
            """, article_id=data['article_id'], cited_article_id=data['cited_article_id'])

def articles_generator(filename):
    with open(filename, 'r') as file:
        for article in ijson.items(file, 'item', use_float=True):
            yield article

def process_and_send_to_neo4j(articles_batch):
    data_batch = []
    for article in articles_batch:
        data_batch.extend(process_article(article))

    with driver.session() as session:
        session.write_transaction(add_articles_batch, data_batch)

def main():
    # start
    start_time = datetime.datetime.now()
    print(f"Processing started at {start_time}")

    # process articles
    for article in articles_generator(filename):
        process_and_send_to_neo4j(article)

    # end
    end_time = datetime.datetime.now()  # <-- 3. Get the current time again
    elapsed_time = end_time - start_time
    print(f"Processing finished at {end_time}. Total time taken: {elapsed_time}")

if __name__ == '__main__':
    main()


# Write a file to indicate that the data has been imported
with open('/tmp/data_imported', 'w') as f:
    f.write('Data imported on {}\n'.format(datetime.datetime.now()))
