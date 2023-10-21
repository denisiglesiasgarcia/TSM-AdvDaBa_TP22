import ijson
import re
from neo4j import GraphDatabase

filename = 'biggertest.json'
uri = "bolt://neo4j:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "testtest"))

# Function to preprocess JSON lines
def corrected_json_content(file):
    corrected_lines = list(corrected_json_lines(file))
    return '\n'.join(corrected_lines)

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

def add_article(tx, article_id, title, authors, cited_articles):
    # Merge the current ARTICLE node with its title and _id properties
    tx.run("MERGE (a:Article {_id: $id}) SET a.title = $title", id=article_id, title=title)
    
    for author in authors:
        if author:  
            # Merge the AUTHOR node with its name and _id properties
            tx.run("MERGE (a:Author {_id: $author_id, name: $name})", author_id=author["_id"], name=author["name"])
            # Merge the AUTHORED relationship between AUTHOR and ARTICLE
            tx.run("""
                MATCH (a:Author {_id: $author_id}), (b:Article {_id: $article_id})
                MERGE (a)-[:AUTHORED]->(b)
            """, author_id=author["_id"], article_id=article_id)
            
    for cited_article_id in cited_articles:
        tx.run("""
            MERGE (a:Article {_id: $article_id})
            MERGE (b:Article {_id: $cited_article_id})
            MERGE (a)-[:CITES]->(b)
        """, article_id=article_id, cited_article_id=cited_article_id)

def articles_generator(filename):
    with open(filename, 'r') as file:
        articles = ijson.items(corrected_json_content(file), 'item')
        for article in articles:
            yield article

def process_and_send_to_neo4j(article):
    with driver.session() as session:
        data_list = process_article(article)
        for data in data_list:
            if data['author_name'] and data['author_id']:
                authors = [{"name": data['author_name'], "_id": data['author_id']}]
                if data['cited_article_id']:
                    session.execute_write(add_article, data['article_id'], data['article_title'], authors, [data['cited_article_id']])
                else:
                    session.execute_write(add_article, data['article_id'], data['article_title'], authors, [])

for article in articles_generator(filename):
    process_and_send_to_neo4j(article)
