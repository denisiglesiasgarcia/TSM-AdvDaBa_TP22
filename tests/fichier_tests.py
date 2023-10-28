import ijson
import re
from neo4j import GraphDatabase

# Your add_article function here
def add_article(tx, article_id, title, author=None, cited_articles=None):
    tx.run(
        "MERGE (a:Article {_id: $id}) "
        "ON CREATE SET a.title = $title "
        "ON MATCH SET a.title = $title",
        id=article_id,
        title=title
    )
    if author:
        author_id = author.get('_id')
        author_name = author.get('name')
        if author_id and author_name:
            tx.run(
                "MERGE (a:Author {_id: $author_id, name: $name})", 
                author_id=author_id, 
                name=author_name
            )
            tx.run("""
                MATCH (a:Author {_id: $author_id}), (b:Article {_id: $article_id})
                MERGE (a)-[:AUTHORED]->(b)
            """, author_id=author_id, article_id=article_id)
    if cited_articles:
        for cited_article in cited_articles:
            cited_article_id = cited_article.get('_id')
            cited_article_title = cited_article.get('title')
            if cited_article_id:
                tx.run(
                    "MERGE (c:Article {_id: $cited_article_id}) "
                    "ON CREATE SET c.title = $cited_article_title "
                    "ON MATCH SET c.title = $cited_article_title",
                    cited_article_id=cited_article_id,
                    cited_article_title=cited_article_title
                )
                tx.run("""
                    MATCH (a:Article {_id: $article_id}), (c:Article {_id: $cited_article_id})
                    MERGE (a)-[:CITES]->(c)
                """, article_id=article_id, cited_article_id=cited_article_id)

def articles_generator(filename):
    with open(filename, 'r') as file:
        for article in ijson.items(file, 'item', use_float=True):
            yield article

def process_article(session, article):
    article_id = article['_id']
    title = article['title']
    for author in article.get('authors', []):
        session.execute_write(add_article, article_id, title, author=author)
    # Create a list of dictionaries for cited articles, assuming titles are not available
    cited_articles = [{'_id': cited_article_id, 'title': None} for cited_article_id in article.get('references', [])]
    # Now pass the cited_articles list to add_article
    session.execute_write(add_article, article_id, title, cited_articles=cited_articles)

def process_and_send_to_neo4j(articles_batch):
    with GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "testtest")) as driver:
        with driver.session() as session:
            for article in articles_batch:
                process_article(session, article)

def main(filename):
    batch_size = 100  # Process articles in batches of 100 (adjust as needed)
    articles_batch = []
    for article in articles_generator(filename):
        articles_batch.append(article)
        if len(articles_batch) >= batch_size:
            process_and_send_to_neo4j(articles_batch)
            articles_batch = []
    if articles_batch:  # Process any remaining articles
        process_and_send_to_neo4j(articles_batch)

# Run the main function with the filename as argument
filename1 = 'biggertest.json'
main(filename1)
