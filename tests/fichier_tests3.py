import codecs
import ijson
import urllib3
import re

pattern_number_int = re.compile(r'NumberInt\((.*?)\)')
pattern_nan = re.compile(r'NaN')

def replace_patterns(line, pattern_number_int, pattern_nan):
    line = pattern_number_int.sub(r'\1', line)
    line = pattern_nan.sub('null', line)
    return line

http = urllib3.PoolManager()
resp = http.request(
    "GET",
    "http://vmrum.isc.heia-fr.ch/dblpv13.json",
    preload_content=False
)

list = []

# Use a generator to process the streamed data
for line in codecs.getreader("utf-8")(resp.stream(32)):
    line = replace_patterns(line, pattern_number_int, pattern_nan)
    yield line



resp.release_conn()












# import requests
# import ijson
# import re
# from io import BytesIO
# import time

# def stream_and_replace_large_json(url, pattern_number_int, pattern_nan, chunk_size):
#     with requests.get(url, stream=True) as r:
#         r.raise_for_status()
#         buffer = BytesIO()

#         for chunk in r.iter_content(chunk_size=chunk_size):
#             # Apply the replacements on the chunk
#             chunk = replace_patterns(chunk, pattern_number_int, pattern_nan)
#             buffer.write(chunk)
#             buffer.seek(0)

#             try:
#                 objects = ijson.items(buffer, 'item')
#                 for obj in objects:
#                     yield obj

#                 remaining_data = buffer.read()
#                 buffer = BytesIO()
#                 buffer.write(remaining_data)
#             except ijson.common.IncompleteJSONError:
#                 continue

# def replace_patterns(chunk, pattern_number_int, pattern_nan):
#     chunk_str = chunk.decode('utf-8')
#     chunk_str = pattern_number_int.sub(r'\1', chunk_str)
#     chunk_str = pattern_nan.sub('null', chunk_str)
#     return chunk_str.encode('utf-8')

# def process_articles_chunk(articles_chunk, batch_size_apoc):
#     # Initialize batches and tracking dictionaries
#     authors_batches = [[] for _ in range(batch_size_apoc)]
#     article_batches = [[] for _ in range(batch_size_apoc)]
#     relations_batches = [[] for _ in range(batch_size_apoc)]
#     references_batches = [[] for _ in range(batch_size_apoc)]

#     # Function to assign batch number ensuring no duplicates in the same batch
#     def get_or_assign_batch(col1, col2, batches, batch_size_apoc):
#         MAX_BATCH_SIZE = batch_size_apoc
#         MAX_BATCH_COUNT = batch_size_apoc

#         # Check if col1 or col2 is already in a batch
#         for batch_number in range(MAX_BATCH_COUNT):
#             col1_exists = any(col1 in item.values() for item in batches[batch_number])
#             col2_exists = any(col2 in item.values() for item in batches[batch_number])

#             if not col1_exists and not col2_exists and len(batches[batch_number]) < MAX_BATCH_SIZE:
#                 return batch_number

#         # If no suitable batch is found
#         raise ValueError("No suitable batch found. Consider increasing MAX_BATCH_COUNT or adjusting batch size.")

#     # Process articles, authors, and relations
#     unique_articles = set()
#     unique_authors = set()
#     unique_relations = set()
#     unique_references = set()
    
#     for item in articles_chunk:
#         # articles (article_id, article_title)
#         article_id = item.get('_id')
#         article_title = item.get('title')
#         if article_id and article_title:
#             if (article_id, article_title) not in unique_articles:
#                 unique_articles.add((article_id, article_title))
#                 batch_num_articles = get_or_assign_batch(article_id, article_title, article_batches, batch_size_apoc)
#                 article_batches[batch_num_articles].append({'_id': article_id, 'title': article_title})
        
#         # authors (author_id, author_name)
#         for author in item.get('authors', []):
#             author_id = author.get('_id')
#             author_name = author.get('name')
#             if author_id and author_name:
#                 if (author_id, author_name) not in unique_authors:
#                     unique_authors.add((author_id, author_name))
#                     batch_num_authors = get_or_assign_batch(author_id, author_name, authors_batches, batch_size_apoc)
#                     authors_batches[batch_num_authors].append({'_id': author_id, 'name': author_name})

#                 # relations (article_id, author_id)
#                 if (article_id, author_id) not in unique_relations:
#                     unique_relations.add((article_id, author_id))
#                     batch_num_relations = get_or_assign_batch(article_id, author_id, relations_batches, batch_size_apoc)
#                     relations_batches[batch_num_relations].append({'article_id': article_id, 'author_id': author_id})

#     # Process references (mainArticleId, refId)
#         main_article_id = article_id
#         references_id = item.get('references', [])
#         if main_article_id and references_id:
#             for ref_id in references_id:
#                 if (main_article_id, ref_id) not in unique_references:
#                     unique_references.add((main_article_id, ref_id))
#                     batch_num_refs = get_or_assign_batch(main_article_id, ref_id, references_batches, batch_size_apoc)
#                     references_batches[batch_num_refs].append({'mainArticleId': main_article_id, 'refId': ref_id})

#     # Clean up empty batches
#     authors_batches = [batch for batch in authors_batches if batch]
#     article_batches = [batch for batch in article_batches if batch]
#     relations_batches = [batch for batch in relations_batches if batch]
#     references_batches = [batch for batch in references_batches if batch]

#     return authors_batches, article_batches, relations_batches, references_batches

# # Usage
# url = 'http://vmrum.isc.heia-fr.ch/dblpv13.json'
# pattern_number_int = re.compile(r'NumberInt\((.*?)\)')
# pattern_nan = re.compile(r'NaN')
# chunk_size = 1024 * 1024  # 1 MB
# batch_size_apoc = 1000
# BATCH_SIZE_JSON = 10000
# article_batch = []

# for json_object in stream_and_replace_large_json(url, pattern_number_int, pattern_nan, chunk_size):
#     article_batch.append(json_object)

#     # Process the batch when it reaches a certain size
#     if len(article_batch) >= BATCH_SIZE_JSON:
#         authors_batches, article_batches, relations_batches, references_batches = process_articles_chunk(article_batch, batch_size_apoc)
#         print(f'Batch raw size: {len(article_batch)}')
#         print(f'Authors batch length: {len(authors_batches)}')
#         for batch in authors_batches:
#             print(len(batch))
#         print(f'Article batch length: {len(article_batches)}')
#         for batch in article_batches:
#             print(len(batch))
#         print(f'Relations batch length: {len(relations_batches)}')
#         for batch in relations_batches:
#             print(len(batch))
#         print(f'References batch length: {len(references_batches)}')
#         for batch in references_batches:
#             print(len(batch))

#         # Clear the batch after processing
#         article_batch = []

# # Process any remaining articles in the batch
# if article_batch:
#     authors_batches, article_batches, relations_batches, references_batches = process_articles_chunk(article_batch, batch_size_apoc)
#     print(f'Authors batch length: {len(authors_batches)}')
#     print(authors_batches)