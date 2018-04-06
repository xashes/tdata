# query data from neo4j graph

import py2neo as neo

graph = neo.Graph(password='ivey198013')
selector = neo.NodeSelector(graph)

def query_all_concepts() -> list:
    concepts = selector.select('Concept')
    return [cp['name'] for cp in concepts]

def query_new_concepts(delta):
    pass
