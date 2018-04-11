# query data from neo4j graph

# TODO: model and migrate

import py2neo as neo
from py2neo import ogm

graph = neo.Graph(password='ivey198013')
selector = neo.NodeSelector(graph)

def query_all_concepts() -> list:
    concepts = selector.select('Concept')
    return [cp['name'] for cp in concepts]

def query_new_concepts(delta):
    pass

def query_related_nodes(depth=1):
    pass

class Company(ogm.GraphObject):
    __primarykey__ = 'name'

    name = ogm.Property()
    sid = ogm.Property()
    created = ogm.Property()

    concepts = ogm.RelatedTo('Concept', 'CONCEPT')

class Concept(ogm.GraphObject):
    __primarykey__ = 'name'

    name = ogm.Property()
    created = ogm.Property()

    companys = ogm.RelatedFrom('Company', 'CONCEPT')
