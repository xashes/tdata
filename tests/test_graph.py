from tdata import graph_proxy as tgraph
import py2neo as neo
import pytest

@pytest.fixture(scope='module')
def selector():
    graph = neo.Graph(password='ivey198013')
    return neo.NodeSelector(graph)

def test_query_all_concepts(selector):
    selection = selector.select('Concept')
    assert len(list(selection)) == len(tgraph.query_all_concepts())

def test_query_new_concepts(selector):
    pass
