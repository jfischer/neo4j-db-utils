#!/usr/bin/env python3
# Copyright 2018 by Jeff Fischer. Licensed under the BSD 3-clause license.
"""
Build graphs from DBLP data files. The source file is in NEL
format. Our parsing intreprets the files specifically for the DBLP
data, creating Paper and Keyword nodes, based on whether the input
datatype is a string or in integer.

The file format and sample data is taken from the Standford SNAP repository:
https://snap.stanford.edu/data/com-DBLP.html

The original source of the data is:
J. Yang and J. Leskovec. Defining and Evaluating Network Communities based on Ground-truth. ICDM, 2012.
"""
import sys
from os.path import exists, dirname, abspath
from collections import Counter
import argparse

try:
    import neo4j_db_utils.import_defs
except:
    modpath=dirname(dirname(abspath(__file__)))
    print(modpath)
    sys.path.append(modpath)

from neo4j_db_utils.import_defs import Node, SimpleRelationship, MapReduceTemplate
from neo4j_db_utils.build_import import add_command_args, run

class PaperNode(Node):
    def __init__(self, node_id, paper_no):
        self.node_id = str(node_id)
        self.paper_no = paper_no

    def get_node_type(self):
        return "Paper"

    def get_node_id(self):
        return self.node_id

    def reduce(self, other):
        assert other.node_id==self.node_id
        assert other.paper_no==self.paper_no
        return self

    def to_csv_row(self):
        return [self.node_id, self.paper_no, 'Paper']

    def __str__(self):
        return '(Paper %d, id=%s)' % (self.paper_no, self.node_id)

class KeywordNode(Node):
    def __init__(self, node_id, word):
        self.node_id = node_id
        self.word = word

    def get_node_type(self):
        return "Keyword"

    def get_node_id(self):
        return self.node_id

    def reduce(self, other):
        assert other.node_id==self.node_id
        assert other.word==self.word
        return self

    def to_csv_row(self):
        return [self.node_id, self.word, 'Keyword']

    def __str__(self):
        return '(Keyword %s, id=%s)' % (self.word, self.node_id)

class Graph:
    def __init__(self, use_local_nodes):
        self.use_local_nodes = use_local_nodes
        self.nodes = {}
        self.edges = []
        self.graph_id = None
        self.target = None

    def map(self):
        assert self.graph_id is not None
        assert self.target is not None
        nodes = []
        nodes_by_id = {}
        edges = []
        def get_node_id(local_name):
            if self.use_local_nodes:
                return '%d-%d' % (self.graph_id, local_name)
            else:
                return self.nodes[local_name] # the global name
        def is_paper(local_name):
            return isinstance(self.nodes[local_name], int)

        for (local_name, global_name) in self.nodes.items():
            if is_paper(local_name):
                n = PaperNode(get_node_id(local_name), global_name)
            else:
                n = KeywordNode(get_node_id(local_name), global_name)
            nodes_by_id[local_name] = n
            nodes.append(n)
        for (src, dest, label) in self.edges:
            edges.append(SimpleRelationship(nodes_by_id[src].get_node_type(),
                                            nodes_by_id[src].get_node_id(),
                                            label,
                                            nodes_by_id[dest].get_node_type(),
                                            nodes_by_id[dest].get_node_id()))
        return (nodes, edges)

use_local_nodes = False

def generate_from_file(input_filename):
    with open(input_filename, 'r') as f:
        graph = Graph(use_local_nodes)
        for line in f:
            line = line.rstrip()
            if len(line)==0:
                if graph.graph_id is not None:
                    yield graph
                print("# New graph")
                graph = Graph(use_local_nodes)
                continue
            fields = line.split()
            if fields[0]=='n':
                local_id = int(fields[1])
                global_id_str = fields[2]
                if global_id_str.isdigit():
                    global_id = int(global_id_str)
                    print("# Node %d maps to paper %d" %
                          (local_id, global_id))
                    graph.nodes[local_id] = global_id
                else:
                    graph.nodes[local_id] = global_id_str
            elif fields[0]=='e':
                #print("# edge: %s" % line)
                src = int(fields[1])
                dest = int(fields[2])
                label = fields[3]
                graph.edges.append((src, dest, label))
            elif fields[0]=='g':
                #print("# graph: %s" % line)
                graph.graph_id = int(fields[2])
            elif fields[0]=='x':
                #print("# target: %s" % line)
                graph.target = float(fields[1])
            else:
                print("Unknown line: %s" % line)

class MapReduce(MapReduceTemplate):
    def map_input(self, input):
        return input.map() # input was a Graph instance

    def get_node_header_row(self, node_type):
        if node_type=='Paper':
            return ['node_id:ID(Paper)', 'paper_no:int', ':LABEL']
        else:
            return ['node_id:ID(Keyword)', 'word', ':LABEL']

    def get_rel_header_row(self, rel_type, source_type, dest_type):
        return SimpleRelationship.get_header_row(rel_type, source_type, dest_type)


def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(description="Process a NEL file containing DBLP data")
    parser.add_argument('--local-nodes', action='store_true',
                        help="If specified, treat each node id as local to a graph (defaults to global)")
    parser.add_argument('FILENAME', metavar='FILENAME', type=str,
                        help="Name of input NEL file")
    add_command_args(parser)
    args = parser.parse_args()
    if not exists(args.FILENAME):
        parser.error("%s does not exist" % args.FILENAME)
    global use_local_nodes
    use_local_nodes = args.local_nodes
    run(args, generate_from_file(args.FILENAME), MapReduce())
    return 0


if __name__=='__main__':
    sys.exit(main())
