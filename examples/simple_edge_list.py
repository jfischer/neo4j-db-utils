#!/usr/bin/env python
import sys
import argparse
from os.path import abspath, dirname, exists

try:
    import neo4j_db_utils.import_defs
except:
    modpath=dirname(dirname(abspath(__file__)))
    print(modpath)
    sys.path.append(modpath)

from neo4j_db_utils.import_defs import Node, SimpleRelationship, MapReduceTemplate
from neo4j_db_utils.build_import import add_command_args, run

class MyNode(Node):
    __slots__ = ('name', )
    def __init__(self, name):
        self.name = name

    def get_node_type(self):
        return 'Node'

    def get_node_id(self):
        return self.name

    def reduce(self, other):
        assert self.name == other.name
        return self

    def to_csv_row(self):
        return [self.name,'Node']

def generate_from_file(edgelist_filename):
    with open(edgelist_filename, 'r') as f:
        lineno = 0
        for line in f:
            lineno += 1
            if line.startswith('#'):
                continue
            fields = line.rstrip().split()
            if len(fields)!=3:
                raise Exception("Parse error in %s, line %d: expecting src dest label"%
                                (edgelist_filename, lineno))
            yield fields

class MapReduce(MapReduceTemplate):
    def map_input(self, input):
        return ([MyNode(input[0]), MyNode(input[1])],
                [SimpleRelationship('Node', input[0],
                                    input[2],
                                    'Node', input[1])])

    def get_node_header_row(self, node_type):
        return ['name:ID(Node)', ':LABEL']

    def get_rel_header_row(self, rel_type, source_type, dest_type):
        return SimpleRelationship.get_header_row(rel_type, source_type, dest_type)


def main(args=sys.argv[1:]):
    parser = argparse.ArgumentParser("Create .csv import files for neo4j from a simple edge list format")
    add_command_args(parser)
    parser.add_argument('input_file', metavar='INPUT_FILE', type=str,
                        help="Name of input edgelist file")
    parsed_args = parser.parse_args(args)
    if not exists(parsed_args.input_file):
        parser.error("Input file %s does not exist" % parsed_args.input_file)

    run(parsed_args, generate_from_file(parsed_args.input_file), MapReduce())
    return 0


if __name__ == '__main__':
    sys.exit(main())
