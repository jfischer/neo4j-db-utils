#!/usr/bin/env python3
"""
Sequential implementation of map-reduce for graph import
"""

from __future__ import print_function
from os.path import dirname, exists, abspath
import csv
import time

from .import_defs import cleanup_text


##########################################################################
#    Internal functions that do the work
##########################################################################

def map_and_reduce(mr, generator):
    nodes = []
    nodes_to_index = {}
    relationships = []
    rels_to_index = {}
    num_node_reductions = 0
    num_edge_reductions = 0
    for (i, input) in enumerate(generator):
        (new_nodes, new_relationships) = mr.map_input(input)
        for node in new_nodes:
            node_id = node.get_node_id()
            if node_id in nodes_to_index:
                # already a version in the list, merge it
                idx = nodes_to_index[node_id]
                nodes[idx] = nodes[idx].reduce(node)
                num_node_reductions += 1
            else:
                nodes.append(node)
                nodes_to_index[node_id] = len(nodes)-1
        for rel in new_relationships:
            rel_id = rel.get_rel_id()
            if rel_id in rels_to_index:
                idx = rels_to_index[rel_id]
                rels_to_index[rel_id] = relationships[rel_id].reduce(rel)
                num_edge_reductions += 1
            else:
                relationships.append(rel)
                rels_to_index[rel_id] = len(relationships)-1

        if ((i+1)%10000)==0:
            print("  Processed %d inputs" % (i+1))
    print("Map-reduce completed: %d nodes, %d relationships, %d node reductions, %d relationship reductions."%
          (len(nodes), len(relationships), num_node_reductions, num_edge_reductions))
    return (nodes, relationships)


class OutputFile:
    def __init__(self, fname):
        self.fname = fname
        self.fobj = open(fname, 'w')
        self.writer = csv.writer(self.fobj)
        self.rows = 0

    def writerow(self, row):
        self.writer.writerow(row)
        self.rows += 1

    def close(self):
        self.fobj.close()
        print("Wrote %d records to %s" % (self.rows, self.fname))


def write_nodes(mr, nodes, node_file_template):
    output_files = {}
    try:
        for node in nodes:
            ntype = node.get_node_type()
            nname = node.get_node_id()
            assert '\n' not in nname, "Node '%s' contains a newline!"
            if ntype not in output_files:
                fname = node_file_template.replace('NODE_LABEL',
                                                   ntype)
                ofile = OutputFile(fname)
                output_files[ntype] = ofile
                ofile.writer.writerow(mr.get_node_header_row(ntype))
            else:
                ofile = output_files[ntype]
            row = node.to_csv_row()
            for i in range(len(row)):
                if isinstance(row[i], list):
                    row[i] = cleanup_text(';'.join(row[i]))
                elif isinstance(row[i], bool):
                    row[i] = 'true' if row[i] else 'false'
                elif isinstance(row[i], str):
                    row[i] = cleanup_text(row[i])
                else:
                    row[i] = str(row[i])
            ofile.writerow(row)
    finally:
        for ofile in output_files.values():
            ofile.close()
    

def write_relationships(mr, relationships, edge_file_template):
    output_files = {} # indexed by (reltype, srctype, desttype)
    try:
        for rel in relationships:
            rel_id = rel.get_rel_id()
            if (rel_id.rel_type, rel_id.source_type, rel_id.dest_type) \
               not in output_files:
                fname = edge_file_template.replace('EDGE_LABEL',
                                                   "%s_%s_to_%s" %
                                                   (rel_id.rel_type,
                                                    rel_id.source_type,
                                                    rel_id.dest_type))
                ofile = OutputFile(fname)
                output_files[(rel_id.rel_type, rel_id.source_type, rel_id.dest_type)]\
                    = ofile
                ofile.writer.writerow(mr.get_rel_header_row(rel_id.rel_type,
                                                            rel_id.source_type,
                                                            rel_id.dest_type))
            else:
                ofile = output_files[(rel_id.rel_type,
                                      rel_id.source_type, rel_id.dest_type)]
            ofile.writerow([rel_id.source_id, rel_id.dest_id, rel_id.rel_type])
    finally:
        for ofile in output_files.values():
            ofile.close()

##########################################################################
#    The functions the user needs to call
##########################################################################

def add_command_args(parser):
    """Add the options we need for outputing the results
    """
    parser.add_argument('--output-node-files', type=str,
                        default='nodes-NODE_LABEL.csv',
                        help="Location and format for output node csv files, default "+
                        "nodes-NODE_LABEL.csv. At runtime, one file is "+
                        "created per unique node label (replacing NODE_LABEL with "+
                        "the actual label).")
    parser.add_argument('--output-edge-files', type=str,
                        default='edges-EDGE_LABEL.csv',
                        help="Location and format for output edge csv files, default "+
                        "is edges-EDGE_LABEL.csv. At runtime, one file is "+
                        "created per unique node label (replacing NODE_LABEL with "+
                        "the actual label).")
    parser.add_argument('--sorted', default=False, action='store_true',
                        help="If specified, sort all the nodes and relationships"+
                        " before writing to ensure consistent results."+
                             " Since it may be expensive, only use it for tests.")


def run(parsed_args, generator, map_reduce):
    if 'NODE_LABEL' not in parsed_args.output_node_files:
        raise Exception("--output-node-files value of '%s' does not contain NODE_LABEL"%
                        parsed_args.output_node_files)
    if 'EDGE_LABEL' not in parsed_args.output_edge_files:
        raise Exception("--output-edge-files value of '%s' does not contain EDGE_LABEL"%
                        parsed_args.output_edge_files)
    output_node_template = abspath(parsed_args.output_node_files)
    if not exists(dirname(output_node_template)):
        raise Exception("--output-node-files parent directory '%s' not found"%
                        dirname(output_node_template))
    output_edge_template = abspath(parsed_args.output_edge_files)
    if not exists(dirname(output_edge_template)):
        raise Exception("--output-edge-files parent directory '%s' not found"%
                        dirname(output_edge_template))

    t1 = time.time()
    (nodes, relationships) = map_and_reduce(map_reduce, generator)
    if parsed_args.sorted:
        nodes.sort(key=lambda n:(n.get_node_type(), n.get_node_id()))
        relationships.sort(key=lambda r:r.get_rel_id())

    write_nodes(map_reduce, nodes, output_node_template)
    write_relationships(map_reduce, relationships, output_edge_template)
    t2 = time.time()
    print("Completed generation of import files in %.0f seconds" % (t2-t1))
    return 0


