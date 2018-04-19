"""
Definitions for map-reduce based creation of Neo4j import files.

We use Python's new typing to define the expected values (mostly just
for documentation purposes). Type names that we define end in a captial "T".

Enumerations are defined using the Enum functionity added in Python 3.4.
It is pretty similar to defining constants under a namespace, but the type
of the constants is the type of the Enum.

After all the type definitions, we will define the class MapReduceTemplate.
This is the class a user of the map-reduce infrastructure should subclass.

The basic flow of the map-reduce infrastructure will be:

1. Parse the headers.json input file to produce a list of dicts.
2. Pass these dicts to map_input() to create a list of nodes and a list
   of relationships.
3. Each node and relationship is hashed to a map worker which writes them
   to a file (using pickle?).
4. When the entire input has been processed, reduce works are started
5. Each reduce worker reads a subset of the node and relationship files
   into memory.
6. reduce_nodes() and reduce_relationships() are called to merge duplicate
   entires for the same node or relastionship.
7. node_to_csv_row() and rel_to_csv_row() are called by the reduce workers to
   create CSV rows from the merged nodes/relationships. These are written
   out to CSV files.
"""
from abc import ABC, abstractmethod
from collections import namedtuple
import string

##########################################################################
#    Types for the Graph Components (nodes and relationships)
##########################################################################

class Node(ABC):
    """Subclass from this to represent your nodes.
    """
    @abstractmethod
    def get_node_type(self):
        pass

    @abstractmethod
    def get_node_id(self):
        """Return the id for this node. It should be unique
        within the node type. It does not necessarily have to
        be unique across all node types."""
        pass

    @abstractmethod
    def reduce(self, other):
        """Combine two nodes with the same id"""
        pass

    @abstractmethod
    def to_csv_row(self):
        """Convert the node to a single row in the final csv. This should
        return a list of values, one per each column."""
        pass

# How we uniquely identify relationships
# We assume that there is only one edge of a given types
# between any two nodes.
RelId = namedtuple('RelId',
                   ['source_type', 'source_id', 'rel_type', 'dest_type', 'dest_id'])

class Relationship(ABC):
    """Subclass for your relationships. You will want to use __slots__
    when possible to reduce the memory usage of each instance. If you
    do not have any properties on the relationship, just use the
    SimpleRelationship subclass.
    """
    @abstractmethod
    def get_rel_id(self):
        """Return the unique id for this relationship.
        Should be a RelId tuple
        """
        pass

    @abstractmethod
    def merge(self, other):
        """Combine two relationships with the same id.
        """
        pass

    @abstractmethod
    def to_csv_row(self):
        """Convert the relationship to a single row in the final csv. This should
        return a list of values, one per each column"""
        pass


class SimpleRelationship(Relationship, RelId):
    """This is a concrete class you can use for relationships that don't have
    any properties, just a type. In that case, the "id" of the relationship
    provides the full specification
    """
    def get_rel_id(self):
        return self

    def merge(self, other):
        assert self==other
        return self

    def to_csv_row(self):
        return [self.source_id, self.dest_id, self.rel_type]

    @staticmethod
    def get_header_row(rel_type, from_type, to_type):
        """Can use this in MapReduceTemplate.get_rel_header_row()
        if all your relationships have no properties.
        """
        return [':START_ID(%s)' % from_type,
                ':END_ID(%s)' % to_type,
                ':TYPE']


class MapReduceTemplate(ABC):
    """This abstract class contains methods which provide the necessary
    implementation needed for the map-reduce algorithm.
    """
    @abstractmethod
    def map_input(self, input):
        """Take a single input and returns (node_list, relationship_list) tuple
        """
        pass

    @abstractmethod
    def get_node_header_row(self, node_type):
        """For the specified node type, return the header row to be used by
        the ndoe csv file. See 
        https://neo4j.com/docs/operations-manual/current/tools/import/file-header-format/
        """
        pass

    @abstractmethod
    def get_rel_header_row(self, rel_type, source_type, dest_type):
        """For the specific relationship type, return the header row to be used
        by the relationship csv file.
        """
        pass

##########################################################################
#    General utilities for data cleansing
##########################################################################
NONPRINTABLE=set('\n\r\x0b\x0c').union(set([chr(i) for i in range(128)]).difference(string.printable))
XLATE={ord(character):chr(183) for character in NONPRINTABLE}
# tab  converted to space
XLATE[ord('\t')] = ' '
def cleanup_text(text):
    """Remove problematic characters for the CSV import"""
    return text.translate(XLATE) if text is not None else None

XLATE_IDS={ord(character):chr(183) for character in NONPRINTABLE}
XLATE_IDS[ord(' ')]=''
XLATE_IDS[ord('\t')]=''

def cleanup_id(text):
    """Remove problematic characters for the CSV import. Neo4j seems to ignore spaces in ids,
    so we remove them completely."""
    return text.translate(XLATE_IDS) if text is not None else None

