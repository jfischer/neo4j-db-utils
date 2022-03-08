# Copyright 2018 by Jeff Fischer. Licensed under the 3-clause BSD license.
"""
Definitions for map-reduce based creation of Neo4j import files.

"""
from abc import ABCMeta, abstractmethod
from collections import namedtuple
import string
import sys
from six import add_metaclass
PYTHON_BASE_VERSION=sys.version_info[0]

##########################################################################
#    Types for the Graph Components (nodes and relationships)
##########################################################################
class MergeError(Exception):
    """Merged in reduce() if both instances have a value and they disagree"""
    pass

class ValidationError(Exception):
    """Raised if a node fails validation after merging is complete"""
    pass


@add_metaclass(ABCMeta)
class Node(object):
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

    def validate(self):
        """Optional method to validate a node after it has been reduced.
        Should throw a ValidationError exception if missing a required property, etc.
        The default implementation looks on the class object for a REQUIRED_ATTRS
        member, and if found, checks that all those attributes on the instance
        have a value.
        """
        if hasattr(self.__class__, "REQUIRED_ATTRS"):
            for attr in self.__class__.REQUIRED_ATTRS:
                value = getattr(self, attr)
                if (value is None) or value=="":
                    raise ValidationError(f"Node {self.get_node_id()} of type {self.get_node_type()} is missing a value for {attr}")

    def _merge_values(self, other, attribute):
        """Helper method for reduce() that compares an attribute value on this instance
        and the other instance. Returns the value that's present if only on one instance
        or if they agree if on both. Raises an error if on both, but disagree.
        """
        self_val = getattr(self, attribute)
        other_val = getattr(other, attribute)
        if self_val==other_val:
            return self_val
        elif other_val is None:
            return self_val
        elif self_val is None:
            return other_val
        else:
            raise MergeError(f"Unable to merge values {self_val} and {other_val} for {self.get_node_type()} attribute {attribute}")


@add_metaclass(ABCMeta)
class Relationship(object):
    """Subclass for your relationships. You will want to use __slots__
    when possible to reduce the memory usage of each instance. If you
    do not have any properties on the relationship, just use the
    SimpleRelationship subclass.
    """
    @abstractmethod
    def get_rel_id(self):
        """Return the unique id for this relationship.
        """
        pass

    @abstractmethod
    def get_source_node_type(self):
        pass

    @abstractmethod
    def get_dest_node_type(self):
        passs

    @abstractmethod
    def reduce(self, other):
        """Combine two relationships with the same id.
        """
        pass

    @abstractmethod
    def to_csv_row(self):
        """Convert the relationship to a single row in the final csv. This should
        return a list of values, one per each column"""
        pass


# One way to uniquely define relationships
# We assume that there is only one edge of a given types
# between any two nodes.
RelId = namedtuple('RelId',
                   ['source_type', 'source_id', 'rel_type', 'dest_type', 'dest_id'])

class SimpleRelationship(Relationship, RelId):
    """This is a concrete class you can use for relationships that don't have
    any properties, just a type. In that case, the "id" of the relationship
    provides the full specification.
    """
    def get_rel_id(self):
        return self

    def reduce(self, other):
        assert self==other
        return self

    def to_csv_row(self):
        return [self.source_id, self.dest_id, self.rel_type]

    def get_source_node_type(self):
        return self.source_type

    def get_dest_node_type(self):
        return self.dest_type

    def get_rel_type(self):
        # TODO: check whether this needs to be on metaclass - used to get file key in build_import
        return self.rel_type
    
    @staticmethod
    def get_header_row(rel_type, from_type, to_type):
        """Can use this in MapReduceTemplate.get_rel_header_row()
        if all your relationships have no properties.
        """
        return [':START_ID(%s)' % from_type,
                ':END_ID(%s)' % to_type,
                ':TYPE']


@add_metaclass(ABCMeta)
class MapReduceTemplate(object):
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
if PYTHON_BASE_VERSION==3:
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
else: # The translate() method behaves a little differently in python 2
    DELETECHARS=''.join(NONPRINTABLE)+'\t'
    def cleanup_text(text):
        """Remove problematic characters for the CSV import"""
        return text.translate(None, DELETECHARS) if text is not None else None

    IDDELETECHARS=DELETECHARS+' '
    def cleanup_id(text):
        """Remove problematic characters for the CSV import. Neo4j seems to ignore spaces in ids,
        so we remove them completely."""
        return text.translate(NONE, IDDELETECHARS) if text is not None else None
