===============
Neo4j DB Utils
===============

This file contains some utilities for working with the Neo4j graph database.

neo4j_db_utils
--------------
This is a python package for building importers for the Neo4j bulk import
utility. It works with both Python 2.7 and Python 3.3+ and generates a
set of csv files in the expected format.

To use it, you create a script that imports the definitions from
import_defs.py and provides subclass implementations of ``Node`` and
``MapReduceTemplate`` (and potentially ``Relationship`` if ``SimpleRelationship``
is not sufficient for your graph.

You then define a ``main()`` function that calls ``build_import.add_command_args()``
to add in the arguments which control the output files. You typically add a
positional command line argument for the input file(s) and then parse the
arguments. Now, you call ``run()`` which does the work of generating the
csv files.

Examples
~~~~~~~~
See the ``examples`` subdirectory, which contains an import builder for a simple edge
list graph format. To run it, type::

    python simple_edge_list.py simple_edge_graph.txt

TODO
~~~~
Need to add a ``setup.py`` file to install as a package. Also need more conceptual explanation.

neo4j.sh
--------
``neo4j.sh`` is a shell script to create, start, stop, and destroy Neo4j databases
running in a Docker container.

Before the first time you use it, you need to run the following to create a Docker image
to be used by the bulk loader::

    cd ./neo4j-loader-docker
    make build

The ``neo4j.sh`` script can be invoked as follows::

    neo4j.sh create|start|stop|status|destroy [NEO4J_HOME]

The first argument is the subcommand and the second is the directory to contain the
files used by the Neo4j instance (the database files, logs, and container id files).
You can also specify ``NEO4J_HOME`` as an environment variable. In that case, you
can leave it off the command line.

When you run the ``create`` subcommand, the script expects the bulk loader csv
files to be in the current directory.

License
-------
Copyright 2018 by Jeff Fischer. This is made available
under the 3-clause BSD license.
