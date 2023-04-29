#!/usr/bin/env python3
"""Manage a Neo4j instance running in docker

Copyright 2020-2023 by Jeff Fischer.
Released under the BSD 3-clause license.
"""

import sys
import argparse
import os
from os.path import abspath, expanduser, join, exists, isdir, basename
import subprocess
import shutil
import glob
import json

DEFAULT_NEO4J_VERSION='latest'
DEFAULT_NEO4J_ROOT=abspath(expanduser('~/neo4j'))
DEFAULT_IMPORT_DIRECTORY=abspath(expanduser('.'))

# import directory from within the container
NEO4J_IMPORT_DIR='/var/lib/neo4j/import'

def find_exe(executable, paths):
    for path in paths:
        exe_path = join(path, executable)
        if exists(exe_path):
            return exe_path
    raise Exception(f"Did not find executable {executable} in any of {', '.join(paths)}")

DOCKER=find_exe('docker', ['/usr/local/bin', '/usr/bin', expanduser('~/.docker/bin')])

def run_docker(command_and_args):
    cmd = DOCKER + ' '+command_and_args
    print(cmd)
    try:
        cp = subprocess.run(cmd, shell=True)
        cp.check_returncode()
    except Exception as e:
        raise Exception(f"Docker failed with command: {command_and_args}") from e


def get_dirs(args):
    """Return data, log, and cid directories"""
    return (join(args.neo4j_root, 'data'),
            join(args.neo4j_root, 'logs'),
            join(args.neo4j_root, 'cid-files'),
            join(args.neo4j_root, 'conf'))

def get_conf_mount(conf_path):
    """If neo4j_root/conf exists, return the command option to mount conf. Otherwise return an empty string"""
    if exists(conf_path):
        arg = f"--volume={conf_path}:/conf"
        print(f"setting conf argument to: {arg}")
        return arg
    else:
        return ""

def get_cid(cid_dir):
    cid_file = join(cid_dir, 'neo4j.cid')
    if exists(cid_file):
        with open(cid_file, 'r') as f:
            cid = f.read().strip()
            if cid!='':
                return cid
            else:
                return None
    else:
        return None

def neo4j_running(cid_dir):
    cid = get_cid(cid_dir)
    if cid is None:
        return False
    else:
        cmd = DOCKER + " inspect -f '{{.State.Running}}' "+cid
        print(cmd)
        cp = subprocess.run(cmd, shell=True, encoding='utf-8', stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
        if cp.returncode==0:
            result = cp.stdout.strip()
            if result=='true':
                return True
            elif result=='false':
                return False
            else:
                raise Exception(f"Unexpected output from docker inspect: {result}")
        elif f"No such object: {cid}" in cp.stderr:
            return False
        else:
            print(cp.stderr)
            raise Exception("Error invoking docker inspect")

def get_user_map_args():
    return f"-u {os.getuid()}:{os.getgid()} --userns=host -v /etc/group:/etc/group:ro -v /etc/passwd:/etc/passwd:ro"

def create(args):
    (data, log, cid_dir, conf_dir) = get_dirs(args)
    if neo4j_running(cid_dir):
        raise Exception("Neo4j is running, destroy it first before attempting a create")
    # gather the import files
    node_import_files = []
    for fpath in glob.glob(join(args.import_directory, 'nodes-*.csv')):
        node_import_files.append(join(NEO4J_IMPORT_DIR, basename(fpath)))
    if len(node_import_files)==0:
        raise Exception(f"No node import files of the form nodes-*.csv found in {args.import_directory}")
    nodes_args=' '.join(['--nodes='+f for f in node_import_files])
    edge_import_files = []
    for fpath in glob.glob(join(args.import_directory, 'edges-*.csv')):
        edge_import_files.append(join(NEO4J_IMPORT_DIR, basename(fpath)))
    edges_args=' '.join(['--relationships='+f for f in edge_import_files])
    # recreate the directories
    for dirpath in [cid_dir, data, log]:
        if exists(dirpath):
            shutil.rmtree(dirpath)
        os.makedirs(dirpath)
        print(f"created directory {dirpath}")
        os.chmod(dirpath, 0o777)
    run_docker(f"pull neo4j:{ags.neo4j_version}")
    CREATE_COMMAND=f"run -it --rm --volume={data}:/data --volume={log}:/logs --volume={args.import_directory}:/var/lib/neo4j/import --env=SECURE_FILE_PERMISSIONS=no --env=NEO4J_AUTH=neo4j/{args.password} --env=NEO4J_dbms_memory_pagecache_size=1024M --env=NEO4J_dbms_memory_heap_maxSize=1024M {get_user_map_args()} neo4j:{args.neo4j_version} bin/neo4j-admin import {nodes_args} {edges_args}"
    print(CREATE_COMMAND)
    run_docker(CREATE_COMMAND)

def start(args):
    (data, log, cid_dir, conf_dir) = get_dirs(args)
    if neo4j_running(cid_dir):
        raise Exception("Neo4j is already running")
    cid_file = join(cid_dir, 'neo4j.cid')
    if exists(cid_file):
        os.remove(cid_file) # remove from a dead container
    START_COMMAND=f"run -d --rm --publish=7474:7474 --publish=7687:7687 --cidfile={cid_file}  {get_conf_mount(conf_dir)} --volume={data}:/data  --volume={log}:/logs --env=NEO4J_AUTH=neo4j/{args.password} --env=NEO4J_dbms_memory_pagecache_size=1024M --env=NEO4J_dbms_memory_heap_maxSize=1024M {get_user_map_args()} neo4j:{args.neo4j_version}"
    print(START_COMMAND)
    run_docker(START_COMMAND)

def destroy(args):
    (data, log, cid_dir, conf_dir) = get_dirs(args)
    if neo4j_running(cid_dir):
        cid = get_cid(cid_dir)
        assert cid is not None
        run_docker(f"stop {cid}")
    for dirpath in (data, log, cid_dir):
        if exists(dirpath):
            shutil.rmtree(dirpath)
    print("Destroyed neo4j instance and its data.")

def stop(args):
    (data, log, cid_dir, conf_dir) = get_dirs(args)
    if neo4j_running(cid_dir):
        cid = get_cid(cid_dir)
        assert cid is not None
        run_docker(f"stop {cid}")
        print("Neo4j stopped.")
    else:
        print("Neo4j already stopped.")

def status(args):
    if not exists(args.neo4j_root):
        print(f"Root directory for Neo4j install {args.neo4j_root} does not exist.")
        return
    (data, log, cid_dir, conf_dir) = get_dirs(args)
    if neo4j_running(cid_dir):
        cid = get_cid(cid_dir)
        print(f"Neo4j is running, container id is {cid}.")
    else:
        print("Neo4j is not running.")



def main(argv=sys.argv[1:]):
    # set defaults
    default_neo4j_root = DEFAULT_NEO4J_ROOT
    default_import_directory = DEFAULT_IMPORT_DIRECTORY
    default_neo4j_version = DEFAULT_NEO4J_VERSION
    default_password = None

    # We look for a neoctl_conf.json file in the current directory. If it exists, we use
    # that to override our defaults. The command line arguments will still override these.
    if exists('./neoctl_conf.json'):
        with open('./neoctl_conf.json', 'r') as f:
            data = json.load(f)
        if 'neo4j_root' in data:
	        default_neo4j_root = data['neo4j_root']
        if 'import_directory' in data:
            default_import_directory = data['import_directory']
        if 'neo4j_version' in data:
            default_neo4j_version = data['neo4j_version']
        if 'password' in data:
            default_password = data['password']

    # now we can parse the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--neo4j-root', default=default_neo4j_root,
                        help=f"Root directory for Neo4j files, defaults to {DEFAULT_NEO4J_ROOT}")
    parser.add_argument('--import-directory', default=default_import_directory,
                        help=f"Directory for import files, defaults to {DEFAULT_IMPORT_DIRECTORY}."+
                              " Used only when creating database.")
    parser.add_argument('--neo4j-version', default=default_neo4j_version,
                        help=f"Version (tag) of Neo4j container to use, defaults to {DEFAULT_NEO4J_VERSION}.")
    parser.add_argument('--password', default=default_password,
                        help="Password for neo4j user. Needed for create and start commands.")
    parser.add_argument('command', metavar='COMMAND', nargs='+',
                        help="Command to run, one of create, start, stop, status, or destroy")
    args = parser.parse_args(argv)

    
    args.neo4j_root = abspath(expanduser(args.neo4j_root))
    args.import_directory = abspath(expanduser(args.import_directory))
    # check commands before running them
    print(f"command: {args.command}")
    for command in args.command:
        if command=='create':
            if not exists(args.import_directory):
                parser.error(f"Import directory {args.import_directory} does not exist")
            if args.password is None:
                parser.error("Need to specify password for create command")
        elif command=='start':
            if args.password is None:
                parser.error("Need to specify password for start command")
        elif command=='stop':
            pass
        elif command=='destroy':
            pass
        elif command=='status':
            pass
        else:
            parser.error(f"Unknown command {args.command}")

    # now run the commmands
    for command in args.command:
        if command=='create':
            print("Running create...")
            create(args)
        elif command=='start':
            print("Running start...")
            start(args)
        elif command=='stop':
            stop(args)
        elif command=='destroy':
            print("Running destroy...")
            destroy(args)
        elif command=='status':
            status(args)
        else:
            assert 0

    return 0


if __name__ == '__main__':
    sys.exit(main())
