# Copyright (c) The University of Edinburgh 2014-2015
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Collection of dispel4py utilities.
"""

import os.path
import sys
import traceback
from importlib import import_module

from dispel4py.workflow_graph import WorkflowGraph


def findWorkflowGraph(mod, attr):
    if attr is not None:
        # Use the named attribute
        graph = getattr(mod, attr)
    else:
        # Search for a workflow graph in the given module
        for i in dir(mod):
            attr = getattr(mod, i)
            if isinstance(attr, WorkflowGraph):
                if not hasattr(attr, "inputmappings") and not hasattr(
                    attr,
                    "outputmappings",
                ):
                    graph = attr
    return graph


def loadGraphFromFile(module_name, path, attr=None):
    from importlib import util

    # print(f'Importing {module_name} from "{path}...')
    spec = util.spec_from_file_location(module_name, path)
    # print(spec)

    module = util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    return findWorkflowGraph(module, attr)


def loadGraph(module_name, attr=None):
    """
    Loads a graph from the given module.
    """
    mod = import_module(module_name)
    return findWorkflowGraph(mod, attr)


def load_graph(graph_source, attr=None):
    # Try to load from a module
    error_message = ""
    try:
        return loadGraph(graph_source, attr)
    except ImportError:
        # It's not a module
        error_message += f'No module "{graph_source}"\n'
    except Exception:
        error_message += f"Error loading graph module:\n{traceback.format_exc()}"

    # Maybe it's a file?
    try:
        module_name = os.path.splitext(os.path.basename(graph_source))[0]
        return loadGraphFromFile(module_name, graph_source, attr)
    except OSError:
        # It's not a file
        error_message += f'No file "{graph_source}"\n'
    except Exception:
        error_message += f"Error loading graph from file:\n{traceback.format_exc()}"

    # We don't know what it is
    print(f'Failed to load graph from "{graph_source}":\n{error_message}')
    return None


from collections import deque
from itertools import chain
from sys import getsizeof


def dict_handler(d):
    return chain.from_iterable(d.items())


def total_size(o, handlers=None, verbose=False):
    """
    From: http://code.activestate.com/recipes/577504/
    Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """
    if handlers is None:
        handlers = {}
    all_handlers = {
        tuple: iter,
        list: iter,
        deque: iter,
        dict: dict_handler,
        set: iter,
        frozenset: iter,
    }
    all_handlers.update(handlers)
    seen = set()
    default_size = getsizeof(0)

    def sizeof(o):
        if id(o) in seen:
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)


import copy


def make_hash(o):
    """
    Makes a hash from a dictionary, list, tuple or set to any level, that
    contains only other hashable types (including any lists, tuples, sets, and
    dictionaries).
    """
    if isinstance(o, (set, tuple, list)):
        return hash(tuple([make_hash(e) for e in o]))

    if not isinstance(o, dict):
        return hash(o)

    new_o = copy.deepcopy(o)
    for k, v in new_o.items():
        new_o[k] = make_hash(v)

    return hash(tuple(frozenset(sorted(new_o.items()))))
