#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from base64 import standard_b64encode as b64enc
import copy
from collections import defaultdict
from collections import namedtuple
from itertools import chain, ifilter, imap
import operator
import os
import sys
import shlex
import traceback
from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile
from threading import Thread
import warnings
from heapq import heappush, heappop, heappushpop

from pyspark.rdd import RDD
from pyspark.storagelevel import StorageLevel
from vertexrdd import VertexRDD

class Graph():
    """
    The Graph abstractly represents a graph with arbitrary objects
    associated with vertices and edges.  The graph provides basic
    operations to access and manipulate the data associated with
    vertices and edges as well as the underlying structure.  Like Spark
    RDDs, the graph is a functional data-structure in which mutating
    operations return new graphs.
    """

    vertices = None //a VertexRDD
    edges = None //an EdgeRDD
    triplets = None //an RDD of edge triplets

    def fromEdgeTuples(self, rawEdges, defaultValue, uniqueEdges=None, edgeStorageLevel = StorageLevel.MEMORY_ONLY, vertexStorageLevel = StorageLevel.MEMORY_ONLY):
        self.edges = EdgeRDD(rawEdges.map(lambda p: Edge(p[0], p[1])))
        self.vertices = VertexRDD(rawEdges.flatMap(lambda p: [e for e in p]).distinct())
        
    
