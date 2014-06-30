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
from pyspark.serializers import NoOpSerializer, CartesianDeserializer, \
    BatchedSerializer, CloudPickleSerializer, PairDeserializer, pack_long
from pyspark.join import python_join, python_left_outer_join, \
    python_right_outer_join, python_cogroup
from pyspark.statcounter import StatCounter
from pyspark.rddsampler import RDDSampler
from pyspark.storagelevel import StorageLevel

from py4j.java_collections import ListConverter, MapConverter

class VertexRDD(RDD):
    """
    Extends RDD by ensuring that there is only one entry for each vertex.
    """
    
    def __init__(self, partitionsRDD):
        #initialize the RDD
        super(VertexRDD, self).__init__(partitionsRDD._jrdd, \
            partitionsRDD.context, partitionsRDD._jrdd_deserializer)
        #set the name
        super(VertexRDD,self).setName("pythonVertexRDD")
        
        #ensure uniqueness of vertex IDs
        def _add_id(x):
            if hasattr(x, "__iter__"):
                x_id = x[0]
                try:
                    return tuple([long(x_id)]+ [i for i in x[1:]])
                except:
                    return None
            else:
                try:
                    return (long(x), x)
                except:
                    return None
        
        #this is the index
        self.partitionsRDD = partitionsRDD.map(lambda x: _add_id(x)).distinct()

    # this probably doesn't preserve partitions, it's here for api completeness
    def mapVertexPartitions(self, f):
        return self.partitionsRDD.map(f)
        
    # def filter(self, f):
    #     """
    #     >>> ftest = VertexRDD(sc.parallelize(range(100)))
    #     >>> evens = ftest.filter(lambda x: x%2 == 0)
    #     >>> evens.count() == len(filter(lambda x: x%2 == 0, range(100)))
    #     True
    #     """
    #     return self.filter(f)

    def _mapVertexIDs(self, f):
        return self.partitionsRDD.map(lambda x: f(x[0]))
    
    #helper function for getting vids
    def _vertexIDs(self):
        return self.partitionsRDD.map(lambda x: x[0])
        
    #the existing test for mapValues seems odd to me, this is a deviation
    #note that if we have multiple attributes on a vertex, all attributes will be passed to f
    def mapValues(self, f):
        """
        >>> vertices = VertexRDD(sc.parallelize(range(100)))
        >>> negatives = vertices.mapValues(lambda x: -x[0]).cache()
        >>> positives = vertices.mapValues(lambda x: x[0])
        >>> positives.union(negatives).sum()
        0
        """
        return self.partitionsRDD.map(lambda x: f(x[1:]))
        
    def diff(self, other):
        """
        >>> a = VertexRDD(sc.parallelize(range(50)))
        >>> b = VertexRDD(sc.parallelize(range(25,75)))
        >>> c = a.diff(b)
        >>> c.filter(lambda x: x == 50).count()
        0
        """
        to_rem = VertexRDD(self._vertexIDs().intersection(other._vertexIDs()))
        return VertexRDD(self.partitionsRDD.subtractByKey(to_rem.partitionsRDD).union(other.partitionsRDD.subtractByKey(to_rem.partitionsRDD)))
        
    def join(self, other):
        def clear_and_flat(v):
            g = list(v)
            new_v = [g.pop(0)]
            
            def flat_iter(v1):
                s = set()
                for i in v1:
                    if hasattr(i, "__iter__"):
                        s.update([x for x in i])
                    else:
                        s.add(i)
                return s
                
            while len(g) > 0:
                s = flat_iter(list(g.pop(0)))
                new_v += list(s)
            return tuple(new_v)
        return VertexRDD(self.partitionsRDD.join(other.partitionsRDD).map(clear_and_flat))
        
    def count(self):
        """
        >>> ftest = VertexRDD(sc.parallelize(range(100)))
        >>> ftest.count()
        100
        """
        return self.partitionsRDD.map(lambda x: 1).reduce(lambda x, y: x+y)
        
    # def take(self, n):
    #     return self.partitionsRDD.take(n)
    
    def takeOrdered(self, n, key=None):
        return self.partitionsRDD.takeOrdered(n,key)
        
    def top(self, num):
        return self.partitionsRDD.top(num)
        
    
    
        
