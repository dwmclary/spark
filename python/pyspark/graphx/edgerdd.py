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

from edge import Edge
from edgedirection import EdgeDirection
from edgetriplet import EdgeTriplet

class EdgeRDD(RDD):
    """
    Extends RDD model to edges but lacks the columnar format and partitioning
    which Scala takes advantage of.
    """

    def __init__(self, partitionsRDD):
        #initialize the RDD
        super(EdgeRDD, self).__init__(partitionsRDD._jrdd, \
            partitionsRDD.context, partitionsRDD._jrdd_deserializer)
        #set the name
        super(EdgeRDD,self).setName("pythonEdgeRDD")

    
    #without a better way to deal with edge partitioning, we'll start with just
    #exposing the API

    def mapEdgePartitions(self, f):
        return self.partitionsRDD.map(f)

    
    """ Map the values in an edge, preserving the structure, but changing the values.
    """
    def mapValues(self, f):
        """
        Tests go here
        """
        return self.partitionsRDD.map(f)
        
    

