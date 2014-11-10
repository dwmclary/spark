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


class EdgeTriplet(Object):

    srcAttr = {}
    dstAttr = {}
    attr = {}
    srcId = None
    destId = None

    """
    An edge triplet represents an edge along with the vertex attributes of its neighboring vertices.
    """
    def __init__():
    
    """
      Set the edge properties of this triplet from another edge
    """
    def _set(self, other):
        #EdgeTriplets don't seem to be referenced by Edge
        # do we need srcId and dstId?
        self.srcId = other.srcId
        self.dstId = other.dstId
        self.attr= other.attr

    """
      Given one vertex in the edge, return the attribute hash for the other vertex.

    """
    def otherVertexAttr(self, vid):
        if (vid == srcId):
            return dstAttr
        elif (vid == dstId):
            return srcAttr
        else:
            return None

    """
      Get the vertex object for the given vertex in the edge.

    """
    def vertexAttr(self, vid):
        if (srcId == vid):
            return srcAttr
        elif (dstId == vid):
            return dstAttr
        else:
            return None
            
    def __str__(self, ):
       return str(((srcId,srcAttr), (dstId, dstAttr), attr))

    
    def toTuple(self):
        return ((srcId,srcAttr), (dstId, dstAttr), attr)
        
        
        
        
