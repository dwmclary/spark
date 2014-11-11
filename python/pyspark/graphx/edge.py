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

from edgedirection import EdgeDirection

class Edge():
  """
    Base class for an edge with srcId, destID, and attr.  attr includes an
    EdgeDirection
  """
  srcId = None
  dstId = None
  attr = None
  
  """
    Initialize an edge object.
  """
  def __init__(self, srcId=0, dstId=0, attr={EdgeDirection("Out")}):
    self.srcId = srcId
    self.dstId = dstId
    self.attr = attr
  
  """
    Given one vertex in an edge, return the other vertex.
  """
  def otherVertexId(self, vid):
    """
    >>> edge = Edge(1,2)
    >>> dest = edge.otherVertexId(1)
    >>> dest == 2
    True
    """
    if (self.srcId == vid):
      return self.dstId
    elif (self.dstId == vid):
      return self.srcId
    else:
      return None
  
  """
    Return the relative direction of the edge to the corresponding vertex.
  """
  def relativeDirection(self, vid):
    """
    >>> edge = Edge(1,2)
    >>> dir = edge.relativeDirection(1)
    >>> dir == EdgeDirection("Out")
    True
    """
    if (vid == self.srcId):
      return EdgeDirection("Out")
    elif (vid == self.dstId):
      return EdgeDirection("In")
    else:
      return None

  
  def __cmp__(self, b):
    if (a.srcId == b.srcId):
      if (a.dstId == b.dstId):
        return 0
      elif (a.dstId < b.dstId):
        return -1
      else:
        return 1
    elif (a.srcId < b.srcId):
      return -1
    else:
      return 1

  ## do we need a lexigogrpahicOrdering funciton?  Probably not in the
  ## first draft, as we are more likely to invoke this from Graph

      
    
  
