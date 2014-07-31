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
class EdgeDirectionException(Exception):
  def __init__(value):
    self.value = value

  def __str__(self):
    return repr(self.value)

class EdgeDirection(Object):
  name = None

  def __init__(name):
    if name in ("In", "Out", "Either", "Both"):
      self.name = name
    else:
      raise EdgeDirectionError("Directions can only be: In, Out, Either, Both")

  def __str__(self):
    return "EdgeDirection."+name

  def __eq__(self, other):
    if self.name.equals(other.name):
      return True
    return False

  def reverse(self):
    """
      Reverse the direction of an edge.  In -> Out, Out -> In.  Either and Both are
      unchanged.
    """
    if self.name == "In":
      self.name = "Out"
    else if self.name == "Out":
      self.name = "In"
