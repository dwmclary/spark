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
from py4j.java_collections import MapConverter
from py4j.java_gateway import java_import, Py4JError

from pyspark.storagelevel import StorageLevel
from pyspark.serializers import PairDeserializer, NoOpSerializer, UTF8Deserializer
from pyspark.streaming import DStream


__all__ = ['FlumeUtils', 'utf8_decoder']

def utf8_decoder(s):
    """ Decode the unicode as UTF-8 """
    return s and s.decode('utf-8')

class FlumeUtils(object):

  DEFAULT_POLLING_PARALLELISM = 5
  DEFAULT_POLLING_BATCH_SIZE = 1000

  @staticmethod
  def createStream(ssc, hostname, port, storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2,
    enableDecompression=True, keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):
    java_import(ssc._jvm, "org.apache.spark.streaming.flume.FlumeUtils")
    jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
    jstream = ssc._jvm.FlumeUtils.createStream(ssc._jssc, hostname, port, jlevel)
    stream = DStream(jstream, ssc, UTF8Deserializer())
    # except Py4JError, e:
    #   print e.message

    return stream


  @staticmethod
  def createPollingStream(ssc, hostname, port,
    storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2,
    keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):

    return None
