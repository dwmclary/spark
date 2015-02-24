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

"""
 Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 Usage: network_wordcount.py <zk> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --driver-class-path external/kafka-assembly/target/scala-*/\
      spark-streaming-kafka-assembly-*.jar examples/src/main/python/streaming/kafka_wordcount.py \
      localhost:2181 test`
"""

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: flume_wordcount.py <host> <port>"
        exit(-1)

    sc = SparkContext(appName="PythonStreamingFlumeWordCount")
    ssc = StreamingContext(sc, 10)

    host, port = sys.argv[1:]
    flumeStream = FlumeUtils.createStream(ssc, host, int(port))
    flumeStream.count().pprint()

    ssc.start()
    ssc.awaitTermination()
