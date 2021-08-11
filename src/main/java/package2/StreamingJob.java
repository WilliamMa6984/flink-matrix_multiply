/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package package2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * https://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */
		DataStream<Tuple2<Integer,Integer[]>> A = env.fromElements(Matrices.A);
		Integer[][] B = Matrices.B;

		DataStream<Tuple2<Integer,String>> result = A
				.keyBy(value -> value.f0)
				// Each row in A
				.map(new MapFunction<Tuple2<Integer,Integer[]>, Tuple2<Integer,String>>() {
					@Override
					public Tuple2<Integer,String> map(Tuple2<Integer,Integer[]> A_row) throws Exception {
						Integer[] vector = new Integer[A_row.f1.length];

						for (int j = 0; j < B.length; j++) {
							Integer sum = 0;
							for (int k = 0; k < B[j].length; k++) {
								sum += A_row.f1[k] * B[k][j];
							}
							vector[j] = sum;
						}

						String res = "";
						for (Integer val : vector) {
							res += val + " ";
						}
						return new Tuple2<>(A_row.f0, res);
					}
				});

		result.print();

		// execute program
		env.execute("Matrix Multiply");
	}
}
