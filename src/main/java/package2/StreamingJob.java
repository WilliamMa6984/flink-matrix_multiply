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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.*;
import java.util.concurrent.TimeUnit;

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
	public static int N = 20;
	public static Random rng = new Random(42);

	public static void main(String[] args) throws Exception {
		long current = System.nanoTime();
		String[] A = new String[N];
		int[][] B = new int[N][N]; // Pre compiled data

		for (int i = 0; i < N; i++) {
			StringBuilder AStr = new StringBuilder();
			AStr.append(i).append(","); // Add index of row for key
			for (int j = 0; j < N; j++) {
				AStr.append(rng.nextInt()).append(" ");
				B[i][j] = rng.nextInt();
			}
			AStr.deleteCharAt(AStr.length() - 1);

			A[i] = AStr.toString();
		}
		System.out.println(System.nanoTime() - current);

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

		// Filter input
		DataStream<String> A_str = env.fromElements(A); // Stream of read data

		DataStream<Integer[][]> arr = A_str
				// Each row in A
				.map(new MapFunction<String, Tuple2<Integer, Integer[]>>() {
					@Override
					public Tuple2<Integer, Integer[]> map(String input) throws Exception {
						// String to integer array
						String[] row = input.split(","); // Separate index and vector values

						Integer[] A_row = new Integer[N];
						String[] A_row_str = row[1].split(" ");
						for (int j = 0; j < N; j++) {
							A_row[j] = Integer.parseInt(A_row_str[j]);
						}

						Integer[] C_row = new Integer[N];

						// Matrix multiplcation
						for (int j = 0; j < N; j++) {
							int sum = 0;
							for (int k = 0; k < N; k++) {
								sum += A_row[k] * B[k][j];
							}
							C_row[j] = sum;
						}

						return new Tuple2<>(Integer.parseInt(row[0]), C_row);
					}
				})
				.keyBy(value -> value.f0)
				.countWindowAll(N)
				.aggregate(new AggregateFunction<Tuple2<Integer, Integer[]>, SortAccumulator, Integer[][]>() {
					@Override
					public SortAccumulator createAccumulator() {
						return new SortAccumulator();
					}

					@Override
					public SortAccumulator add(Tuple2<Integer, Integer[]> input, SortAccumulator acc) {
						acc.matrix.put(input.f0, input.f1);
						return acc;
					}

					@Override
					public Integer[][] getResult(SortAccumulator acc) {
						Set<Integer> keySet = acc.matrix.keySet();

						Integer[][] sorted_matrix = new Integer[N][N];
						for (Integer key : keySet) {
							sorted_matrix[key] = acc.matrix.get(key);
						}

						return sorted_matrix;
					}

					@Override
					public SortAccumulator merge(SortAccumulator a, SortAccumulator b) {
						a.matrix.putAll(b.matrix);
						return a;
					}
				});

		// Convert to string
		DataStream<String> result = arr
				.map(new MapFunction<Integer[][], String>() {
					@Override
					public String map(Integer[][] C) throws Exception {
						StringBuilder res = new StringBuilder();

						// Row number of resulting matrix
						for (int i = 0; i < N; i++) {
							for (int j = 0; j < N; j++) {
								res.append(C[i][j]).append(" ");
							}
							res.deleteCharAt(res.length() - 1); // Remove last " "
							res.append("\n");
						}

						return res.toString();
					}
				});

		// Write output to file
		final String outputPath = "Data/out";
		// https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/datastream/streamfile_sink/
		final StreamingFileSink<String> sink = StreamingFileSink
				.forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
				.withRollingPolicy(
						DefaultRollingPolicy.builder()
								.withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
								.withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
								.withMaxPartSize(1024 * 1024 * 1024)
								.build())
				.build();

		result.addSink(sink).setParallelism(1);

		// execute program
		env.execute("Matrix Multiply");
	}

	public static class SortAccumulator {
		HashMap<Integer, Integer[]> matrix = new HashMap<Integer, Integer[]>();
	}
}
