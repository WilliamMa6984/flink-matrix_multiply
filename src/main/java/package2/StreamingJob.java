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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
	public static int N = 10;
	public static Random rng = new Random(42);

	public static void main(String[] args) throws Exception {
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

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> A_str = env.fromElements(A); // Stream of read data

		DataStream<List<Tuple2<Integer, Integer[]>>> arr = A_str
				// Each row in A
				.map(new MapFunction<String, List<Tuple2<Integer, Integer[]>>>() {
					@Override
					public List<Tuple2<Integer, Integer[]>> map(String input) throws Exception {
						String[] row = input.split(","); // Separate index and vector values

						Integer[] vectorRaw = new Integer[N];
						String[] vectorStr = row[1].split(" ");
						for (int j = 0; j < N; j++) {
							vectorRaw[j] = Integer.parseInt(vectorStr[j]);
						}

						Integer[] vector = new Integer[N];

						for (int j = 0; j < N; j++) {
							int sum = 0;
							for (int k = 0; k < N; k++) {
								sum += vectorRaw[k] * B[k][j];
							}
							vector[j] = sum;
						}

						Tuple2<Integer, Integer[]> listRow = new Tuple2<>(Integer.parseInt(row[0]), vector);
						List<Tuple2<Integer, Integer[]>> list = new ArrayList<>(N);
						list.add(0,listRow);

						return list;
					}
				});
		arr.print();

		DataStream<List<Tuple2<Integer, Integer[]>>> arr1 = arr
				.keyBy(value -> value.get(0).f0)
				.countWindow(N)
				.reduce(new ReduceFunction<List<Tuple2<Integer, Integer[]>>>() {
					@Override
					public List<Tuple2<Integer, Integer[]>> reduce(List<Tuple2<Integer, Integer[]>> reduced, List<Tuple2<Integer, Integer[]>> inVector) throws Exception {
						reduced.add(inVector.get(0).f0, inVector.get(0));
						return reduced;
					}
				});
		arr1.print();

		// Convert to string
		DataStream<String> result = arr1
				.map(new MapFunction<List<Tuple2<Integer, Integer[]>>, String>() {
					@Override
					public String map(List<Tuple2<Integer, Integer[]>> vectorList) throws Exception {
						StringBuilder res = new StringBuilder();

						for (Tuple2<Integer, Integer[]> vectorTuple : vectorList) {
							res.append(vectorTuple.f0).append(",");
							Integer[] vector = vectorTuple.f1;

							for (Integer val : vector) {
								res.append(val).append(" ");
							}
							res.deleteCharAt(res.length() - 1); // Remove last " "
						}

						return res.toString();
					}
				}).setParallelism(1);

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
}
