package package2;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.FileWriter;

public class SequentialJob {

    public static void main(String[] args) throws Exception {
        int N = WriteInputFile.N;

        Tuple2<Integer,Integer[]>[] A = MatrixA.matrix; // Pre computed data
        Tuple2<Integer,Integer[]>[] B = MatrixB.matrix; // Pre computed data

        int[][] C = new int[N][N];


        for (int i = 0; i < N; i++)
        {
            for (int j = 0; j < N; j++)
            {
                C[i][j] = 0;

                for (int k = 0; k < N; k++)
                {
                    C[i][j] += A[i].f1[k] * B[k].f1[j];
                }
            }
        }

        StringBuilder res = new StringBuilder();
        for (int[] row : C) {

            res = new StringBuilder(row + ",");
            for (Integer val : row) {
                res.append(val).append(" ");
            }
            res.deleteCharAt(res.length() - 1); // Remove last " "
        }

        FileWriter writer = new FileWriter("Data/written.txt", false);
        writer.write(res.toString());
    }
}
