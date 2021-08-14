package package2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class WriteInputFile {
    public static int N = 1000;
    public static Random rng = new Random(42);

    public static void main(String[] args) throws Exception {
        int[][] A = new int[N][N];
        int[][] B = new int[N][N];

        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                A[i][j] = rng.nextInt();
                B[i][j] = rng.nextInt();
            }
        }

        /**
         * Matrix A
         */
        File file = new File("Data/MatrixA.txt");

        FileWriter myWriter = new FileWriter(file, false);

        WriteMatrix(myWriter, A, "MatrixA");

        /**
         * Matrix B
         */
        File fileB = new File("Data/MatrixB.txt");

        FileWriter myWriterB = new FileWriter(fileB, false);

        WriteMatrix(myWriterB, B, "MatrixB");

    }

    private static void WriteMatrix(FileWriter myWriter, int[][] matrix, String name) throws IOException {
        StringBuilder out = new StringBuilder();

        for (int i = 0; i < matrix.length; i++) {
            out.append(i).append(","); // Index

            for (int Aj : matrix[i]) {
                out.append(Aj).append(" ");
            }
            out.deleteCharAt(out.length() - 1).append("\n");
        }

        myWriter.write(out.toString());
        myWriter.close();
    }

    /*
    private static void WriteMatrix(FileWriter myWriter, int[][] matrix, String name) throws IOException {
        StringBuilder out = new StringBuilder();

        out.append("" +
                "package package2;\n\n" +
                "import org.apache.flink.api.java.tuple.Tuple2;\n\n" +
                "public class " + name + " {\n" +
                "\tpublic static final Tuple2<Integer,Integer[]>[] matrix = new Tuple2[] {\n");

        for (int i = 0; i < matrix.length; i++) {
            out.append("\t\tnew Tuple2<>(").append(i).append(", new Integer[]{"); // Index

            for (int Aj : matrix[i]) {
                out.append(Aj).append(",");
            }
            out.deleteCharAt(out.length() - 1).append("}),\n");
        }
        out.deleteCharAt(out.length() - 2).append("\n\t};\n}");

        myWriter.write(out.toString());
        myWriter.close();
    }
     */
}
