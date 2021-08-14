package package2;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class ReadMatrixTextFile {
    // https://www.baeldung.com/java-csv-file-array
    /*public static Tuple2<Integer,Integer[]>[] Read(String filename) {
        Tuple2<Integer,Integer[]>[] records = new Tuple2<Integer, Integer[]>[];

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return records;
    }*/
}
