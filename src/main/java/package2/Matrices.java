package package2;

import org.apache.flink.api.java.tuple.Tuple2;

public class Matrices {
    public static final Tuple2<Integer,Integer[]>[] A = new Tuple2[] {
            new Tuple2<>(1, new Integer[]{1, 2, 3, 4, 5, 6}),
            new Tuple2<>(2, new Integer[]{7, 8, 9, 10, 11, 12}),
            new Tuple2<>(3, new Integer[]{13, 14, 15, 16, 17, 18}),
            new Tuple2<>(4, new Integer[]{19, 20, 21, 22, 23, 24}),
            new Tuple2<>(5, new Integer[]{25, 26, 27, 28, 29, 30}),
            new Tuple2<>(6, new Integer[]{31, 32, 33, 34, 35, 36})
    };
    public static final Integer[][] B = {{11, 12, 13, 14, 15, 16}, {17, 18, 19, 20, 21, 22}, {23, 24, 25, 26, 27, 28}, {29, 30, 31, 32, 33, 34}, {35, 36, 37, 38, 39, 40}, {41, 42, 43, 44, 45, 46}};
}
