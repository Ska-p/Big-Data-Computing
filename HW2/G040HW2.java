import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Array;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;


import java.io.IOException;
import java.util.*;

public class G040HW2 {
    static long p = 8191;

    //  Hash function h_C which maps each vertex u in V into a color h_C(u) in  [0,C−1]
    public static int hash_function(int vertex, long a, long b, int C) {
        //h_C(vertex)=((a * vertex + b) mod p) mod C
        long prod = a * vertex;
        return (int)((prod + b) % p) % C;
    }

    public static Long CountTriangles(ArrayList<Tuple2<Integer, Integer>> edgeSet) {
        if (edgeSet.size() < 3) return 0L;
        HashMap<Integer, HashMap<Integer, Boolean>> adjacencyLists = new HashMap<>();
        for (Tuple2<Integer, Integer> edge : edgeSet) {
            int u = edge._1();
            int v = edge._2();
            HashMap<Integer, Boolean> uAdj = adjacencyLists.get(u);
            HashMap<Integer, Boolean> vAdj = adjacencyLists.get(v);
            if (uAdj == null) {
                uAdj = new HashMap<>();
            }
            uAdj.put(v, true);
            adjacencyLists.put(u, uAdj);
            if (vAdj == null) {
                vAdj = new HashMap<>();
            }
            vAdj.put(u, true);
            adjacencyLists.put(v, vAdj);
        }
        Long numTriangles = 0L;
        for (int u : adjacencyLists.keySet()) {
            HashMap<Integer, Boolean> uAdj = adjacencyLists.get(u);
            for (int v : uAdj.keySet()) {
                if (v > u) {
                    HashMap<Integer, Boolean> vAdj = adjacencyLists.get(v);
                    for (int w : vAdj.keySet()) {
                        if (w > v && (uAdj.get(w) != null)) numTriangles++;
                    }
                }
            }
        }
        return numTriangles;
    }

    public static Long CountTriangles2(ArrayList<Tuple2<Integer, Integer>> edgeSet, Tuple3<Integer, Integer, Integer> key, long a, long b, long p, int C) {
        if (edgeSet.size() < 3) return 0L;
        HashMap<Integer, HashMap<Integer, Boolean>> adjacencyLists = new HashMap<>();
        HashMap<Integer, Integer> vertexColors = new HashMap<>();
        for (int i = 0; i < edgeSet.size(); i++) {
            Tuple2<Integer, Integer> edge = edgeSet.get(i);
            int u = edge._1();
            int v = edge._2();
            if (vertexColors.get(u) == null) {
                vertexColors.put(u, (int) ((a * u + b) % p) % C);
            }
            if (vertexColors.get(v) == null) {
                vertexColors.put(v, (int) ((a * v + b) % p) % C);
            }
            HashMap<Integer, Boolean> uAdj = adjacencyLists.get(u);
            HashMap<Integer, Boolean> vAdj = adjacencyLists.get(v);
            if (uAdj == null) {
                uAdj = new HashMap<>();
            }
            uAdj.put(v, true);
            adjacencyLists.put(u, uAdj);
            if (vAdj == null) {
                vAdj = new HashMap<>();
            }
            vAdj.put(u, true);
            adjacencyLists.put(v, vAdj);
        }
        Long numTriangles = 0L;
        for (int u : adjacencyLists.keySet()) {
            HashMap<Integer, Boolean> uAdj = adjacencyLists.get(u);
            for (int v : uAdj.keySet()) {
                if (v > u) {
                    HashMap<Integer, Boolean> vAdj = adjacencyLists.get(v);
                    for (int w : vAdj.keySet()) {
                        if (w > v && (uAdj.get(w) != null)) {
                            ArrayList<Integer> tcol = new ArrayList<>();
                            tcol.add(vertexColors.get(u));
                            tcol.add(vertexColors.get(v));
                            tcol.add(vertexColors.get(w));
                            Collections.sort(tcol);
                            boolean condition = (tcol.get(0).equals(key._1())) && (tcol.get(1).equals(key._2())) && (tcol.get(2).equals(key._3()));
                            if (condition) {
                                numTriangles++;
                            }
                        }
                    }
                }
            }
        }
        return numTriangles;
    }

    // Count triangles using node coloring
    // Round 1:
    //  Create C subsets of edges, where, for 0≤i<C, the i-th subset, E(i) consist of all edges (u,v) of E such that h_C(u)=h_C(v)=i
    //  Compute the number t(i) triangles formed by edges of E(i) separately for each 0≤i<C
    // Round 2:
    //  Compute and return t_final=(C^2)*∑t(i) for (0≤i<C) as final estimate of the number of triangles in G
    public static long MR_ApproxTCwithNodeColors(JavaPairRDD<Integer, Integer> edges, int C) {

        // Randomly select the values of a between [1,p) and b between [0,p) at the beginning of each MR_ApproxTCwithNodeColors
        // so that each of the R runs uses different values for a and b
        int a = (int) (Math.random() * (p - 1) + 1);
        int b = (int) (Math.random() * p);

        JavaPairRDD<Integer, Integer> proc_edges = edges
                // Round 1 (Map phase): Given an RDD of edges, for each element (u,v) in RDD, emit (i,(u,v)) if h_C(u)=h_C(v)=i
                .flatMapToPair((element) -> {
                    HashMap<Integer, Tuple2<Integer, Integer>> edge = new HashMap<>();
                    ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>> subset = new ArrayList<>();
                    // Check if h_C(u)=h_C(v)=i
                    if (hash_function(element._1(), a, b, C) == hash_function(element._2(), a, b, C)) {
                        // If true add element(key,value)
                        edge.put(hash_function(element._2(), a, b, C), new Tuple2<>(element._1(), element._2()));
                    }
                    for (Map.Entry<Integer, Tuple2<Integer, Integer>> e : edge.entrySet()) {
                        subset.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return subset.iterator();
                })
                // Shuffle + Grouping by key: for each key i, let L_i be the set of elements (u,v) with key i, emit (i,L_i)
                .groupByKey()
                // Round 1 (Reduce Phase): for each key i, given (i, L_i) emit (0,c_i) where c_i is the number of triangles formed by the edges in L_i
                .flatMapToPair((element) -> { // <-- Reduce phase R1
                    ArrayList<Tuple2<Integer, Integer>> tr_count = new ArrayList<>();
                    for (Tuple2<Integer, Integer> e : element._2()) {
                        tr_count.add(e);
                    }
                    Long t = CountTriangles(tr_count);
                    tr_count.clear();
                    tr_count.add(new Tuple2<>(0, t.intValue()));
                    return tr_count.iterator();
                })
                // Shuffle + Grouping by key: for key 0, let L_0 be the set of t(i) computed at R1 (Reduce Phase), emit (0,L_0)
                .groupByKey()
                // Round 2 (Reduce Phase): (0, L_0) emit (0, t_final)
                .flatMapToPair((element) -> {
                    int t_final = 0;
                    for (Integer num : element._2()) {
                        t_final += num;
                    }
                    t_final = C * C * t_final;
                    ArrayList<Tuple2<Integer, Integer>> last_RDD = new ArrayList<>();
                    last_RDD.add(new Tuple2<>(0, t_final));
                    return last_RDD.iterator();
                }); // <-- Reduce Phase R2*/

        // Return the value of t_final
        Tuple2<Integer, Integer> e = proc_edges.collect().get(0);
        return e._2();
    }

    /* Task 2
     * Round 1:
     * Map: For each edge create C key-value pairs (ki,(u,v)) with i=0,1,…C−1 where each key ki is a triplet
     *      (use the scala type Tuple3<Integer,Integer,Integer>) containing the three colors hC(u),hC(v),i sorted in non-decreasing order.
     *
     * Reduce: For each key k=(x,y,z) let Lk be the list of values (i.e., edges) of intermediate pairs with key k.
     *         Compute the number tk of triangles formed by the edges of Lk whose node colors, in sorted order, are x,y,z.
     *         Note that the edges of Lk may form also triangles whose node colors are not the correct ones: e.g., (x,y,y) with y≠z.
     * HINT: Use CountTriangles2 to compute the number of triangles for each key k=(x,y,z) you can run it on the set of edges Lk.
     *
     * Round 2:
     * Compute and output the sum of all tk's determined in Round 1. It is easy to see that every triangle in the graph G
     * is counted exactly once in the sum. You can assume that the total number of tk's is small, so that they can be garthered
     * in a local structure. Alternatively, you can use some ready-made reduce method to do the sum. Both approaches are fine.
     */
    public static Long MR_ExactTC(JavaPairRDD<Integer, Integer> edges, Integer C) {
        // Randomly select the values of a between [1,p) and b between [0,p) at the beginning of each MR_ApproxTCwithNodeColors
        // so that each of the R runs uses different values for a and b
        int a = (int) (Math.random() * (p - 1) + 1);
        int b = (int) (Math.random() * p);

        // Round 1
        // Map
        JavaPairRDD<Integer, Integer> counted = edges
                .flatMapToPair((element) -> {
                    HashMap<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>> edge = new HashMap<>();
                    ArrayList<Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>> subset = new ArrayList<>();
                    // For each element, create C copies, one for each color
                    for (int i = 0; i < C; i++) {
                        int[] k = new int[]{hash_function(element._1(), a, b, C), hash_function(element._2(), a, b, C), i};
                        Arrays.sort(k);
                        edge.put(new Tuple3<>(k[0], k[1], k[2]), new Tuple2<>(element._1(), element._2()));
                    }
                    for (Map.Entry<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>> e : edge.entrySet()) {
                        subset.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return subset.iterator();
                // Grouping + Shuffle, group together elements with the same key
                })
                .groupByKey()
                // Map R2, count triangles for each key
                .flatMapToPair((element) -> {
                    ArrayList<Tuple2<Integer, Integer>> list_edges = new ArrayList<>();
                    for (Tuple2<Integer, Integer> e : element._2()) {
                        list_edges.add(e);
                    }
                    Long tr = CountTriangles2(list_edges, element._1(), a, b, p, C);
                    list_edges.clear();
                    list_edges.add(new Tuple2<>(0, tr.intValue()));
                    return list_edges.iterator();
                })
                .groupByKey()
                // Reduce R2, compute final estimate of the number of triangles
                .flatMapToPair((element) -> {
                    int t_final = 0;
                    for (Integer num : element._2()) {
                        t_final += num;
                    }
                    ArrayList<Tuple2<Integer, Integer>> last_RDD = new ArrayList<>();
                    last_RDD.add(new Tuple2<>(0, t_final));
                    return last_RDD.iterator();
                });

        Tuple2<Integer, Integer> e = counted.collect().get(0);
        return (long) e._2();
    }

    public static void main(String[] args) throws IOException {

        if (args.length != 4) { // Checking the number of the parameters
            throw new IllegalArgumentException("USAGE: num_partitions num_products country file_path");
        }

        // Spark setup
        SparkConf conf = new SparkConf(true).setAppName("G040HW1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        int C = Integer.parseInt(args[0]); // Number of partitions
        int R = Integer.parseInt(args[1]); // Number of products
        int F = Integer.parseInt(args[2]); // Value of Flag
        String file_path = args[3]; // File path

        // Reads the input graph into an RDD of strings (called rawData)
        JavaRDD<String> rawData = sc.textFile(file_path);

        // transform RDD rawData into an RDD of edges (called edges), represented as pairs of integers, partitioned into C partitions, and cached.
        JavaPairRDD<Integer, Integer> edges = rawData
                .flatMapToPair((line) -> {
                    // Parsing
                    String[] strArray = line.split(",");
                    Integer ID = Integer.parseInt(strArray[0]);
                    Integer Val = Integer.parseInt(strArray[1]);
                    ArrayList<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
                    pairs.add(new Tuple2<>(ID, Val));
                    return pairs.iterator();
                }).repartition(C).cache();


        if (F == 0) {
            System.out.println("Dataset = " + file_path);
            System.out.println("Number of Edges = " + edges.count());
            System.out.println("Number of Colors = " + C);
            System.out.println("Number of Repetitions = " + R);

            System.out.println("Approximation through node coloring");
            long timeSum = 0;
            long[] counts = new long[R];
            // Runs R times MR_ApproxTCwithNodeColors
            for (int i = 0; i < R; i++) {
                long startTime = System.currentTimeMillis();
                counts[i] = MR_ApproxTCwithNodeColors(edges, C);
                timeSum += System.currentTimeMillis() - startTime;
            }

            // Finds the median between the estimation
            Arrays.sort(counts);
            double median;
            if (counts.length % 2 == 0)
                median = ((double) counts[counts.length / 2] + (double) counts[counts.length / 2 - 1]) / 2;
            else
                median = (double) counts[counts.length / 2];

            System.out.println("- Number of triangles (median over " + R + " runs) = " + (int) median);
            System.out.println("- Running time (average over " + R + " runs) = " + timeSum / R + " ms");
        }
        else
        {
            System.out.println("OUTPUT with parameters: with ....");
            System.out.println("Dataset = " + file_path);
            System.out.println("Number of Edges = " + edges.count());
            System.out.println("Number of Colors = " + C);
            System.out.println("Number of Repetitions = " + R);
            System.out.println("Exact algorithm with node coloring");
            long[] estimates = new long[R];
            long timeSum = 0;
            for (int i = 0; i < R; i++) {
                long startTime = System.currentTimeMillis();
                estimates[i] = MR_ExactTC(edges, C);
                timeSum += System.currentTimeMillis() - startTime;
            }
            System.out.println("- Number of triangles = " + estimates[R-1]);
            System.out.println("- Running time (average over " + R + " runs) = " + timeSum / R + " ms");
        }
    }
}
