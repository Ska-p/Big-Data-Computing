import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import shapeless.Tuple;

import java.util.*;
import java.util.concurrent.*;

/**
 * server: algo.dei.unipd.it
 * port: 8888
 * <p>
 * Your program will define a Spark Streaming context that accesses the stream through the method socketTextStream which
 * transforms the input stream (coming from the specified machine and port number) into DStream (Discretized Stream)
 * of batches of items arrived during a time interval whose duration is specified at the creation of the context.
 * <p>
 * A method foreachRDD is then invoked to process the batches one after the other. Each batch is seen as an RDD and
 * a set of RDD methods are available to process it.
 * <p>
 * Typically, the processing of a batch entails the update of some data structures stored in the driver's local space
 * (i.e., its working memory) which are needed to perform the required analysis.
 * The beginning/end of the stream processing will be set by invoking start/stop methods from the context sc.
 * <p>
 * For the homework, the stop command will be invoked after (approximately) 10M items have been read.
 * The threshold 10M will be hardcoded as a constant in the program.
 */
public class G040HW3 {

    // Threshold for number of items to process
    public static final int THRESHOLD = 10000000;

    static long p = 8191;

    // Function to sort hashmap by values
    // code from https://www.geeksforgeeks.org/sorting-a-hashmap-according-to-values/ with few modifications
    public static HashMap<Long, Long> sortByValue(HashMap<Long, Long> hm) {
        //  Create a list from elements of HashMap
        List<Map.Entry<Long, Long>> list = new LinkedList<>(hm.entrySet());

        //  Sort the list
        list.sort((o1, o2) -> -1 * (o1.getValue()).compareTo(o2.getValue()));

        //  Put data from sorted list to hashmap
        HashMap<Long, Long> temp = new LinkedHashMap<>();
        for (Map.Entry<Long, Long> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }

    // Hash function for column index and g_function value computation
    public static int hash_function(long x, long a, long b, int C) {
        // If g is computed, input C = 2 retrieves values in [0, 1], if returned value is 0 it is then mapped to -1
        // If h is computed, input C = W retrieves values in [0, W-1]
        long prod = a * x;
        return (int) ((prod + b) % p) % C;
    }

    // Function to estimate the second moment, give in input the count sketch matrix
    public static long calculateEstimatedSecondMoment(long[][] counters, int D, int W) {
        long[] rowsSquaredSum = new long[D];    //  Array to store all rows squared sum values
        long rowSquadedSum;
        //  Compute the estimated second moment for each row
        for (int i = 0; i < D; i++) {
            rowSquadedSum = 0;
            for (int j = 0; j < W; j++) {
                rowSquadedSum += (counters[i][j]) * (counters[i][j]);
            }
            rowsSquaredSum[i] = rowSquadedSum;
        }
        //  Sort array and later extract the median value
        Arrays.sort(rowsSquaredSum);
        long median;
        if (rowsSquaredSum.length % 2 == 0)
            median = (rowsSquaredSum[rowsSquaredSum.length / 2] + rowsSquaredSum[rowsSquaredSum.length / 2 - 1]) / 2;
        else
            median = rowsSquaredSum[rowsSquaredSum.length / 2];
        return median;
    }

    // Function to compute the estimated frequencies for the top_K elements, given in inout the count sketch matrix
    // The estimated value is the median of the values retrieved from the count sketch
    public static HashMap<Long, Tuple2<Long, Long>> ComputeEstimatedFrequencies(Map<Long, Tuple2<Long, Long>> top_k, long[][] count_sketch, int D, int W,
                                                                                int[] a_h, int[] b_h, int[] a_g, int[] b_g) {
        long[] frequency_value; // Array to store the various frequencies estimates for the elements
        HashMap<Long, Tuple2<Long, Long>> estimated_frequencies = new HashMap<>();
        for (Map.Entry<Long, Tuple2<Long, Long>> e : top_k.entrySet()) { // <Item, <exact_freq, estimated_freq>>
            frequency_value = new long[D]; // Array to store the estimated frequencies of the single element
            for (int j = 0; j < D; j++) {
                // Get the corresponding estimated value for the specified element
                frequency_value[j] = count_sketch[j][hash_function(e.getKey(), a_h[j], b_h[j], W)] * (hash_function(e.getKey(), a_g[j], b_g[j], 2) * 2 - 1);
            }
            // Extract the estimated frequency from frequency_value. The estimated one is the median among all estimates
            Arrays.sort(frequency_value);
            long median;
            if (frequency_value.length % 2 == 0)
                median = (frequency_value[frequency_value.length / 2] + frequency_value[(frequency_value.length / 2) - 1]) / 2;
            else
                median = frequency_value[frequency_value.length / 2];
            estimated_frequencies.put(e.getKey(), new Tuple2<>(e.getValue()._1, median));
        }
        return estimated_frequencies;
    }

    // Function to compute the average relative error between the exact frequencies and the estimated ones
    // Recall that if f~u is the estimated frequency for u, the relative error of is |fu−f~u|/fu
    public static float calculateAvgRelativeError(Map<Long, Tuple2<Long, Long>> top_k_estimated_frequencies) {
        float sum_relative_error = 0L;
        for (Map.Entry<Long, Tuple2<Long, Long>> e : top_k_estimated_frequencies.entrySet()) {
            sum_relative_error += Math.abs((float) e.getValue()._1 - (float) e.getValue()._2) / ((float) e.getValue()._1);
        }
        return (sum_relative_error / top_k_estimated_frequencies.size());
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 6) { // Checking the number of the parameters
            throw new IllegalArgumentException("USAGE: num_rows num_cols left_endpoint right_endpoint top_frequent portExp");
        }

        // IMPORTANT: when running locally, it is *fundamental* that the
        // `master` setting is "local[*]" or "local[n]" with n > 1, otherwise
        // there will be no processor running the streaming computation and your
        // code will crash with an out of memory (because the input keeps accumulating).
        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]") // remove this line if running on the cluster
                .setAppName("G040HW3");

        // Here, with the duration you can control how large to make your batches.
        // Beware that the data generator we are using is very fast, so the suggestion
        // is to use batches of less than a second, otherwise you might exhaust the
        // JVM memory.
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        sc.sparkContext().setLogLevel("ERROR");

        // TECHNICAL DETAIL:
        // The streaming spark context and our code and the tasks that are spawned all
        // work concurrently. To ensure a clean shut down we use this semaphore.
        // The main thread will first acquire the only permit available and then try
        // to acquire another one right after spinning up the streaming computation.
        // The second tentative at acquiring the semaphore will make the main thread
        // wait on the call. Then, in the `foreachRDD` call, when the stopping condition
        // is met we release the semaphore, basically giving "green light" to the main
        // thread to shut down the computation.
        // We cannot call `sc.stop()` directly in `foreachRDD` because it might lead
        // to deadlocks.
        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        int D = Integer.parseInt(args[0]); // number of rows of the count sketch
        int W = Integer.parseInt(args[1]); // number of columns of the count sketch
        int left = Integer.parseInt(args[2]); // left endpoint of the interval of interest
        int right = Integer.parseInt(args[3]); // right endpoint of the interval of interest
        int K = Integer.parseInt(args[4]); // number of top frequent items of interest
        int portExp = Integer.parseInt(args[5]); // port number

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long[] streamLength = {0L}; // Stream length (an array to be passed by reference)
        long[] filteredStreamLength = {0L}; // Stream length (an array to be passed by reference)
        HashMap<Long, Long> exactFrequencies = new HashMap<>(); // Hash Table for the distinct elements
        long[][] counters = new long[D][W]; // D rows and W columns initialized to 0s by default
        long estimatedTrueSecondMoment;
        float avgRelativeError;

        //  Generate random a and b values for the hash functions
        int[] a_h = new int[D];
        int[] b_h = new int[D];
        int[] a_g = new int[D];
        int[] b_g = new int[D];
        for (int i = 0; i < D; i++) {
            a_h[i] = (int) (Math.random() * (p - 1) + 1);
            b_h[i] = (int) (Math.random() * (p - 1));
            a_g[i] = (int) (Math.random() * (p - 1) + 1);
            b_g[i] = (int) (Math.random() * (p - 1));
        }

        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                // For each batch, to the following.
                // BEWARE: the `foreachRDD` method has "at least once semantics", meaning
                // that the same data might be processed multiple times in case of failure.
                .foreachRDD((batch, time) -> {
                    // This is working on the batch at time `time`.
                    long batchSize = batch.count();
                    streamLength[0] += batchSize;
                    Map<Long, Long> batchItems = batch.filter(value -> {    //  Filter data according to rules
                                long number = Long.parseLong(value);
                                return number >= left && number <= right;
                            })
                            // Map R1: map each element as follows, element -> (element, 1L)
                            .flatMapToPair((value) -> {
                                ArrayList<Tuple2<Long, Long>> pair = new ArrayList<>();
                                pair.add(new Tuple2<>(Long.parseLong(value), 1L));
                                return pair.iterator();
                            })
                            // Grouping + Shuffle
                            .groupByKey()
                            // Reduce R1: Compute number of occurrences in batch for each distinct element
                            .flatMapToPair((pair) -> {
                                long sum = 0;
                                for (long val : pair._2()) {
                                    sum += val;
                                }
                                ArrayList<Tuple2<Long, Long>> item_occ = new ArrayList<>();
                                item_occ.add(new Tuple2<>(pair._1(), sum));
                                return item_occ.iterator();
                            })
                            .collectAsMap();

                    for (Map.Entry<Long, Long> pair : batchItems.entrySet()) {
                        // Update the global structure with the exact frequencies of each item
                        exactFrequencies.put(pair.getKey(), pair.getValue() + exactFrequencies.getOrDefault(pair.getKey(), 0L));
                        for (int k = 0; k < D; k++) {
                            // Update the count sketch in position [k][H_j] with the value of the frequencies in the batch of the specific item
                            // and multiply it by the hash function g
                            counters[k][hash_function(pair.getKey(), a_h[k], b_h[k], W)] += pair.getValue() * (hash_function(pair.getKey(), a_g[k], b_g[k], 2) * 2 - 1);
                        }
                    }
                    if (batchSize > 0) {
                        System.out.println("Batch size at time [" + time + "] is: " + batchSize);
                    }
                    if (streamLength[0] >= THRESHOLD) {
                        stoppingSemaphore.release();
                    }
                });

        // MANAGING STREAMING SPARK CONTEXT
        System.out.println("Starting streaming engine");
        sc.start();
        System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        System.out.println("Stopping the streaming engine");
        // NOTE: You will see some data being processed even after the
        // shutdown command has been issued: This is because we are asking
        // to stop "gracefully", meaning that any outstanding work
        // will be done.
        sc.stop(false, true);
        System.out.println("Streaming engine stopped");

        // COMPUTE AND PRINT FINAL STATISTICS
        System.out.println("****** OUTPUT ******");
        System.out.println("D = " + D + " W = " + W + " [left,right] = [" + left + "," + right + "] K = " + K + " Port = " + portExp);
        System.out.println("Total number of items = " + streamLength[0]);
        int count_occ = 0;
        for (Map.Entry<Long, Long> e : exactFrequencies.entrySet()) {
            count_occ += e.getValue();
        }
        System.out.println(String.format("Total number of items in [%d, %d] = %d", left, right, count_occ));
        System.out.println(String.format("Number of distinct items in [%d, %d] = %d", left, right, exactFrequencies.size()));

        // Extract top K items occurrences
        Map<Long, Long> sorted_exact_frequencies = sortByValue(exactFrequencies);
        HashMap<Long, Tuple2<Long, Long>> top_k = new HashMap<>();
        // Since the Map is already ordered, we seek for the first K items of the Map
        int cont = 1;
        long KElementFrequency = 0L;
        for (Map.Entry<Long, Long> e : sorted_exact_frequencies.entrySet()) {
            if (cont < K) { // Retrieve first K items
                top_k.put(e.getKey(), new Tuple2<>(e.getValue(), 0L));
            } else if (cont == K) { // Once we reach K-th item we save its frequency
                KElementFrequency = e.getValue();
                top_k.put(e.getKey(), new Tuple2<>(e.getValue(), 0L));
            } else if (e.getValue() == KElementFrequency) { // If the frequency of the element after K is equal to the K-th, add the element
                top_k.put(e.getKey(), new Tuple2<>(e.getValue(), 0L));
            } else { // Otherwise, stop saving items
                break;
            }
            cont++;
        }

        // Compute the estimated frequencies values
        HashMap<Long, Tuple2<Long, Long>> top_k_estimated_frequencies = ComputeEstimatedFrequencies(top_k, counters, D, W, a_h, b_h, a_g, b_g);
        if (K <= 20) {
            for (Map.Entry<Long, Tuple2<Long, Long>> e : top_k_estimated_frequencies.entrySet()) {
                System.out.println("Item " + e.getKey() + " Freq. = " + e.getValue()._1() + " Est. Freq. = " + e.getValue()._2());
            }
        }

        // Compute avg relative error between estiamted frequencies and exact frequencies
        avgRelativeError = calculateAvgRelativeError(top_k_estimated_frequencies);
        System.out.println(String.format("Avg err for top %d = ", K) + avgRelativeError);

        // Compute the true second moment and the estimated second moment
        long second_moment_est = 0;
        for (Map.Entry<Long, Long> e : exactFrequencies.entrySet()) {
            second_moment_est += e.getValue() * e.getValue();
        }

        float second_moment_est_norm = (float) second_moment_est / (float) (Math.pow(count_occ, 2));
        estimatedTrueSecondMoment = calculateEstimatedSecondMoment(counters, D, W);
        float estimatedTrueSecondMoment_ = (float) estimatedTrueSecondMoment / (float) (Math.pow(count_occ, 2));
        System.out.println("F2 " + second_moment_est_norm + " F2 estimate " + estimatedTrueSecondMoment_);
    }
}