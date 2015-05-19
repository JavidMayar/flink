package org.apache.flink.graph.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.util.Collector;

/**
 * This program implement the HITS algorithm.
 * the result is combination of three values. first is the ID, Second is the Hub Value and third is  the Authority Value.
 */
public class HITSExample {

    public static void main(String[] args ) throws Exception {


        if(!parseParameters(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Edge<Long, Double>> edges = getEdges(env);

        Graph<Long, Double, Double> graph = Graph.fromDataSet(edges, new MapFunction<Long, Double>() {

            public Double map(Long value) throws Exception {
                return 1.0;
            }
        }, env);

        // add  graph to HITS class with iteration value.
        DataSet<Tuple3<Long, Double, Double>> hubAndAuthority = new HITS(graph,maxIterations).run();


        // print the retrieved data set.
        hubAndAuthority.print();

        env.execute("Hits Example");

    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static long numPages = 10;
    private static String edgeInputPath = null;
    private static String outputPath = null;
    private static int maxIterations = 5;

    private static boolean parseParameters(String[] args) {

        if(args.length > 0) {
            if(args.length != 3) {
                System.err.println("Usage: HITS <input edges path> <output path> <num iterations>");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            outputPath = args[1];
            maxIterations = Integer.parseInt(args[2]);
        } else {
            System.out.println("Executing HITS example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  Usage: HITS <input edges path> <output path> <num iterations>");
        }
        return true;
    }

    @SuppressWarnings("serial")
    private static DataSet<Edge<Long, Double>> getEdges(ExecutionEnvironment env) {

        if (fileOutput) {
            return env.readCsvFile(edgeInputPath)
                    .fieldDelimiter("\t")
                    .lineDelimiter("\n")
                    .types(Long.class, Long.class, Double.class)
                    .map(new Tuple3ToEdgeMap<Long, Double>());
        }

        return env.generateSequence(1, numPages).flatMap(
                new FlatMapFunction<Long, Edge<Long, Double>>() {
                    @Override
                    public void flatMap(Long key,
                                        Collector<Edge<Long, Double>> out) throws Exception {
                        int numOutEdges = (int) (Math.random() * (numPages / 2));
                        for (int i = 0; i < numOutEdges; i++) {
                            long target = (long) (Math.random() * numPages) + 1;
                            out.collect(new Edge<Long, Double>(key, target, 1.0));
                        }
                    }
                });
    }
}
