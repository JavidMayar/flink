package org.apache.flink.graph.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.util.Collector;

public class BranchingExample {

    static int maxIterations;

    public static void main(String[] args)  throws Exception{

        maxIterations = 2;

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Edge<Long, Double>> links = getEdgesDataSet(env);

        Graph<Long, Double, Double> network = Graph.fromDataSet(links, new MapFunction<Long, Double>() {

            public Double map(Long value) throws Exception {
                return 1.0;
            }
        }, env);
        network=network.getUndirected();
        network= network.runVertexCentricIteration(new updateVertex(), new  updateMessage(), maxIterations);
//        network.getEdges().print();
        network.getVertices().print();
        env.execute();
    }
    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************


    @SuppressWarnings("serial")
    private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {

        return env.generateSequence(1, 1).flatMap(
                new FlatMapFunction<Long, Edge<Long, Double>>() {
                    @Override
                    public void flatMap(Long key, Collector<Edge<Long, Double>> out) throws Exception {
                        out.collect(new Edge<Long, Double>(1l, 2l, 1.0));
                        out.collect(new Edge<Long, Double>(2l, 1l, 1.0));
                        out.collect(new Edge<Long, Double>(2l, 3l, 1.0));
                        out.collect(new Edge<Long, Double>(3l, 1l, 1.0));
                        out.collect(new Edge<Long, Double>(3l, 2l, 1.0));

                    }
                });
    }

    private static class updateVertex extends VertexUpdateFunction<Long, Double, Double>  {
        @Override
        public void updateVertex(Long aLong, Double aLong2, MessageIterator<Double> inMessages) throws Exception {
            if (getSuperstepNumber() % 2 == 0) {

                setNewVertexValue(aLong2 + 5);

            } else {

                setNewVertexValue(aLong2+10);

            }
        }
    }

    private static class updateMessage extends MessagingFunction<Long, Double, Double, Double> {
        @Override
        public void sendMessages(Long aLong, Double aLong2) throws Exception {
            sendMessageToAllNeighbors(aLong2);
        }
    }
}

