package org.apache.flink.graph.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceNeighborsFunction;

/**
 *
 * This class implement the HITS algorithm by using flink Gelly API
 *Hyperlink-Induced Topic Search (HITS; also known as hubs and authorities) is a link analysis algorithm that rates Web pages,
 *developed by Jon Kleinberg.
 *
 */
public class HITS {

    static Graph< Long, Double, Double > mainGraph;

    static int iterationValue;


    public HITS( Graph< Long, Double, Double > myGraph, int iteration ){

        mainGraph = myGraph;
        iterationValue = iteration;
    }
    /**
     * This function compute the hub and authority values from graph
     * and update the graph in each iteration with hubValues and authorityValues
     *
     * return DataSet< Tuple3 > with IDs, Hub Values, Authority Values
     *
     */
    public static DataSet<Tuple3<Long, Double, Double>> run(){

        DataSet<Tuple2<Long, Double>> authorityValues = null;
        DataSet<Tuple2<Long, Double>> hubValues = null;

        for(int i = 0; i < iterationValue ; i++ ) {
            authorityValues = mainGraph.reduceOnNeighbors(new SumValues(), EdgeDirection.IN);

            mainGraph = mainGraph.joinWithVertices(authorityValues, new MapFunction<Tuple2<Double, Double>, Double>() {

                public Double map(Tuple2<Double, Double> value) {
                    return value.f0 = value.f1;
                }
            });

            hubValues = mainGraph.reduceOnNeighbors(new SumValues(), EdgeDirection.OUT);


            mainGraph = mainGraph.joinWithVertices(hubValues, new MapFunction<Tuple2<Double, Double>, Double>() {

                public Double map(Tuple2<Double, Double> value) {
                    return value.f0 = value.f1;
                }
            });

        }

        DataSet<Tuple3<Long, Double, Double> > HubAuthory=hubValues.join(authorityValues).where(0).equalTo(0).map(new HubAuthorityScore());
        return  HubAuthory;


    }

    /**
     * This Class have the function of map which join the hub and authority DataSets
     * to one Tuple3 dataSet
     * Example < ID , HubValue, AuthorityValue >
     * @param1 HubValueDataSet
     * @param2 AuthorityValueDatSet
     *
     * @return Tuple3DataSet
     */

    public static class HubAuthorityScore implements MapFunction<Tuple2<Tuple2<Long,Double>,Tuple2<Long,Double>>,Tuple3<Long, Double,Double>> {

        @Override
        public Tuple3<Long, Double, Double> map(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) throws Exception {
            return new Tuple3<Long, Double, Double>(value.f0.f0,value.f0.f1,value.f1.f1);
        }
    }

    // function to sum the neighbor values
    static final class SumValues implements ReduceNeighborsFunction<Double> {

        @Override
        public Double reduceNeighbors(Double firstNeighbor, Double secondNeighbor) {
            return firstNeighbor + secondNeighbor;
        }
    }
}
