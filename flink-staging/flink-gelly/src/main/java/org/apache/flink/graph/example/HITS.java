package org.apache.flink.graph.example;


import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

/**
 *
 * This class implements the HITS algorithm by using flink Gelly API
 *Hyperlink-Induced Topic Search (HITS; also known as hubs and authorities) is a link analysis algorithm that rates Web pages,
 *developed by Jon Kleinberg.
 *
 * The algorithm performs a series of iterations, each consisting of two basic steps:
 *
 *Authority Update: Update each node's Authority score to be equal to the sum of the Hub Scores of each node that points to it.
 * That is, a node is given a high authority score by being linked from pages that are recognized as Hubs for information.
 *Hub Update: Update each node's Hub Score to be equal to the sum of the Authority Scores of each node that it points to.
 * That is, a node is given a high hub score by linking to nodes that are considered to be authorities on the subject.
 *
 * The Hub score and Authority score for a node is calculated with the following algorithm:
 *  Start with each node having a hub score and authority score of 1.
 *  Run the Authority Update Rule
 *  Run the Hub Update Rule
 *  Repeat from the second step as necessary.
 *
 * http://en.wikipedia.org/wiki/HITS_algorithm
 *
 */
public class HITS<K> implements GraphAlgorithm<K, Double, Double> {

    private int maxIterations;
    Hits hubAuthoritySelection;

    public HITS(int maxValue, Hits hubAuthority){

        maxIterations = maxValue;
        hubAuthoritySelection = hubAuthority;

    }

    /**
     * This function return a graph with hub or Authority values by hubAuthoritySelection.
     */
    @Override
    public Graph<K, Double, Double> run(Graph<K, Double, Double> network) throws Exception {
        switch (hubAuthoritySelection)
        {
            case HUB:
                    if(maxIterations%2==1){  maxIterations-=1;   }
                    break;
            case AUTHORITY:
                    if(maxIterations%2==0){  maxIterations-=1;   }
                    break;
        }

        return (network.getUndirected()).runVertexCentricIteration(new VertexRankUpdater<K>(), new RankMessenger<K>(), maxIterations);
    }



    /**
     * Function that updates the rank of a vertex by summing up the partial
     * ranks from all incoming messages.
     *
     */


    @SuppressWarnings("serial")
    public static final class VertexRankUpdater<K> extends VertexUpdateFunction<K, Double, Double> {

        public void updateVertex(K vertexKey, Double vertexValue, MessageIterator<Double> inMessages) {
            double sum= 0.0;

            if(getSuperstepNumber()%2==0){

                // implement hub code here  ?

                /*
                 sum = 0.0;
			     for (double msg : inMessages) {
				    sum += msg;

			     }
                 */

            }
            else {

                // implement Authority code here  ?

                /*
                 sum = 0.0;
			     for (double msg : inMessages) {
				    sum += msg;

			     }
                 */

            }
            setNewVertexValue(sum);
            }
        }


    /**
     * Distributes the rank of a vertex among all target vertices.
     */
    @SuppressWarnings("serial")
    public static final class RankMessenger<K> extends MessagingFunction<K, Double, Double, Double> {

        @Override
        public void sendMessages(K vertexId, Double newRank) {
            sendMessageToAllNeighbors( newRank );
        }
    }
}

/**
 * This enum is used to select authority or hub value to be returned by program.
 */

enum Hits {
    HUB(0), AUTHORITY(1);

    private int value;

    private Hits(int i) {

        this.value = i;

    }
}

