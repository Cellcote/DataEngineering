/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.myorg.quickstart;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

/**
 *
 * @author rikschreurs
 */
public class APSP implements ProgramDescription {
    private static int maxIterations = 5;
    
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "facebook";

        DataSet<Tuple3<Long, Long, Long>> edges = env.readCsvFile(filePath)
                .fieldDelimiter(" ")
                .includeFields("1101")
                .types(Long.class, Long.class, Long.class);

        Graph<Long, Long, Long> graph = Graph.fromTupleDataSet(edges, new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return new Long(0);
            }
        }, env).getUndirected();
        
        Graph<Long, Long, Long> result = graph.runVertexCentricIteration(
                new VertexDistanceUpdater(), new MinDistanceMessenger(), maxIterations);
        
        DataSet<Vertex<Long, Long>> singleSourceShortestPaths = result.getVertices();
        singleSourceShortestPaths.print();

    }


    @Override
    public String getDescription() {
        return "Get All pair shortest paths Example";
    }
    
    public static final class MinDistanceMessenger extends MessagingFunction<Long, Long, Long, Long> {
		@Override
		public void sendMessages(Vertex<Long, Long> vertex) {
			for (Edge<Long, Long> edge : getEdges()) {
				sendMessageTo(edge.getTarget(), vertex.getValue() + edge.getValue());
			}
		}
	}
    	public static final class VertexDistanceUpdater extends VertexUpdateFunction<Long, Long, Long> {

		@Override
		public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {

			Long minDistance = Long.MAX_VALUE;

			for (Long msg : inMessages) {
				if (msg < minDistance) {
					minDistance = msg;
				}
			}

			if (vertex.getValue() > minDistance) {
				setNewVertexValue(minDistance);
			}
		}
	}

}
