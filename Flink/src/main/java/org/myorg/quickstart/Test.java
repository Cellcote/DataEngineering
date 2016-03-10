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
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;

/**
 *
 * @author rikschreurs
 */
public class Test implements ProgramDescription {
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
        
        Graph<Long, Long, Long> result = graph.runGatherSumApplyIteration(new CalculateDistances(), new ChooseMinDistance(), new UpdateDistance(), maxIterations);
        
        DataSet<Vertex<Long, Long>> singleSourceShortestPaths = result.getVertices();
        singleSourceShortestPaths.print();

    }


    @Override
    public String getDescription() {
        return "Get All pair shortest paths Example";
    }
    
    @SuppressWarnings("serial")
	private static final class CalculateDistances extends GatherFunction<Long, Long, Long> {

                @Override
		public Long gather(Neighbor<Long, Long> neighbor) {
			return neighbor.getNeighborValue() + 1;
		}
	};

	@SuppressWarnings("serial")
	private static final class ChooseMinDistance extends SumFunction<Long, Long, Long> {

                @Override
		public Long sum(Long newValue, Long currentValue) {
			return Math.min(newValue, currentValue);
		}
	};

	@SuppressWarnings("serial")
	private static final class UpdateDistance extends ApplyFunction<Long, Long, Long> {

                @Override
		public void apply(Long newDistance, Long oldDistance) {
			if (newDistance < oldDistance) {
				setResult(newDistance);
			}
		}
	}
}
