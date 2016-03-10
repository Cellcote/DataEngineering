/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.myorg.quickstart;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.util.Collector;

/**
 *
 * @author rikschreurs
 */
public class Test implements ProgramDescription {

    private static int maxIterations = 50;

    public static void main(String[] args) throws Exception {
        Double total = 0.0;
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "facebook";
        Long srcId = new Long("56143");
        DataSet<Tuple3<Long, Long, Long>> edges = env.readCsvFile(filePath)
                .fieldDelimiter(" ")
                .includeFields("1101")
                .types(Long.class, Long.class, Long.class);
         Graph<Long, Double, Long> g = Graph.fromTupleDataSet(edges, new InitVertices(srcId), env).getUndirected();
        List<Vertex<Long, Double>> vertices = g.getVertices().collect();
        for (int i = 0; i < 10; i++) {
            Graph<Long, Double, Long> graph = Graph.fromTupleDataSet(edges, new InitVertices(vertices.get(i).f0), env).getUndirected();
            

            Graph<Long, Double, Long> result = graph.runGatherSumApplyIteration(new CalculateDistances(), new ChooseMinDistance(), new UpdateDistance(), maxIterations);
            //Graph<Long, Double, Long> result = graph.runVertexCentricIteration(new VertexDistanceUpdater(), new MinDistanceMessenger(), maxIterations);

            DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();
            FilterOperator f = singleSourceShortestPaths.filter(new FilterFunction<Vertex<Long, Double>>() {
                @Override
                public boolean filter(Vertex<Long, Double> value) throws Exception {
                    return value.getValue() != Double.POSITIVE_INFINITY;
                }
            });
            List<Vertex<Long, Double>> list = f.sum(1).collect();
            total += list.get(0).f1;
        }
        System.out.println("Total:" + total);
        //singleSourceShortestPaths.print();

    }

    @Override
    public String getDescription() {
        return "Get All pair shortest paths Example";
    }

    @SuppressWarnings("serial")
    private static final class CalculateDistances extends GatherFunction<Double, Long, Double> {

        @Override
        public Double gather(Neighbor<Double, Long> neighbor) {
            return neighbor.getNeighborValue() + 1;
        }
    };

    @SuppressWarnings("serial")
    private static final class ChooseMinDistance extends SumFunction<Double, Long, Double> {

        public Double sum(Double newValue, Double currentValue) {
            return Math.min(newValue, currentValue);
        }
    };

    @SuppressWarnings("serial")
    private static final class UpdateDistance extends ApplyFunction<Long, Double, Double> {

        @Override
        public void apply(Double newDistance, Double oldDistance) {
            if (newDistance < oldDistance) {
                setResult(newDistance);
            }
        }
    };

    public static final class InitVertices implements MapFunction<Long, Double> {

        private final long srcId;

        public InitVertices(long srcId) {
            this.srcId = srcId;
        }

        @Override
        public Double map(Long id) {
            if (id.equals(srcId)) {
                return 0.0;
            } else {
                return Double.POSITIVE_INFINITY;
            }
        }
    }
}
