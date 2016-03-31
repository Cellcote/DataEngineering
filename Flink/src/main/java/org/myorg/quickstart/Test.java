/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.myorg.quickstart;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
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
        DataSet<Tuple3<Long, Long, Long>> edges = env.readCsvFile(filePath)
                .fieldDelimiter(" ")
                .includeFields("1101")
                .ignoreComments("%")
                .types(Long.class, Long.class, Long.class);
        Graph<Long, Double, Long> g = Graph.fromTupleDataSet(edges, new InitVertices(0), env).getUndirected();
        Long totalTime = edges.maxBy(2).collect().get(0).f2 - edges.minBy(2).collect().get(0).f2;
        Long timeUnit = 30l * 24l * 60l * 60l;
        final Long beginTime = edges.minBy(2).collect().get(0).f2;
        Long iterations = totalTime / timeUnit;
        Graph<Long, Double, Long> beginVertices = g.filterOnEdges(new FilterFunction<Edge<Long, Long>>() {
            @Override
            public boolean filter(Edge<Long, Long> value) throws Exception {
                return value.f2 <= beginTime;
            }
        });
        List<Long> allVertices = beginVertices.getVertexIds().collect();
        List<Integer> representativeVertices = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            int randomNumber = new Random().nextInt(allVertices.size());
            representativeVertices.add(allVertices.get(randomNumber).intValue());
            allVertices.remove(randomNumber);
        }

        System.out.println(beginVertices.getVertexIds().collect().size());
        List<Vertex<Long, Double>> vertices = g.getVertices().collect();
        double[] results = new double[iterations.intValue()];
        for(int i = 0; i < iterations.intValue(); i++) {
            results[i] = 0;
        }
        for (int i = 0; i < representativeVertices.size(); i++) {
            
            for (int j = 440; j < iterations; j++) {
                final Long time = beginTime + j * timeUnit;
                Graph<Long, Double, Long> graph = Graph.fromTupleDataSet(edges, new InitVertices(vertices.get(representativeVertices.get(i)).f0), env).filterOnEdges(new FilterFunction<Edge<Long, Long>>() {
                    @Override
                    public boolean filter(Edge<Long, Long> value) throws Exception {
                        return value.f2 <= time;
                    }
                }).getUndirected();
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
                results[j] += list.get(0).f1/f.count();
            }

        }
        long numberOfVertices = g.getVertices().count();
        for(int i = 0; i < results.length; i++) {
            System.out.println(i +"\t"+results[i]/representativeVertices.size());
        }
        //System.out.println("Total:" + total/g.getVertices().count());
        //System.out.println(g.getVertices().count());

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
