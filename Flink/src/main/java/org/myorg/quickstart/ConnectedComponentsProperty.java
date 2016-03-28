/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.myorg.quickstart;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.GroupCombineOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.ReduceEdgesFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 *
 * @author rikschreurs
 */
public class ConnectedComponentsProperty implements ProgramDescription {

    private static int maxIterations = 50;

    private static long monthFromTimestamp(long timestamp) {
        return Math.round(timestamp / (60 * 60 * 24 * 30));
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "data/facebook-wosn-links/out.facebook-wosn-links";
        //String filePath = "data/youtube-u-growth/out.youtube-u-growth";
        //String filePath = "data/flickr-growth/out.flickr-growth";

        DataSet<Tuple3<Long, Long, Long>> edges = env
                .readCsvFile(filePath)
                .fieldDelimiter(" ")
                .ignoreComments("%")
                .includeFields("1101") //1101 for facebook and youtube, 11001 for flickr
                .types(Long.class, Long.class, Long.class);

        Graph<Long, Long, Long> graph = Graph.fromTupleDataSet(edges, new InitVertices(), env).getUndirected();

        //Group edges into months
        DataSet<Tuple2<Long, Edge<Long, Long>>> timestamps = graph.groupReduceOnEdges(new EdgesFunction<Long, Long, Tuple2<Long, Edge<Long, Long>>>() {
            @Override
            public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> neighbours, Collector<Tuple2<Long, Edge<Long, Long>>> out) throws Exception {
                for (Tuple2<Long, Edge<Long, Long>> a : neighbours) {
                    long timestamp = a.f1.f2;
                    long month = monthFromTimestamp(timestamp);
                    out.collect(new Tuple2<Long, Edge<Long, Long>>(month, a.f1));
                }
            }
        }, EdgeDirection.IN);

        //Extract unique months
        List<Long> months = timestamps
                .distinct(0)
                .map(new MapFunction<Tuple2<Long, Edge<Long, Long>>, Long>() {
                    @Override
                    public Long map(Tuple2<Long, Edge<Long, Long>> t) throws Exception {
                        return t.f0;
                    }
                })
                .collect();
        Collections.sort(months);

        //For each month, calculate the number of connected components
        List<Long> counts = new ArrayList<>();
        List<Long> groupSizeMinimums = new ArrayList<>();
        List<Long> groupSizeMaximums = new ArrayList<>();
        List<Double> groupSizeAverages = new ArrayList<>();
        List<Double> groupSizeStandardDeviations = new ArrayList<>();
        for (final Long month : months) {
            DataSet<Vertex<Long, Long>> vertexId_groupId = graph
                .filterOnEdges(new FilterFunction<Edge<Long, Long>>() {
                    @Override
                    public boolean filter(Edge<Long, Long> t) throws Exception {
                        long timestamp = t.getValue();
                        return monthFromTimestamp(timestamp) == month;
                    }
                })
                .run(new ConnectedComponents<Long, Long>(10)); //(vertex ID, group ID)

            //Total number of connected components
            long ccCount = vertexId_groupId
                    .distinct(1) //all distinct group IDs
                    .count();
            counts.add(ccCount);
            
            //Group sizes
            GroupCombineOperator<Vertex<Long, Long>, Vertex<Long, Long>> groupId_groupSize = vertexId_groupId
                    .groupBy(1) //create groups of vertices
                    .combineGroup(new GroupCombineFunction<Vertex<Long, Long>, Vertex<Long, Long>>() {
                        //in: (vertex ID, group ID)
                        //out: (group ID, count)
                        @Override
                        public void combine(Iterable<Vertex<Long, Long>> itrbl, Collector<Vertex<Long, Long>> clctr) throws Exception {
                            long groupId = -1;
                            long count = 0;
                            for(Vertex<Long, Long> a : itrbl) {
                                if(groupId == -1)
                                    groupId = a.f1;
                                
                                count++;
                            }
                            clctr.collect(new Vertex<Long, Long>(groupId, count));
                        }
                    });
            
            long minGroupSize = groupId_groupSize
                    .minBy(1)
                    .collect()
                    .get(0).f1;
            groupSizeMinimums.add(minGroupSize);
            
            long maxGroupSize = groupId_groupSize
                    .maxBy(1)
                    .collect()
                    .get(0).f1;
            groupSizeMaximums.add(maxGroupSize);
            
            final double averageGroupSize = groupId_groupSize
                    .sum(1)
                    .collect()
                    .get(0).f0 / (double)ccCount;
            groupSizeAverages.add(averageGroupSize);
            
            double stdDevSum = groupId_groupSize
                    .map(new MapFunction<Vertex<Long, Long>, Double>() {
                        @Override
                        public Double map(Vertex<Long, Long> t) throws Exception {
                            return t.f1 - averageGroupSize;
                        }
                    })
                    .reduce(new ReduceFunction<Double>() {
                        @Override
                        public Double reduce(Double a, Double b) throws Exception {
                            return a + b;
                        }
                    })
                    .collect()
                    .get(0).longValue();
            double stdDev = Math.sqrt(stdDevSum / ccCount);
            groupSizeStandardDeviations.add(stdDev);
        }

        System.out.println("Connectedness");
        for (int i = 0; i < months.size(); i++) {
            System.out.println("Month " + months.get(i) +
                    " | count: " + counts.get(i) +
                    " | min: " + groupSizeMinimums.get(i) +
                    " | max: " + groupSizeMaximums.get(i) +
                    " | avg: " + groupSizeAverages.get(i) +
                    " | std: " + groupSizeStandardDeviations.get(i));
        }
    }

    @Override
    public String getDescription() {
        return "Connected Components";
    }

    public static final class InitVertices implements MapFunction<Long, Long> {

        @Override
        public Long map(Long id) {
            return id;
        }
    }
}
