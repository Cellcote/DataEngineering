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
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.FilterOperator;
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
        Double total = 0.0;
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "facebook";
        long srcId = 56143;
        DataSet<Tuple3<Long, Long, Long>> edges = env
                .readCsvFile(filePath)
                .fieldDelimiter(" ")
                .includeFields("1101")
                .types(Long.class, Long.class, Long.class);
        
        Graph<Long, Long, Long> graph = Graph.fromTupleDataSet(edges, new InitVertices(), env).getUndirected();

        //Group edges into months
        DataSet<Tuple2<Long, Edge<Long, Long>>> timestamps = graph.groupReduceOnEdges(new EdgesFunction<Long, Long, Tuple2<Long, Edge<Long, Long>>>() {
            @Override
            public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Long>>> neighbours, Collector<Tuple2<Long, Edge<Long, Long>>> out) throws Exception {
                for(Tuple2<Long, Edge<Long, Long>> a : neighbours) {                    
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
        
        //For each month, calculate the connected components
        List<Long> counts = new ArrayList<>();
        for(final Long month : months) {
            long ccCount = graph
                    .filterOnEdges(new FilterFunction<Edge<Long, Long>>() {
                        @Override
                        public boolean filter(Edge<Long, Long> t) throws Exception {
                            long timestamp = t.getValue();
                            return monthFromTimestamp(timestamp) == month;
                        }
                    })
//                    .filterOnVertices(new FilterFunction<Vertex<Long, Long>>() {
//                        @Override
//                        public boolean filter(Vertex<Long, Long> t) throws Exception {
//                            return true;
//                        }
//                    })
                    .run(new GSAConnectedComponents<Long, Long>(10))
                    .distinct(1)
                    .count();
            
            counts.add(ccCount);
//                    .print();
        }
        
        System.out.println("Connectedness");
        for(int i = 0; i < months.size(); i++) {            
            System.out.println("Month " + months.get(i) + " has " + counts.get(i) + " connected components");
        }
        
        
//        DataSet<Vertex<Long, Long>> verticesWithComponents = graph.run(new GSAConnectedComponents<Long, Long>(10));
//        verticesWithComponents.filter(new FilterFunction<Vertex<Long, Long>>() {
//            @Override
//            public boolean filter(Vertex<Long, Long> t) throws Exception {
//                return true;
//            }
//        }).print();
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
