/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.myorg.quickstart;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceEdgesFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

/**
 *
 * @author rikschreurs
 */
public class ColdStart implements ProgramDescription {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "youtube";

        DataSet<Tuple3<Long, Long, Long>> edges = env.readCsvFile(filePath)
                .fieldDelimiter(" ")
                .ignoreComments("%")
                .includeFields("1101")
                .types(Long.class, Long.class, Long.class);
        //System.out.println(edges.maxBy(2).collect().get(0).f2 - edges.minBy(2).collect().get(0).f2);
        //System.out.println(edges.maxBy(2).collect().get(0).f2 + " - " + edges.minBy(2).collect().get(0).f2);
        Long totalTime = edges.maxBy(2).collect().get(0).f2 - edges.minBy(2).collect().get(0).f2;
        Long timeUnit = 30l * 24l * 60l * 60l;
        Long beginTime = edges.minBy(2).collect().get(0).f2;
        Long iterations = totalTime / timeUnit;
        //System.out.println(iterations);
        //System.out.println(edges.maxBy(2).collect().get(0).f2);
        double[] results = new double[iterations.intValue()];
        for (int i = 0; i < iterations; i++) {
            results[i] = 0l;
            final Long filterTime = + i * timeUnit;
            DataSet<Tuple3<Long, Long, Long>> localEdges = edges.filter(new FilterFunction<Tuple3<Long, Long, Long>>() {
                @Override
                public boolean filter(Tuple3<Long, Long, Long> value) throws Exception {
                    return value.f2 <= filterTime;
                }
            });
            System.out.println(localEdges.count());
            Graph<Long, List<Long>, Long> graph = Graph.fromTupleDataSet(localEdges, new MapFunction<Long, List<Long>>() {
                @Override
                public List<Long> map(Long value) throws Exception {
                    return new ArrayList<>();
                }
            }, env).getUndirected();
            DataSet<Tuple2<Long, List<Long>>> groupedGraph = graph.groupReduceOnEdges(new EdgesFunctionWithVertexValue<Long, List<Long>, Long, Tuple2<Long, List<Long>>>() {
                @Override
                public void iterateEdges(Vertex<Long, List<Long>> vertex, Iterable<Edge<Long, Long>> edgeIterable, Collector<Tuple2<Long, List<Long>>> out) throws Exception {
                    List<Long> edgeValues = new ArrayList();
                    Iterator<Edge<Long, Long>> iterator = edgeIterable.iterator();
                    while (iterator.hasNext()) {
                        Long edgeValue = iterator.next().getValue();
                        edgeValues.add(edgeValue);
                    }
                    edgeValues.sort(new Comparator<Long>() {
                        @Override
                        public int compare(Long o1, Long o2) {
                            return o1.compareTo(o2);
                        }
                    });
                    if (edgeValues.size() > 0) {
                        long reduction = edgeValues.get(0);
                        for (int i = 0; i < edgeValues.size(); i++) {
                            edgeValues.set(i, edgeValues.get(i) - reduction);
                        }
                    }
                    out.collect(new Tuple2<>(vertex.getId(), edgeValues));
                }
            }, EdgeDirection.IN);
            System.out.println(groupedGraph.count());
            
            groupedGraph.filter(new FilterFunction<Tuple2<Long, List<Long>>>() {
                @Override
                public boolean filter(Tuple2<Long, List<Long>> value) throws Exception {
                    return value.f1.size() >= 10;
                }
            });
            List<Tuple2<Long, List<Long>>> list = groupedGraph.collect();
            for(int j = 0; j < list.size(); j++) {
                results[i] += (double) list.get(j).f1.size()/ (double)list.size();
            }
            
        }
        for(int i = 0; i < results.length; i++) {
            System.out.println("Month " + i + ": "+ results[i]);
        }

        //System.out.println(groupedGraph.count());
        //DataSet<Tuple2<Long, Long>> friends = graph.reduceOnEdges(new SelectMinWeight(), EdgeDirection.IN);
        //System.out.println(friends.count());
        //friends.print();
    }

    @Override
    public String getDescription() {
        return "Connected Components Example";
    }
}
