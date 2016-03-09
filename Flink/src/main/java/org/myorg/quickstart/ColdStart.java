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

        String filePath = "facebook";

        DataSet<Tuple3<Long, Long, Long>> edges = env.readCsvFile(filePath)
                .fieldDelimiter(" ")
                .includeFields("1101")
                .types(Long.class, Long.class, Long.class);

        Graph<Long, List<Long>, Long> graph = Graph.fromTupleDataSet(edges, new MapFunction<Long, List<Long>>() {
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
                while(iterator.hasNext()) {
                    Long edgeValue = iterator.next().getValue();
                    edgeValues.add(edgeValue);
                }
                out.collect(new Tuple2<>(vertex.getId(), edgeValues));
            }
        }, EdgeDirection.IN);
        System.out.println(groupedGraph.count());
        //groupedGraph.print();
        //System.out.println(groupedGraph.count());
        groupedGraph.filter(new FilterFunction<Tuple2<Long, List<Long>>>() {
            @Override
            public boolean filter(Tuple2<Long, List<Long>> value) throws Exception {
                return value.f1.size() >= 10;
            }
        }).print();
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
