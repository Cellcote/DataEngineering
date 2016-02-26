/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.myorg.quickstart;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.ConnectedComponentsDefaultData;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.types.NullValue;

/**
 *
 * @author rikschreurs
 */
public class ConnectedComponents implements ProgramDescription {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "facebook";

        DataSet<Tuple3<Long, Long, Long>> edges = env.readCsvFile(filePath)
                .fieldDelimiter(" ")
                .includeFields("1101")
                .types(Long.class, Long.class, Long.class);

        Graph<Long, NullValue, Long> graph = Graph.fromTupleDataSet(edges, env);
        System.out.println(graph.subgraph(
                new FilterFunction<Vertex<Long, NullValue>>() {
            @Override
            public boolean filter(Vertex<Long, NullValue> vertex) {
                return true;
            }
        },
                new FilterFunction<Edge<Long, Long>>() {
            @Override
            public boolean filter(Edge<Long, Long> edge) {
                return edge.getValue() > 0;
            }
        }).numberOfEdges());
    }

    @Override
    public String getDescription() {
        return "Connected Components Example";
    }
}
