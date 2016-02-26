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
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
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
        
        DataSource<Tuple1<Long>> firstVariable = env.readCsvFile(filePath)
                .fieldDelimiter(" ")
                .includeFields("1000")
                .types(Long.class);
        DataSource<Tuple1<Long>> secondVariable = env.readCsvFile(filePath)
                .fieldDelimiter(" ")
                .includeFields("0100")
                .types(Long.class);
        UnionOperator<Tuple1<Long>> vertices = firstVariable.union(secondVariable);
        DistinctOperator<Tuple1<Long>> vertices1 = vertices.distinct();
        System.out.println(vertices1.count());
//DataSet<Edge<Long, Integer>> edges = 
//
//    Graph<Long, String, Integer> graph = Graph.fromDataSet(vertices, edges, env);
    }

    @Override
    public String getDescription() {
        return "Connected Components Example";
    }
}