/*
 * Copyright 2014 Ryan Linneman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umkc.sce.rdf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import com.hp.hpl.jena.graph.Node;
import com.sun.tools.corba.se.idl.InvalidArgument;

/** Manages the HBase connection/{@link HBaseAdmin} */
public class HBaseStore {
    private final HBaseAdmin admin;
    private final PredicateMap predicateMap;
    private final HashMap<TableName, Partition> partitions;
    private boolean allTableNamesLoaded;

    public HBaseStore(Configuration conf) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {
        this.admin = new HBaseAdmin(conf);
        this.partitions = new HashMap<TableName, Partition>();
        this.predicateMap = new PredicateMap(admin);
    }

    public void format() throws IOException {
        ExecutorService es = Executors.newFixedThreadPool(10);

        for (TableName tableName : admin
                .listTableNamesByNamespace(getNamespace())) {
            es.execute(deleteTable(tableName));
        }
        try {
            System.out.print("Awaiting table drops to complete...");
            es.shutdown();
            es.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Runnable deleteTable(final TableName tableName) {
        return new Runnable() {

            public void run() {
                try {
                    if (admin.isTableEnabled(tableName)) {
                        admin.disableTable(tableName);
                    }
                    admin.deleteTable(tableName);
                } catch (IOException e) {
                }
            }
        };
    }

    public Partition getPartition(PartitionAxis axis, Node predicate)
            throws IOException, InvalidArgument {
        if (!predicate.isConcrete()) {
            throw new InvalidArgument("node must be a concrete node");
        }
        Partition ht = null;
        TableName tableName = getTableName(axis, predicate);
        if (partitions.containsKey(tableName)) {
            ht = partitions.get(tableName);
        } else {
            ht = new HBaseVerticalPartition(tableName, admin, predicate);

            partitions.put(tableName, ht);
        }
        return ht;
    }

    private synchronized void assertAllPartitions() throws IOException {
        if (!allTableNamesLoaded) {
            for (TableName tableName : admin.listTableNames()) {
                if (!partitions.containsKey(tableName)) {
                    Node predicate = predicateMap.get(tableName);
                    if (predicate != null) {
                        partitions.put(tableName, new HBaseVerticalPartition(
                                tableName, admin, predicate));
                    }
                }
            }
            allTableNamesLoaded = true;
        }
    }

    private TableName getTableName(PartitionAxis axis, Node node) {
        StringBuilder nameBuilder = new StringBuilder();

        nameBuilder.append("tbl-").append(getNameOfNode(node)).append("-")
                .append(axis.toString().toLowerCase());

        TableName tableName = TableName.valueOf("rdf", nameBuilder.toString());
        return tableName;
    }

    static String getNameOfNode(Node node) {
        String pred = null;
        if (node.isURI())
            pred = node.getLocalName();
        else if (node.isBlank())
            pred = node.getBlankNodeLabel();
        else if (node.isLiteral())
            pred = node.getLiteralValue().toString();
        else
            pred = node.toString();
        return pred;
    }

    public Iterable<Partition> allPartitions(PartitionAxis axis) {
        try {
            // verify all partitions have been loaded so that we have
            // accurate query scope
            assertAllPartitions();
        } catch (IOException e) {
            // failed to garner query scope so bail with no results
            return null;
        }

        List<Partition> result = new ArrayList<Partition>();
        for (TableName tableName : partitions.keySet()) {
            Partition partition = partitions.get(tableName);
            if (partition.getAxis() == axis) {
                result.add(partition);
            }
        }
        return result;
    }

    public synchronized void sync() {
        ExecutorService es = Executors.newFixedThreadPool(10);
        for (Partition partition : partitions.values()) {
            es.execute(_sync(partition));
        }

        es.shutdown();
        try {
            es.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private Runnable _sync(final Partition partition) {
        return new Runnable() {

            public void run() {
                if (partition.exists() || partition.create()) {
                    predicateMap.put(TableName.valueOf(partition.getName()),
                            partition.getPredicate());
                    partition.flush();
                }
            }
        };
    }

    private String getNamespace() {
        return "rdf";
    }
}
