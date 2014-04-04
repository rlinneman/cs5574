/**
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

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import com.hp.hpl.jena.graph.Node;
import com.sun.tools.corba.se.idl.InvalidArgument;

public class Store {
	private static final String RDF_NAMESPACE = "rdf";
	private final HBaseAdmin admin;
	private final Configuration conf;
	private final PredicateMap predicateMap;
	private final HashMap<TableName, Table> tables = new HashMap<TableName, Table>();
	private boolean allTableNamesLoaded;

	public Store(Configuration conf) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		this.conf = conf;
		this.admin = new HBaseAdmin(conf);
		this.predicateMap = new PredicateMap(admin);
	}

	public void format() throws IOException {
		StopWatch sw = new StopWatch();
		ExecutorService es = Executors.newFixedThreadPool(10);

		sw.start();
		for (TableName tableName : admin
				.listTableNamesByNamespace(RDF_NAMESPACE)) {
			System.out.printf("Dropping %s\n", tableName.getNameAsString());
			es.execute(deleteTable(tableName));
		}
		try {
			System.out.print("Awaiting table drops to complete...");
			es.shutdown();
			es.awaitTermination(1, TimeUnit.HOURS);
			sw.stop();
			System.out.printf("%dS", sw.getTime() / 1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
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

	public Table getTable(TableAxis axis, Node predicate) throws IOException,
			InvalidArgument {
		if (!predicate.isConcrete()) {
			throw new InvalidArgument("node must be a concrete node");
		}
		Table ht = null;
		TableName tableName = getTableName(axis, predicate);
		if (tables.containsKey(tableName)) {
			ht = tables.get(tableName);
		} else {
			ht = new RdfTable(tableName, admin, predicate);

			tables.put(tableName, ht);
		}
		return ht;
	}

	private synchronized void assertAllTables() throws IOException {
		if (!allTableNamesLoaded) {
			for (TableName tableName : admin.listTableNames()) {
				if (!tables.containsKey(tableName)) {
					Node predicate = predicateMap.get(tableName);
					if (predicate != null) {
						tables.put(tableName, new RdfTable(tableName, admin,
								predicate));
					}
				}
			}
			allTableNamesLoaded = true;
		}
	}

	private TableName getTableName(TableAxis axis, Node node) {
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

	public Iterable<Table> allTables(TableAxis axis) {
		try {
			// verify all tables have been loaded so that we have
			// accurate query scope
			assertAllTables();
		} catch (IOException e) {
			// failed to garner query scope so bail with no results
			return null;
		}

		List<Table> result = new ArrayList<Table>();
		for (TableName tableName : tables.keySet()) {
			Table table = tables.get(tableName);
			if (table.getAxis() == axis) {
				result.add(table);
			}
		}
		return result;
	}

	public synchronized void sync() {
		ExecutorService es = Executors.newFixedThreadPool(10);
		for (Table table : tables.values()) {
			es.execute(
			_sync(table)
			);
		}

		es.shutdown();
		try {
			es.awaitTermination(1, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Runnable _sync(final Table table) {
		return new Runnable() {

			public void run() {
				if (table.exists() || table.create()) {
					predicateMap.put(table.getName(), table.getPredicate());
					table.flush();
				}
			}
		};
	}
}
