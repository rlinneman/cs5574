package edu.umkc.sce.rdf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.hadoop.hbase.client.HTable;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.util.iterator.NullIterator;
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

	final ExecutorService executor = Executors.newFixedThreadPool(7);

	public void format() throws IOException {
		StopWatch sw = new StopWatch();
		sw.start();
		for (TableName tableName : admin
				.listTableNamesByNamespace(RDF_NAMESPACE)) {
			System.out.printf("Dropping %s\n", tableName.getNameAsString());
			deleteTable(tableName);
		}
		try {
			System.out.print("Awaiting table drops to complete...");
			executor.shutdown();
			executor.awaitTermination(1, TimeUnit.HOURS);
			sw.stop();
			System.out.printf("%dS", sw.getTime() / 1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void deleteTable(final TableName tableName) {
		executor.execute(new Runnable() {

			public void run() {
				try {
					if (admin.isTableEnabled(tableName)) {
						admin.disableTable(tableName);
					}
					admin.deleteTable(tableName);
				} catch (IOException e) {
				}
			}

		});
	}

	public void CreateTable() {

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

			if (!ht.exists())
				ht.create();
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

	public synchronized void sync(){
		for(Table table : tables.values()){
			table.flush();
		}
	}
}
