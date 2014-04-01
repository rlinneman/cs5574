package edu.umkc.sce.rdf;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import com.hp.hpl.jena.graph.BulkUpdateHandler;
import com.hp.hpl.jena.graph.Capabilities;
import com.hp.hpl.jena.graph.GraphEventManager;
import com.hp.hpl.jena.graph.GraphStatisticsHandler;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.TransactionHandler;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.shared.AddDeniedException;
import com.hp.hpl.jena.shared.DeleteDeniedException;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;

public class Graph implements com.hp.hpl.jena.graph.Graph {

	private String name;
	private HBaseAdmin admin;
	private Store store;
	private PrefixMapping prefixMapping;
	private TransactionHandler transactionHandler;
	private GraphEventManager graphEventManager;

	public Graph(Store store, HBaseAdmin admin) {
		this.store = store;
		this.prefixMapping = new edu.umkc.sce.rdf.PrefixMapping(null, null);
		this.transactionHandler = new TxHandler(this);
		this.graphEventManager = new GraphEventMgr();
		this.admin = admin;
		System.out.println("CTOR");
	}

	public boolean dependsOn(com.hp.hpl.jena.graph.Graph other) {
		System.out.println("dependsOn");
		return false;
	}

	public TransactionHandler getTransactionHandler() {
		System.out.println("txHandler");
		return transactionHandler;
	}

	public BulkUpdateHandler getBulkUpdateHandler() {
		System.out.println("bulk upd handler");
		return null;
	}

	public Capabilities getCapabilities() {
		System.out.println("capabilities");
		return null;
	}

	public GraphEventManager getEventManager() {
		System.out.println("evtMgr");
		return graphEventManager;
	}

	public GraphStatisticsHandler getStatisticsHandler() {
		System.out.println("staticsHandler");
		return null;
	}

	public PrefixMapping getPrefixMapping() {
		System.out.println("prefixMapping");
		return this.prefixMapping;
	}

	public static String getNameOfNode(Node node) {
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

	private HashMap<TableName, HTable> tables = new HashMap<TableName, HTable>();

	private static final TableAxis[] coreAxis = new TableAxis[] {
			TableAxis.Subject, TableAxis.Object };

	public void add(Triple t) throws AddDeniedException {
		Node s = t.getSubject(), p = t.getPredicate(), o = t.getObject();

		for (TableAxis axis : coreAxis) {
			TableName tableName = getTableName(axis, p);

			HTable ht;
			try {
				ht = getTable(tableName);
			} catch (IOException e) {
				throw new AddDeniedException("IOException while adding triple",
						t);
			}

			byte[] rowKey = null;

			if (axis == TableAxis.Subject)
				rowKey = Bytes.toBytes(s.toString());
			else
				rowKey = Bytes.toBytes(o.toString());

			Put update = new Put(rowKey);
			byte[] colFamilyBytes = "nodes".getBytes(), colQualBytes = null;
			if (axis == TableAxis.Subject)
				colQualBytes = Bytes.toBytes(o.toString());
			else
				colQualBytes = Bytes.toBytes(s.toString());

			update.add(colFamilyBytes, colQualBytes, Bytes.toBytes(""));
			put(ht, update, rowKey, colFamilyBytes, colQualBytes);

			update = null;
			rowKey = null;
			colFamilyBytes = null;
			colQualBytes = null;

		}
	}

	private HTable getTable(TableName tableName) throws IOException {
		HTable ht = null;
		if (tables.containsKey(tableName)) {
			ht = tables.get(tableName);
		} else {

			try {
				HTableDescriptor htd = admin.getTableDescriptor(tableName);
			} catch (TableNotFoundException e) {
				ht = createTable(tableName, admin);
			}

			ht = new HTable(tableName, admin.getConnection());

			tables.put(tableName, ht);
		}
		return ht;
	}

	final ExecutorService executor = Executors.newFixedThreadPool(15);

	private void put(final HTable table, final Put update, final byte[] bytes,
			final byte[] colFamilyBytes, final byte[] colQualBytes) {
		executor.execute(new Runnable() {

			public void run() {
				try {
					table.checkAndPut(bytes, colFamilyBytes, colQualBytes,
							null, update);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					try {
						table.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		});
	}

	private HTable createTable(TableName tableName, HBaseAdmin admin)
			throws IOException {

		HTable table = null;
		if (!admin.tableExists(tableName)) {
			System.out.printf("Creating table %s\n",
					tableName.getNameAsString());
			HTableDescriptor desc = new HTableDescriptor(tableName);
			HColumnDescriptor cf = new HColumnDescriptor("nodes".getBytes());
			desc.addFamily(cf);
			admin.createTable(desc);
		}

		table = new HTable(tableName, admin.getConnection());
		table.setAutoFlush(false, true);
		return table;
	}

	public void delete(Triple t) throws DeleteDeniedException {
		System.out.println("delete(Triple)");

	}

	public ExtendedIterator<Triple> find(TripleMatch m) {
		System.out.println("find(TripleMatch)");
		ExtendedIterator<Triple> trIter = NullIterator.instance();

		// Node s = m.getMatchSubject();
		// Node p = m.getMatchPredicate();
		// Node o = m.getMatchObject();
		//
		// // FIXME More robust resolution of axis
		// TableAxis axis = (o==null || !o.isConcrete()
		// ?TableAxis.Subject:TableAxis.Object);
		//
		//
		//
		// try {
		// if(axis == TableAxis.Subject){
		//
		// Get res = new Get(Bytes.toBytes(s.toString()));
		// if (p.isConcrete()) {
		// TableName tableName = getTableName(axis, p);
		//
		// HTable table = getTable(tableName);
		//
		// Result rr = null;
		// if (table != null)
		// rr = table.get(res);
		// if (rr != null && !rr.isEmpty())
		// // XXX
		// trIter = null;
		// // rr. new HBaseRdfSingleRowIterator(rr, sm, pm, om,
		// // pm.toString(),
		// // TableDescVPCommon.COL_FAMILY_NAME_STR);
		// } else {
		// trIter = null; // new HBaseRdfAllTablesIterator();
		// // Iterate over all tables to find all triples for the
		// // subject
		// assertAllTables();
		// for(TableName tableName:tables.keySet()){
		// if(!isOnAxis(axis, tableName))
		// continue;
		//
		// HTable table = tables.get(tableName);
		//
		// Result rr = null;
		// if (table != null)
		// rr = table.get(res);
		// if (rr != null && !rr.isEmpty())
		// // XXX
		// trIter = null;
		// // ((HBaseRdfAllTablesIterator) trIter)
		// // .addIter(new HBaseRdfSingleRowIterator(
		// // rr,
		// // sm,
		// // pm,
		// // om,
		// // getPredicateMapping(tblName),
		// // TableDescVPCommon.COL_FAMILY_NAME_STR));
		// }
		// }
		// res = null;
		// } else if (axis == TableAxis.Object) {
		//
		// Get res = new Get(Bytes.toBytes(o.toString()));
		// if (p.isConcrete()) {
		// TableName tableName = getTableName(axis, p);
		//
		// HTable table = getTable(tableName);
		//
		// Result rr = null;
		// if (table != null)
		// rr = table.get(res);
		// if (rr != null && !rr.isEmpty())
		// // XXX
		// trIter = null;
		// // trIter = new HBaseRdfSingleRowIterator(rr, sm, pm, om,
		// // pm.toString(),
		// // TableDescVPCommon.COL_FAMILY_NAME_STR);
		// } else {
		// trIter = new HBaseRdfAllTablesIterator();
		// // Iterate over all tables to find all triples for the
		// // subject
		// Iterator<String> iterTblNames = tables().keySet()
		// .iterator();
		// while (iterTblNames.hasNext()) {
		// String tblName = iterTblNames.next();
		// String mapPrefix = processTblName(tblName, tblPrefix,
		// "objects", "subjects");
		// if (mapPrefix == null)
		// continue;
		// HTable table = tables().get(tblName);
		// Result rr = null;
		// if (table != null)
		// rr = table.get(res);
		// if (rr != null && !rr.isEmpty())
		// ((HBaseRdfAllTablesIterator) trIter)
		// .addIter(new HBaseRdfSingleRowIterator(
		// rr,
		// sm,
		// pm,
		// om,
		// getPredicateMapping(tblName),
		// TableDescVPCommon.COL_FAMILY_NAME_STR));
		// tblName = null;
		// mapPrefix = null;
		// }
		// ((HBaseRdfAllTablesIterator) trIter).closeIter();
		// }
		// res = null;
		// } else if (tblType.equalsIgnoreCase("pred")) {
		// // Create an iterator over all rows in the subject's HTable
		// Scan scanner = new Scan();
		// HTable table = tables().get(
		// name() + "-" + tblPrefix + "-"
		// + HBaseUtils.getNameOfNode(pm) + "-subjects");
		// if (table != null)
		// trIter = new HBaseRdfSingleTableIterator(
		// table.getScanner(scanner), sm, pm, om,
		// pm.toString(),
		// TableDescVPCommon.COL_FAMILY_NAME_STR);
		// } else if (tblType.equalsIgnoreCase("all")) {
		// trIter = new HBaseRdfAllTablesIterator();
		// // Iterate over all tables to find all triples for the subject
		// Iterator<String> iterTblNames = tables().keySet().iterator();
		// while (iterTblNames.hasNext()) {
		// String tblName = iterTblNames.next();
		// String mapPrefix = processTblName(tblName, tblPrefix,
		// "subjects", "objects");
		// if (mapPrefix == null)
		// continue;
		// HTable table = tables().get(tblName);
		// Scan scanner = new Scan();
		// if (table != null)
		// ((HBaseRdfAllTablesIterator) trIter)
		// .addIter(new HBaseRdfSingleTableIterator(table
		// .getScanner(scanner), sm, pm, om,
		// getPredicateMapping(tblName),
		// TableDescVPCommon.COL_FAMILY_NAME_STR));
		// tblName = null;
		// mapPrefix = null;
		// }
		// ((HBaseRdfAllTablesIterator) trIter).closeIter();
		// }
		// sb = null;
		// } catch (Exception e) {
		// throw new HBaseRdfException("Error in querying tables", e);
		// }
		return trIter;
	}

	private boolean isOnAxis(TableAxis axis, TableName name) {
		return name.getNameAsString().endsWith(axis.toString().toLowerCase());
	}

	private boolean allTableNamesLoaded;

	private synchronized void assertAllTables() throws IOException {
		if (!allTableNamesLoaded) {
			for (TableName tableName : admin.listTableNames()) {
				if (!tables.containsKey(tableName)) {
					tables.put(tableName,
							new HTable(tableName, admin.getConnection()));
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

	public ExtendedIterator<Triple> find(Node s, Node p, Node o) {
		System.out.println("find(Node,Node,Node)");
		// TODO: implement query
		if (p.isConcrete()) {
			// perform gets
			TableName tableName;
			if (s.isConcrete()) {
				tableName = getTableName(TableAxis.Subject, p);
				Get get = new Get(s.toString().getBytes());
				if (o.isConcrete()) {
					get.addColumn("nodes".getBytes(), o.toString().getBytes());
				} else {
					get.addFamily("nodes".getBytes());
				}
				HTable table;
				try {
					table = getTable(tableName);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return NullIterator.instance();
				}
				Result getResult = null;
				try {
					getResult = table.get(get);

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (getResult == null || getResult.isEmpty()) {
					return NullIterator.instance();
				}

				return new ResultIterator(getResult, s, p, o, p.toString(),
						"nodes");
			}
		} else {
			// perform scans
		}
		return null;
	}

	public boolean isIsomorphicWith(com.hp.hpl.jena.graph.Graph g) {
		System.out.println("isIsomorphicWith(Graph)");
		return false;
	}

	public boolean contains(Node s, Node p, Node o) {
		System.out.println("contains(Node,Node,Node)");
		return false;
	}

	public boolean contains(Triple t) {
		System.out.println("contains(Triple)");
		return false;
	}

	public void clear() {
		System.out.println("clear");

	}

	public void remove(Node s, Node p, Node o) {
		System.out.println("remove(Node,Node,Node)");

	}

	public void close() {
		System.out.println("close");

	}

	public boolean isEmpty() {
		System.out.println("isEmpty");
		return false;
	}

	public int size() {
		System.out.println("size");
		return 0;
	}

	public boolean isClosed() {
		System.out.println("isClosed");
		try {
			executor.shutdown();
			executor.awaitTermination(1, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public void sync() {

	}
}
