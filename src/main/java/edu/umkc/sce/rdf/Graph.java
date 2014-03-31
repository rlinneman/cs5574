package edu.umkc.sce.rdf;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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

public class Graph implements com.hp.hpl.jena.graph.Graph {

	private HBaseAdmin admin;
	private Store store;
	private PrefixMapping prefixMapping;
	private TransactionHandler transactionHandler;
	private GraphEventManager graphEventManager;

	public Graph(Store store, HBaseAdmin admin) {
		this.store = store;
		this.prefixMapping = new edu.umkc.sce.rdf.PrefixMapping(null, null);
		this.transactionHandler = new TxHandler();
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

	private String getPrefixAndPred(Node... row) {
		String prefix = null, pred = null;
		if (row.length == 4) {
			prefix = row[0].getLocalName();
			pred = getNameOfNode(row[2]);
		} else {
			prefix = "tbl";
			pred = getNameOfNode(row[1]);
		}
		return prefix + "~" + pred;
	}

	private long totalSize = 0L;

	public void add(Triple t) throws AddDeniedException {
		System.out.println("add(Triple)");

		// totalTriples++ ;
		Node[] row = new Node[] { t.getSubject(), t.getPredicate(),
				t.getObject() };
		String[] prefixAndPred = getPrefixAndPred(row).split("~");

		int start = (row.length == 3) ? 0 : 1;

		for (int i = 0; i < 2; i++) {
			// TableName tableName = TableName.valueOf("rdf", name() + "-"
			// + prefixAndPred[0] + "-" + prefixAndPred[1]
			// + (i == 0 ? "subjects" : "objects"));
			TableName tableName = TableName.valueOf("rdf", prefixAndPred[0]
					+ "-" + prefixAndPred[1] + "-"
					+ (i == 0 ? "subjects" : "objects"));
			try {
				HTableDescriptor htd;
				htd = admin.getTableDescriptor(tableName);
			} catch (TableNotFoundException e) {
				try {
					createTable(tableName, admin);
				} catch (IOException e1) {
					throw new AddDeniedException("Error while creating table",
							t);
				}
			} catch (IOException e) {
				throw new AddDeniedException("Unable to create table", t);
			}
			HTable ht;
			try {
				ht = new HTable(tableName, admin.getConnection());
			} catch (IOException e) {
				throw new AddDeniedException(String.format(
						"unable to use table %s:%s",
						tableName.getNamespaceAsString(),
						tableName.getNameAsString()), t);
			}

			try {
				byte[] bytes = null;
				if (i == 0)
					bytes = Bytes.toBytes(row[start].toString());
				else
					bytes = Bytes.toBytes(row[i + 1 + start].toString());
				totalSize += bytes.length;
				Put update = new Put(bytes);
				byte[] colFamilyBytes = "nodes".getBytes(), colQualBytes = null;
				if (i == 0)
					colQualBytes = Bytes.toBytes(row[2 + start].toString());
				else if (start == 0)
					colQualBytes = Bytes.toBytes(row[0].toString());
				else
					colQualBytes = Bytes.toBytes(row[1].toString());
				totalSize += colQualBytes.length;

				update.add(colFamilyBytes, colQualBytes, Bytes.toBytes(""));
				ht.checkAndPut(bytes, colFamilyBytes, colQualBytes, null,
						update);

				update = null;
				bytes = null;
				colFamilyBytes = null;
				colQualBytes = null;
			} catch (IOException e) {
				throw new AddDeniedException("", t);
			} finally {
				try {
					ht.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		prefixAndPred = null;

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
		return null;
	}

	public ExtendedIterator<Triple> find(Node s, Node p, Node o) {
		System.out.println("find(Node,Node,Node)");
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
		return false;
	}
}
