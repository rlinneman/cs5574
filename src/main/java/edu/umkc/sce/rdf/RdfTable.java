package edu.umkc.sce.rdf;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

import javax.activity.InvalidActivityException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;

public class RdfTable implements Table {
	private static final byte[] columnFamilyBytes = Bytes.toBytes("nodes");
	private static final byte[] emptyStringBytes = Bytes.toBytes("");
	private final TableName tableName;
	private final HBaseAdmin admin;
	private final Node predicate;
	private final TableAxis axis;
	private HTable table;

	public RdfTable(TableName tableName, HBaseAdmin admin, Node predicate) {
		if (!predicate.isConcrete())
			throw new IllegalArgumentException(
					"this table must represent a concrete node.");

		this.tableName = tableName;
		this.admin = admin;
		this.predicate = predicate;
		this.axis = tableName.getNameAsString().endsWith(
				TableAxis.Subject.toString().toLowerCase()) ? TableAxis.Subject
				: TableAxis.Object;
	}

	public ExtendedIterator<Triple> get(Node s, Node o) {
		ExtendedIterator<Triple> results = NullIterator.instance();

		// S | P | O | ACTION
		// ------------------------------------------------------------
		// T | T | T | Get a subject table
		// F | T | T | Get an object table
		// T | T | F | Get a subject table
		// F | T | F | Scan a subject table
		// T | F | T | Get on ALL subject tables
		// T | F | F | Get on ALL subject tables
		// F | F | T | Get on ALL object tables
		// F | F | F | Scan ALL subject tables

		if (s.isConcrete() || o.isConcrete()) {
			// get... may be across multiple tables
			Get get;
			if (s.isConcrete()) {
				get = new Get(s.toString().getBytes());

				if (o.isConcrete()) {
					get.addColumn(columnFamilyBytes, o.toString().getBytes());
				} else {
					get.addFamily(columnFamilyBytes);
				}
			} else {
				get = new Get(o.toString().getBytes());
				get.addFamily(columnFamilyBytes);
			}
			Result gr;

			// get on a specific table
			gr = getResults(tableName, get);
			if (gr != null)
				results = new ResultTripleIterator(gr, s, predicate, o,
						tableName);
		} else {

			Scan scan = new Scan();
			scan.addFamily(columnFamilyBytes);
			ResultScanner scanner;

			// scan of a single subjects table
			scanner = getResults(tableName, scan);
			if (scanner != null) {
				results = new ResultSetTripleIterator(scanner, s, predicate, o,
						tableName);
			}

		}

		return results;
	}

	private ResultScanner getResults(TableName tableName, Scan scan) {

		ResultScanner result = null;
		HTable table;
		try {
			table = getTable(tableName);

			result = table.getScanner(scan);
			return result;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private Result getResults(TableName tableName, Get get) {

		Result result = null;
		HTable table;
		try {
			table = getTable(tableName);

			result = table.get(get);

			if (result != null && !result.isEmpty()) {
				return result;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	boolean hasCheckedExists;
	boolean _exists;

	public boolean exists() {
		if (!hasCheckedExists) {

			try {
				HTableDescriptor htd = admin.getTableDescriptor(tableName);
				_exists = true;
			} catch (TableNotFoundException e) {
				_exists = false;
			} catch (IOException e) {
				_exists = false;
			} finally {
				hasCheckedExists = true;
			}
		}
		return _exists;
	}

	private HTable getTable(TableName tableName) throws IOException {
		if (table == null) {
			table = new HTable(tableName, admin.getConnection());
		}
		return table;
	}

	/**
	 * Create the underlying {@link HTable} if it does not exist
	 * 
	 * returns true if the table was successfully created or already exists.
	 */
	public synchronized boolean create() {
		if (table != null)
			// throw new IllegalStateException("table is already loaded");
			return true;

		try {
			if (!admin.tableExists(tableName)) {
				HTableDescriptor desc = new HTableDescriptor(tableName);
				HColumnDescriptor cf = new HColumnDescriptor("nodes".getBytes());
				desc.addFamily(cf);
				admin.createTable(desc);
				table = new HTable(tableName, admin.getConnection());
				table.setAutoFlush(false, true);
				hasCheckedExists = _exists = true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

		return true;

	}

	public TableAxis getAxis() {
		return axis;
	}

	public void put(Node s, Node o) {
		byte[] rowKey, colQualBytes = null;

		if (axis == TableAxis.Subject) {
			rowKey = Bytes.toBytes(s.toString());
			colQualBytes = Bytes.toBytes(o.toString());
		} else {
			rowKey = Bytes.toBytes(o.toString());
			colQualBytes = Bytes.toBytes(s.toString());
		}

		Put update = new Put(rowKey);

		update.add(columnFamilyBytes, colQualBytes, emptyStringBytes);
//		try {
//			if (!exists())
//				create();
//
//			getTable(tableName).checkAndPut(rowKey, columnFamilyBytes,
//					colQualBytes, null, update);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		 puts.add(update);
		update = null;
		rowKey = null;
		colQualBytes = null;
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

	private List<Put> puts = new ArrayList<Put>();

	public synchronized void flush() {
		if (!puts.isEmpty()) {
			try {
				HTable ht = getTable(tableName);
				ht.put(puts);
				puts.clear();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (table != null) {
			try {
				table.flushCommits();
			} catch (RetriesExhaustedWithDetailsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedIOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public Node getPredicate() {
		return predicate;
	}

	public TableName getName() {
		return tableName;
	}

}