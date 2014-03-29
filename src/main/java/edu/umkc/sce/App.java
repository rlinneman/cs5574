package edu.umkc.sce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor.Builder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.riot.RDFLanguages;

import com.google.common.base.Stopwatch;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * Hadoop/Hbase/Jena Hello world!
 * 
 */
public class App extends Configured implements Tool {
	private static final String MY_NAMESPACE = "foo";

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		int result;
		try {
			result = ToolRunner.run(conf, new App(), args);
		} catch (Exception e) {
			e.printStackTrace();
			result = -1;
		}
		System.exit(result);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		if (args.length != 1) {
			GenericOptionsParser.printGenericCommandUsage(System.out);
			System.exit(2);
		}

		String importFile = args[0];

		FileSystem fs = null;
		BufferedReader br = null;
		Model m = null;

		try {
			fs = FileSystem.get(conf);
			Path path = new Path(importFile);
			br = new BufferedReader(new InputStreamReader(fs.open(path)));
			m = createModel();

			Stopwatch sw = new Stopwatch();
			sw.start();
			m.read(br, null, RDFLanguages.strLangNTriples);
			sw.stop();
			System.out.printf("Loading '%s' took %d.\n", importFile,
					sw.elapsedTime(TimeUnit.MILLISECONDS));
			sw.reset();
			sw.start();
			runTestQuery(m);
			sw.stop();
			System.out.printf("Query '%s' took %d.\n", query,
					sw.elapsedTime(TimeUnit.MILLISECONDS));
			sw.reset();
			sw.start();

			createStore(m);
			sw.stop();
			System.out.printf("loadHbase took %d.\n",
					sw.elapsedTime(TimeUnit.MILLISECONDS));
		} finally {
			if (m != null)
				m.close();
			if (br != null)
				br.close();
			if (fs != null)
				fs.close();
		}

		return 0;
	}

	private Model createModel() {
		Model model = ModelFactory.createDefaultModel();
		// Configuration conf = HBaseConfiguration.create(getConf());
		// HBaseAdmin admin = null;
		// Model model;
		// conf.setQuietMode(true);
		//
		// try {
		// admin = new HBaseAdmin(conf);
		// } catch (MasterNotRunningException | ZooKeeperConnectionException e)
		// {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// if (admin == null)
		// return null;
		//
		// try {
		// com.hp.hpl.jena.sdb.Store store = SDBFactory
		// .connectStore("sdb.ttl");
		//
		// store.getTableFormatter().create();
		//
		// model = SDBFactory.connectDefaultModel(store);
		// } finally {
		// try {
		// admin.close();
		// } catch (IOException e) {
		// }
		// }
		return model;
	}

	private final String query = "select ?x ?z ?a " + "where " + "{ "
			+ " ?x <http://purl.uniprot.org/core/reviewed> ?y . "
			+ " ?x <http://purl.uniprot.org/core/created> ?b . "
			+ " ?x <http://purl.uniprot.org/core/mnemonic> \"003L_IIV3\" . "
			+ " ?x <http://purl.uniprot.org/core/citation> ?z . "
			+ " ?z <http://purl.uniprot.org/core/author> ?a . " + "}";

	private void runTestQuery(Model model) {
		QueryExecution qe = QueryExecutionFactory.create(query, model);
		try {
			ResultSet results = qe.execSelect();
			ResultSetFormatter.out(System.out, results);
		} finally {
			qe.close();
		}
	}

	private void createStore(Model model) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = getAdmin();
		// com.hp.hpl.jena.sdb.Store store = null;

		try {
			Builder ndesc = NamespaceDescriptor.create(MY_NAMESPACE);
			try {
				admin.createNamespace(ndesc.build());
			} catch (IOException ex) {
			}

//			truncate(admin.listTableNamesByNamespace(MY_NAMESPACE));
			// store.getTableFormatter().create();
			//
			// TableName tableName = TableName.valueOf("foo", "bar");
			//
			// if (admin.tableExists(tableName)) {
			// admin.disableTable(tableName);
			// admin.deleteTable(tableName);
			// admin.deleteNamespace(tableName.getNamespaceAsString());
			// } else {
			loadData(model);
			// }

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			admin.close();
		}
	}

	/**
	 * @param listTableNamesByNamespace
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 */
	private void truncate(final TableName[] tableNames)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		HBaseAdmin admin = getAdmin();
		for (TableName tableName : tableNames) {
			System.out.printf("Dropping %s\n", tableName.getNameAsString());
			if (admin.isTableEnabled(tableName)) {
				admin.disableTable(tableName);
			}
			admin.deleteTable(tableName);
		}
	}

	private void loadData(Model model) throws IOException {
		HashMap<TableName, HTable> tables = new HashMap<TableName, HTable>();
		StmtIterator statements = model.listStatements();
		while (statements.hasNext()) {
			Statement stmt = statements.nextStatement();
			Put put = createPut(stmt);
			TableName tableName = getTableName(stmt.getPredicate());
			HTable table = null;
			if (tables.containsKey(tableName)) {
				table = tables.get(tableName);
			} else {
				table = createTable(tableName);
				tables.put(tableName, table);
			}
			table.put(put);
		}

		for (HTable table : tables.values()) {
			table.flushCommits();
		}

	}

	private HTable createTable(TableName name)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		HBaseAdmin admin = getAdmin();
		HTable table = null;
		if (!admin.tableExists(name)) {
			System.out.printf("Creating table %s\n", name.getNameAsString());
			HTableDescriptor desc = new HTableDescriptor(name);
			HColumnDescriptor cf = new HColumnDescriptor("rdf".getBytes());
			desc.addFamily(cf);
			admin.createTable(desc);
		}

		table = new HTable(getHBaseConf(), name);
		table.setAutoFlush(false, true);
		return table;
	}

	/**
	 * @return
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 */
	private HBaseAdmin getAdmin() throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		if (_admin == null) {
			_admin = new HBaseAdmin(getHBaseConf());
		}
		return _admin;
	}

	private HBaseAdmin _admin;
	private Configuration _hbaseConf;

	private Configuration getHBaseConf() {
		if (_hbaseConf == null) {
			Configuration conf = getConf();
			conf.setQuietMode(true);
			conf = HBaseConfiguration.create(conf);
			conf.setQuietMode(true);

			_hbaseConf = conf;
		}
		return _hbaseConf;
	}

	/**
	 * @param predicate
	 * @return
	 */
	private TableName getTableName(Property predicate) {
		TableName name = null;

		name = TableName.valueOf(MY_NAMESPACE, predicate.getLocalName());

		return name;
	}

	private Put createPut(Statement s) {
		Property p = s.getPredicate();
		String predicateName = p.getLocalName();
		Put put = new Put(predicateName.getBytes());
		RDFNode o = s.getObject();

		put.add("rdf".getBytes(), "o".getBytes(), o.isLiteral() ? o.asLiteral()
				.getString().getBytes() : o.asResource().getLocalName()
				.getBytes());

		Resource sub = s.getSubject();

		put.add("rdf".getBytes(), "s".getBytes(), sub.getLocalName().getBytes());

		return put;
	}
}