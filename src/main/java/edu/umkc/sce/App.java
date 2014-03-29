package edu.umkc.sce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * Hadoop/Hbase/Jena Hello world!
 * 
 */
public class App extends Configured implements Tool {
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

	@Override
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
			m = ModelFactory.createDefaultModel();

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

			loadHbase(m);
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

	private void loadHbase(Model model) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		Configuration conf = HBaseConfiguration.create(getConf());
		HBaseAdmin admin = new HBaseAdmin(conf);

		try {
			// Model hbaseModel = ModelFactory.createDefaultModel();
			// StmtIterator itr = model.listStatements();
			// hbaseModel.begin();
			// try{
			// while(itr.hasNext()){
			// hbaseModel.add(itr.nextStatement());
			// hbaseModel.commit();
			// }
			// }catch(Exception ex){
			// hbaseModel.abort();
			// throw ex;
			// }
			TableName tableName = TableName.valueOf("foo", "bar");
			HTableDescriptor desc = new HTableDescriptor(tableName);

			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				admin.deleteNamespace(tableName.getNamespaceAsString());
			} else {
				HColumnDescriptor meta = new HColumnDescriptor(
						"personal".getBytes());
				HColumnDescriptor prefix = new HColumnDescriptor(
						"account".getBytes());
				desc.addFamily(meta);
				desc.addFamily(prefix);
				Builder ndesc = NamespaceDescriptor.create("foo");
				admin.createNamespace(ndesc.build());
				admin.createTable(desc);
			}
		} finally {
			admin.close();
		}
	}
}