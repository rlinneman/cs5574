package edu.umkc.sce;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.riot.RDFLanguages;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import edu.umkc.sce.rdf.Store;

public class VpQuery extends Configured implements Tool {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		int result;
		try {
			result = ToolRunner.run(conf, new VpQuery(), args);
		} catch (Exception e) {
			e.printStackTrace();
			result = -1;
		}
		System.exit(result);
	}

	// start with a very basic concrete query---no variables
	private final String[] queries = new String[] {
			"select ?a "
					+ "where "
					+ "{ "
					+ " <http://purl.uniprot.org/citations/7934828> <http://purl.uniprot.org/core/author> ?a . "
					+ "} order by ?a",
			"select ?p ?o " + "where " + "{"
					+ "<http://purl.uniprot.org/uniprot/Q6GZX4> ?p ?o . "
					+ "} order by ?p",
	// "select ?x ?y " + "where " + "{ "
	// + " ?x <http://purl.uniprot.org/core/name> \"Virology\" . "
	// + " ?x <http://purl.uniprot.org/core/volume> ?y . "
	// + "} order by ?x",
	// "select ?x ?z " + "where " + "{ "
	// + "?x <http://purl.uniprot.org/core/name> ?y . "
	// + "?x <http://purl.uniprot.org/core/volume> ?z . "
	// + "?x <http://purl.uniprot.org/core/pages> \"176-186\" . "
	// + "} order by ?x",
	// // "select ?x ?y ?z " + "where " + "{ "
	// // + "?x <http://purl.uniprot.org/core/name> \"Science\" . "
	// // + "?x <http://purl.uniprot.org/core/author> ?y . "
	// // + "?z <http://purl.uniprot.org/core/citation> ?x . " + "}",
	// "select ?x ?y "
	// + "where "
	// + "{ "
	// + "?x ?y \"Israni S.\" . "
	// + "<http://purl.uniprot.org/citations/15372022> ?y \"Gomez M.\" . "
	// + "} order by ?x",
	// // "select ?a ?b " + "where " + "{ "
	// // + "?x ?y <http://purl.uniprot.org/citations/15165820> . "
	// // + "?a ?b ?y . " + "} ",
	// "select ?x ?z ?a "
	// + "where "
	// + "{ "
	// + "?x <http://purl.uniprot.org/core/reviewed> ?y . "
	// + "?x <http://purl.uniprot.org/core/created> ?b . "
	// + "?x <http://purl.uniprot.org/core/mnemonic> \"003L_IIV3\" . "
	// + "?x <http://purl.uniprot.org/core/citation> ?z . "
	// + "?z <http://purl.uniprot.org/core/author> ?a . "
	// + "} order by ?x"
	};

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

		fs = FileSystem.get(conf);
		Path path = new Path(importFile);
		br = new BufferedReader(new InputStreamReader(fs.open(path)));
		m = ModelFactory.createDefaultModel();
		m.read(br, null, RDFLanguages.strLangNTriples);
		br.close();

		Model model = null;
		Store store = null;
		store = new Store(conf);

		model = createModel(store);
		try {
			for (String query : queries)
				runTestQuery(model, query, m);
		} finally {
			model.close();
		}

		return 0;
	}

	private Model createModel(Store store) {
		Model model;
		Graph graph;
		graph = new edu.umkc.sce.rdf.Graph(store);
		model = ModelFactory.createModelForGraph(graph);

		return model;
	}

	private void runTestQuery(Model model, String query, Model m) {
		System.out.printf("executing %s", query);
		QueryExecution qe = QueryExecutionFactory.create(query, m);
		String result1, result2;
		try {
			ResultSet results = qe.execSelect();

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ResultSetFormatter.out(baos, results);

			result1 = baos.toString();
			qe.close();

			qe = QueryExecutionFactory.create(query, model);
			baos = new ByteArrayOutputStream();
			results = qe.execSelect();
			ResultSetFormatter.out(baos, results);
			result2 = baos.toString();
		} finally {
			qe.close();
		}
		if (result1.equalsIgnoreCase(result2)) {
			System.out.printf("\nQuery passed assertion: %s \n\n", query);
		} else {
			System.out.printf(
					"\nQuery failed assertion expected:\n%s\nactual:\n%s",
					result1, result2);
		}
	}

}
