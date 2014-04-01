package edu.umkc.sce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
	private final String query ="SELECT ?x ?y ?z WHERE { <http://sce.umkc.edu/> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Ontology> .}"; 

	// "select ?x ?z ?a " + "where " + "{ "
	// + " ?x <http://purl.uniprot.org/core/reviewed> ?y . "
	// + " ?x <http://purl.uniprot.org/core/created> ?b . "
	// + " ?x <http://purl.uniprot.org/core/mnemonic> \"003L_IIV3\" . "
	// + " ?x <http://purl.uniprot.org/core/citation> ?z . "
	// + " ?z <http://purl.uniprot.org/core/author> ?a . }";

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		if (args.length != 1) {
			GenericOptionsParser.printGenericCommandUsage(System.out);
			System.exit(2);
		}

		Model model = null;
		Store store = null;
		store = new Store(conf);

		model = createModel(store, new HBaseAdmin(conf));
		try {
			runTestQuery(model, query);
		} finally {
			model.close();
		}

		return 0;
	}
	

	private Model createModel(Store store, HBaseAdmin admin) {
		Model model;
		Graph graph;
		graph = new edu.umkc.sce.rdf.Graph(store, admin);
		model = ModelFactory.createModelForGraph(graph);
		
		return model;
	}

	private void runTestQuery(Model model, String query) {
		QueryExecution qe = QueryExecutionFactory.create(query, model);
		try {
			ResultSet results = qe.execSelect();
			ResultSetFormatter.out(System.out, results);
		} finally {
			qe.close();
		}
	}

}
