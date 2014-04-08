package edu.umkc.sce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Stopwatch;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import edu.umkc.sce.rdf.HBaseStore;

/** Very basic CLI query runner */
public class Query extends Configured implements Tool {

	public static void main(String[] args) {
		Configuration conf = new Configuration();

		int result;
		try {
			result = ToolRunner.run(conf, new Query(), args);
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
			System.out.println("select {args} [where] [orderby]");
			GenericOptionsParser.printGenericCommandUsage(System.out);

			System.exit(2);
		}

		String query = args[0];
		Model model = null;
		HBaseStore hBaseStore = null;
		hBaseStore = new HBaseStore(conf);

		model = createModel(hBaseStore);
		try {

			QueryExecution qe = QueryExecutionFactory.create(query, model);
			Stopwatch sw = new Stopwatch();
			sw.start();
			ResultSet results = qe.execSelect();
			sw.stop();
			ResultSetFormatter.out(System.out, results);
			System.out.printf("%d Results in %dms\n", results.getRowNumber(), sw.elapsedMillis());
		}catch(Exception e){ 
			e.printStackTrace(System.err);
		}finally {
			model.close();
		}

		return 0;
	}

	private Model createModel(HBaseStore hBaseStore) {
		Model model;
		Graph graph;
		graph = new edu.umkc.sce.rdf.HBaseVerticallyPartitionedGraph(hBaseStore);
		model = ModelFactory.createModelForGraph(graph);

		return model;
	}
}
