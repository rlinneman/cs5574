package edu.umkc.sce;

import java.io.BufferedReader;
import java.io.IOException;
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
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import edu.umkc.sce.rdf.Store;

public class VpImport extends Configured implements Tool {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		int result;
		try {
			result = ToolRunner.run(conf, new VpImport(), args);
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

		Model model = null;
		Store store = null;
		store = new Store(conf);
		store.format();
		model = createModel(store, new HBaseAdmin(conf));
		try {
			load(conf, model, importFile);
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

	private void load(Configuration conf, Model model, String importFile)
			throws IOException {

		FileSystem fs = null;
		BufferedReader br = null;
		try {
			fs = FileSystem.get(conf);
			Path path = new Path(importFile);
			br = new BufferedReader(new InputStreamReader(fs.open(path)));

			model.begin();
			model.read(br, null, RDFLanguages.strLangNTriples);
			model.commit();
		} finally {
			if (br != null)
				br.close();
			if (fs != null)
				fs.close();
		}
	}
}
