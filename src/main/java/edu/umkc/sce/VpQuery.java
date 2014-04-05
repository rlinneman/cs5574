/*
 * Copyright 2014 Ryan Linneman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umkc.sce;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import edu.umkc.sce.rdf.HBaseStore;

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
			"select ?x ?y " + "where " + "{ "
					+ " ?x <http://purl.uniprot.org/core/name> \"Virology\" . "
					+ " ?x <http://purl.uniprot.org/core/volume> ?y . "
					+ "} order by ?x",
			"select ?x ?z " + "where " + "{ "
					+ "?x <http://purl.uniprot.org/core/name> ?y . "
					+ "?x <http://purl.uniprot.org/core/volume> ?z . "
					+ "?x <http://purl.uniprot.org/core/pages> \"176-186\" . "
					+ "} order by ?x",
			"select ?x ?y ?z " + "where " + "{ "
					+ "?x <http://purl.uniprot.org/core/name> \"Science\" . "
					+ "?x <http://purl.uniprot.org/core/author> ?y . "
					+ "?z <http://purl.uniprot.org/core/citation> ?x . "
					+ "} order by ?x",
			"select ?x ?y "
					+ "where "
					+ "{ "
					+ "?x ?y \"Israni S.\" . "
					+ "<http://purl.uniprot.org/citations/15372022> ?y \"Gomez M.\" . "
					+ "} order by ?x",
			"select ?a ?b " + "where " + "{ "
					+ "?x ?y <http://purl.uniprot.org/citations/15165820> . "
					+ "?a ?b ?y . " + "} order by ?x",
			"select ?x ?z ?a "
					+ "where "
					+ "{ "
					+ "?x <http://purl.uniprot.org/core/reviewed> ?y . "
					+ "?x <http://purl.uniprot.org/core/created> ?b . "
					+ "?x <http://purl.uniprot.org/core/mnemonic> \"003L_IIV3\" . "
					+ "?x <http://purl.uniprot.org/core/citation> ?z . "
					+ "?z <http://purl.uniprot.org/core/author> ?a . "
					+ "} order by ?x"
	};

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
        System.out.println("Invoking Query tests");

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
		HBaseStore hBaseStore = null;
		hBaseStore = new HBaseStore(conf);

		model = createModel(hBaseStore);
		try {
			int queryIndex = 0;
			for (String query : queries) {
				runTestQuery(queryIndex, model, query, m);
				queryIndex += 1;
			}
		} finally {
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

	private void runTestQuery(int queryIndex, Model model, String query, Model m) {
		System.out.printf("executing [%d] %s", queryIndex, query);
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
		System.out.flush();
		if (result1.equalsIgnoreCase(result2)) {
			System.out.printf("\nQuery [%d] passed assertion\n\n", queryIndex);

		} else {
			System.out.flush();
			// System.err.printf(
			// "\nQuery [%d] failed assertion expected:\n%s\nactual:\n%s",
			// queryIndex, result1, result2);
			System.err.printf("\nQuery [%d] failed assertion\n", queryIndex,
					result1, result2);
			System.err.flush();
		}
	}

}
