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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
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
					+ "} order by ?x" };
	String[] hashes = new String[] {
"1a4a845d8e7f1d13593a3dcf42928229",
"0538ce358c70541d5f2e330b863de5a4",
"91135bf61377b7757ca9bdd4b85837aa",
"e7e2a0df7cdca98456040f28fe9e8719",
"66c1de58f28dcc1bf71b1ac78f1296ac",
"bc326806218a50989af732c3e85faafc",
"f88077c3d2debb9de1a25dc4fec99464",
"03e8116e8953d226fa65205e2c415121"
//			"15074267b6e509235b8834067dd9de39",
//			"dd4af557d075f65aa33cc66faf3a4e63",
//			"d56d6e825058dd16b7492009200e92ac",
//			"31ea05fac7e8b71231fef736218c688d",
//			"c2f43905853f2dada5131370649f1fcf",
//			"b34453a92903b3f4d63369e473e507d2",
//			"0bfa0c4a57959571716cf440b16e1b18",
//			"1f4a87c167173a6a29e5a700df26e1e2" 
			};

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		System.out.println("Invoking Query tests");

		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		// if (args.length != 1) {
		// GenericOptionsParser.printGenericCommandUsage(System.out);
		// System.exit(2);
		// }

		Model model = null;
		HBaseStore hBaseStore = null;
		hBaseStore = new HBaseStore(conf);

		model = createModel(hBaseStore);
		try {
			int queryIndex = 0;
			for (String query : queries) {
				runTestQuery(queryIndex, model, query);
				queryIndex += 1;
			}
		} finally {
			// m.close();
			// model.close();
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

	private void runTestQuery(int queryIndex, Model model, String query) {
		System.out.printf("executing [%d] %s", queryIndex, query);
		QueryExecution qe = null;
		String result = null;
		try {

			qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();
			byte[] hash = getResultHash(results); 

			result = Hex.encodeHexString(hash);
			
			ResultSetFormatter.out(System.out, results);
		} finally {

			qe.close();
		}
		System.out.flush();
		if (hashes[queryIndex].equalsIgnoreCase(result)) {
			System.out.printf("\nQuery [%d] passed assertion\n\n", queryIndex);

		} else {
			System.err.printf("\nQuery [%d] failed assertion %s\n", queryIndex,
					result);
		}
	}

	byte[] getResultHash(ResultSet results) {
		MessageDigest md = null;
		;
		try {
			md = MessageDigest.getInstance(MessageDigestAlgorithms.MD5);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		while (results.hasNext()) {
			QuerySolution sol = results.nextSolution();
			Iterator<String> col = sol.varNames();
			while (col.hasNext()) {
				md.update(sol.get(col.next()).asNode().toString().getBytes());
			}
		}
		return md.digest();
	}
}
