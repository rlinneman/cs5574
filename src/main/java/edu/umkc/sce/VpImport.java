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
import java.io.IOException;
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
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import edu.umkc.sce.rdf.HBaseStore;

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

		System.out.println("Invoking Import");
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		if (args.length != 1) {
			GenericOptionsParser.printGenericCommandUsage(System.out);
			System.out.println(args.length);
			System.exit(2);
		}

		String importFile = args[0];
//		conf.set("fs.hdfs.impl", 
//				
//		        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
//		    );
//		conf.set("fs.file.impl",
//		        org.apache.hadoop.fs.LocalFileSystem.class.getName()
//		    );
		Model model = null;
		HBaseStore hBaseStore = null;
		hBaseStore = new HBaseStore(conf);
		hBaseStore.format();
		model = createModel(hBaseStore);
		try {
			load(conf, model, importFile);
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
