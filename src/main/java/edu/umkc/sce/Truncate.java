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
 */package edu.umkc.sce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umkc.sce.rdf.HBaseStore;

public class Truncate extends Configured implements Tool {
	private static final String MY_NAMESPACE = "foo";

	public static void main(String[] args) {		
		Configuration conf = new Configuration();
		int result;
		try {
			result = ToolRunner.run(conf, new Truncate(), args);
		} catch (Exception e) {
			e.printStackTrace();
			result = -1;
		}
		System.exit(result);
	}

	public int run(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {

		HBaseStore hBaseStore = new HBaseStore(getConf());
		hBaseStore.format();
		return 0;
	}
}
