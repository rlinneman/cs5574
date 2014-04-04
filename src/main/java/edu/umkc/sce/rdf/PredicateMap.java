/**
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

package edu.umkc.sce.rdf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;

public class PredicateMap {
	private final HBaseAdmin admin;
	private final TableName mapTableName;
	private HTable mapTable;

	private final Map<TableName, Node> nodes;

	public PredicateMap(HBaseAdmin admin) {
		this.admin = admin;
		mapTableName = TableName.valueOf("rdf".getBytes(), "predicateMap".getBytes());
		this.nodes = new HashMap<TableName, Node>();
	}

	private HTable getMapTable() throws IOException {
		if (mapTable == null) {
			if (!admin.tableExists(mapTableName)) {
				createTable();
			}else{
				mapTable = new HTable(mapTableName, admin.getConnection());
			}
		}
		return mapTable;
	}

	private synchronized void createTable() throws IOException {
		if(mapTable != null)
			return;
		if (!admin.tableExists(mapTableName)) {
			HTableDescriptor htd = new HTableDescriptor(mapTableName);
			HColumnDescriptor cf = new HColumnDescriptor(
					"predicateUris".getBytes());
			htd.addFamily(cf);
			admin.createTable(htd);
		}
		mapTable = new HTable(mapTableName, admin.getConnection());
	}

	public Node get(TableName tableName) {
		Node node = null;
		if (this.nodes.containsKey(tableName)) {
			node = this.nodes.get(tableName);
		} else {
			node = fetch(tableName);
		}

		return node;
	}

	private synchronized Node fetch(TableName tableName) {
		if (this.nodes.containsKey(tableName)) {
			return this.nodes.get(tableName);
		}
		Node node = null;
		Get get = new Get(tableName.getNameAsString().getBytes());
		get.addFamily("predicateUris".getBytes());
		Result result = null;
		try {
			result = getMapTable().get(get);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (result != null && !result.isEmpty()) {
			String uri = Bytes.toString(result.value());
			node = NodeFactory.createURI(uri);
			_put(tableName, node);
		}
		return node;
	}
	
	private void _put(TableName tableName, Node node) {
		Put put = new Put(tableName.getNameAsString().getBytes());
		put.add("predicateUris".getBytes(), "uri".getBytes(), node.getURI()
				.getBytes());
		try {
			getMapTable().checkAndPut(tableName.getNameAsString().getBytes(),
					"predicateUris".getBytes(), "uri".getBytes(),
					null, put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public synchronized void put(TableName predicateName, Node node) {
		if (!this.nodes.containsKey(predicateName)) {
			nodes.put(predicateName, node);
			_put(predicateName, node);
		}
	}
}
