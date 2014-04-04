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

import org.apache.hadoop.hbase.TableName;

import com.hp.hpl.jena.graph.BulkUpdateHandler;
import com.hp.hpl.jena.graph.Capabilities;
import com.hp.hpl.jena.graph.GraphEventManager;
import com.hp.hpl.jena.graph.GraphStatisticsHandler;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.TransactionHandler;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.shared.AddDeniedException;
import com.hp.hpl.jena.shared.DeleteDeniedException;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NiceIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.sun.tools.corba.se.idl.InvalidArgument;

public class Graph implements com.hp.hpl.jena.graph.Graph {

	private String name;
	private Store store;
	private PrefixMapping prefixMapping;
	private TransactionHandler transactionHandler;
	private GraphEventManager graphEventManager;

	public Graph(Store store) {
		this.store = store;
		this.prefixMapping = new edu.umkc.sce.rdf.PrefixMapping(null, null);
		this.transactionHandler = new TxHandler(this);
		this.graphEventManager = new GraphEventMgr();
		System.out.println("CTOR");
	}

	public boolean dependsOn(com.hp.hpl.jena.graph.Graph other) {
		System.out.println("dependsOn");
		return false;
	}

	public TransactionHandler getTransactionHandler() {
		System.out.println("txHandler");
		return transactionHandler;
	}

	public BulkUpdateHandler getBulkUpdateHandler() {
		System.out.println("bulk upd handler");
		return null;
	}

	public Capabilities getCapabilities() {
		System.out.println("capabilities");
		return null;
	}

	public GraphEventManager getEventManager() {
		System.out.println("evtMgr");
		return graphEventManager;
	}

	public GraphStatisticsHandler getStatisticsHandler() {
		System.out.println("staticsHandler");
		return null;
	}

	public PrefixMapping getPrefixMapping() {
		System.out.println("prefixMapping");
		return this.prefixMapping;
	}

	private static final TableAxis[] coreAxis = new TableAxis[] {
			TableAxis.Subject, TableAxis.Object };

	public void add(Triple t) throws AddDeniedException {
		Node s = t.getSubject(), p = t.getPredicate(), o = t.getObject();

		for (TableAxis axis : coreAxis) {
			Table ht = null;
			try {
				ht = store.getTable(axis, p);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidArgument e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (ht == null)
				throw new AddDeniedException("Failed to create table", t);

			ht.put(s, o);
		}
	}

	public void delete(Triple t) throws DeleteDeniedException {
		System.out.println("delete(Triple)");

	}

	public ExtendedIterator<Triple> find(Node s, Node p, Node o) {
		ExtendedIterator<Triple> results = new NiceIterator<Triple>();
		TableAxis axis = TableAxis.Subject;

		// # | S | P | O | ACTION
		// ------------------------------------------------------------
		// 0 | T | T | T | Get a subject table
		// 1 | F | T | T | Get an object table
		// 2 | T | T | F | Get a subject table
		// 3 | F | T | F | Scan a subject table
		// 4 | T | F | T | Get on ALL subject tables
		// 5 | T | F | F | Get on ALL subject tables
		// 6 | F | F | T | Get on ALL object tables
		// 7 | F | F | F | Scan ALL subject tables

		// toggle the axis iif matching criteria {1,6}
		if (!s.isConcrete() && o.isConcrete())
			axis = TableAxis.Object;

		if (p.isConcrete()) {
			// criteria {0-3}
			try {
				Table table = store.getTable(axis, p);
				results = table.get(s, o);
			} catch (Exception e) {
				// failed to garner query scope so bail with no results
				return NullIterator.instance();
			}
		} else {
			// criteria {4-7}
			for (Table table : store.allTables(axis)) {
				ExtendedIterator<Triple> intermediate =table.get(s, o);
				
				if(intermediate.getClass()==NullIterator.class){
					intermediate.close();
					continue;
				}
				
				results = results.andThen(intermediate);
//				if (results != null) {
//					results.andThen(intermediate);
//				} else {
//					results = intermediate;
//				}
			}
		}
		if (results == null)
			results = NullIterator.instance();
		return results;
	}

	public boolean isIsomorphicWith(com.hp.hpl.jena.graph.Graph g) {
		System.out.println("isIsomorphicWith(Graph)");
		return false;
	}

	public boolean contains(Node s, Node p, Node o) {
		System.out.println("contains(Node,Node,Node)");
		return false;
	}

	public boolean contains(Triple t) {
		System.out.println("contains(Triple)");
		return false;
	}

	public void clear() {
		System.out.println("clear");

	}

	public void remove(Node s, Node p, Node o) {
		System.out.println("remove(Node,Node,Node)");

	}

	public void close() {
		System.out.println("close");
		store.sync();
	}

	public boolean isEmpty() {
		System.out.println("isEmpty");
		return false;
	}

	public int size() {
		System.out.println("size");
		return 0;
	}

	public boolean isClosed() {
		System.out.println("isClosed");
		return false;
	}

	public ExtendedIterator<Triple> find(TripleMatch m) {
		return find(m.getMatchSubject(), m.getMatchPredicate(),
				m.getMatchObject());
	}
}
