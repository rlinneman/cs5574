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

import com.hp.hpl.jena.graph.TransactionHandler;
import com.hp.hpl.jena.shared.Command;

public class TxHandler implements TransactionHandler {
	private final Graph graph;

	public TxHandler(Graph graph) {
		this.graph = graph;
	}

	public boolean transactionsSupported() {
		return false;
	}

	public void begin() {
		// TODO Auto-generated method stub
		System.out.println("TxHandler.begin");
	}

	public void abort() {
		// TODO Auto-generated method stub
		System.out.println("TxHandler.abort");
	}

	public void commit() {
		// TODO Auto-generated method stub
		System.out.println("TxHandler.commit");
	}

	public Object executeInTransaction(Command c) {
		// TODO Auto-generated method stub
		System.out.println("TxHandler.executeInTransaction(Command)");
		return null;
	}

}
