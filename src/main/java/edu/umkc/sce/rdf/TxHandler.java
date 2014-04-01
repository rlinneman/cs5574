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
