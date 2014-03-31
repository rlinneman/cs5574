package edu.umkc.sce.rdf;

import com.hp.hpl.jena.graph.TransactionHandler;
import com.hp.hpl.jena.shared.Command;

public class TxHandler implements TransactionHandler {

	public boolean transactionsSupported() {
		return false;
	}

	public void begin() {
		// TODO Auto-generated method stub
		System.out.println("begin");
	}

	public void abort() {
		// TODO Auto-generated method stub
		System.out.println("abort");
	}

	public void commit() {
		// TODO Auto-generated method stub
		System.out.println("commit");
		
	}

	public Object executeInTransaction(Command c) {
		// TODO Auto-generated method stub
		System.out.println("executeInTransaction(Command)");
		return null;
	}

}
