package edu.umkc.sce.rdf;

import java.util.Iterator;
import java.util.List;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphEventManager;
import com.hp.hpl.jena.graph.GraphListener;
import com.hp.hpl.jena.graph.Triple;

public class GraphEventMgr implements GraphEventManager{

	public void notifyAddTriple(Graph g, Triple t) {
		// TODO Auto-generated method stub
		
	}

	public void notifyAddArray(Graph g, Triple[] triples) {
		// TODO Auto-generated method stub
		
	}

	public void notifyAddList(Graph g, List<Triple> triples) {
		// TODO Auto-generated method stub
		
	}

	public void notifyAddIterator(Graph g, Iterator<Triple> it) {
		// TODO Auto-generated method stub
		
	}

	public void notifyAddGraph(Graph g, Graph added) {
		// TODO Auto-generated method stub
		
	}

	public void notifyDeleteTriple(Graph g, Triple t) {
		// TODO Auto-generated method stub
		
	}

	public void notifyDeleteList(Graph g, List<Triple> L) {
		// TODO Auto-generated method stub
		
	}

	public void notifyDeleteArray(Graph g, Triple[] triples) {
		// TODO Auto-generated method stub
		
	}

	public void notifyDeleteIterator(Graph g, Iterator<Triple> it) {
		// TODO Auto-generated method stub
		
	}

	public void notifyDeleteGraph(Graph g, Graph removed) {
		// TODO Auto-generated method stub
		
	}

	public void notifyEvent(Graph source, Object value) {
		// TODO Auto-generated method stub
		
	}

	public GraphEventManager register(GraphListener listener) {
		// TODO Auto-generated method stub
		return null;
	}

	public GraphEventManager unregister(GraphListener listener) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean listening() {
		// TODO Auto-generated method stub
		return false;
	}

	public void notifyAddIterator(Graph g, List<Triple> triples) {
		// TODO Auto-generated method stub
		
	}

	public void notifyDeleteIterator(Graph g, List<Triple> triples) {
		// TODO Auto-generated method stub
		
	}

}
