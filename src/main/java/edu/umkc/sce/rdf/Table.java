package edu.umkc.sce.rdf;

import org.apache.hadoop.hbase.TableName;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

public interface Table {
	public void put(Node s, Node o);
	public ExtendedIterator<Triple> get(Node s, Node o);
	public boolean exists();
	public boolean create();
	public TableAxis getAxis();
	public void flush();
	public Node getPredicate();
	public TableName getName();
}

