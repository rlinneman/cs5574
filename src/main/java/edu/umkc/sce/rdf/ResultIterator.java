package edu.umkc.sce.rdf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.shared.impl.JenaParameters;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Filter;
import com.hp.hpl.jena.util.iterator.Map1;

public class ResultIterator implements ExtendedIterator<Triple> {
	/*
	 * credit to Copyright © 2010, 2011, 2012 Talis Systems Ltd. for the body of
	 * this class. just trying to get query up and running to investigate SDB
	 * plumbing will drop this implementation for hand rolled one after verified
	 * as functional
	 */
	/** The subject, predicate and object in the given TripleMatch **/
	Node subject = null, predicate = null, object = null;

	/** An iterator over all column names for a given row **/
	Iterator<byte[]> iterColumnNames = null;

	/** The HBase table name over which this iterator runs **/
	String pred = null;

	/** The row over whose columns this iterator iterates **/
	String row = null;

	/**
	 * Constructor
	 * 
	 * @param rr
	 *            - a row fetched from a HTable
	 * @param sm
	 *            - the subject of the triple to be matched
	 * @param pm
	 *            - the predicate of the triple to be matched
	 * @param om
	 *            - the object of the triple to be matched
	 */
	public ResultIterator(Result rr, Node sm, Node pm, Node om, String pred,
			String columnFamily) {
		this.subject = sm;
		this.predicate = pm;
		this.object = om;
		this.pred = pred;
		this.row = Bytes.toString(rr.getRow());
		this.iterColumnNames = rr.getFamilyMap(columnFamily.getBytes())
				.keySet().iterator();
	}

	/**
	 * @see com.talis.hbase.rdf.iterator.AbstractIterator#_next()
	 */
	public Triple _next() {
		Triple tr = null;

		// Iterate while triples still exist in the current row
		while (tr == null && iterColumnNames.hasNext()) {
			Node trSubject = subject, trPredicate = predicate, trObject = object;

			// Get the current object
			String n = new String(iterColumnNames.next());

			// If subject is concrete or triple pattern is of the form ( ANY
			// @ANY ANY )
			// Else predicate or object processing
			if (subject.isConcrete()
					|| (subject.equals(Node.ANY) && predicate.equals(Node.ANY) && object
							.equals(Node.ANY))) {
				trSubject = getNode(row);
				trPredicate = getNode(pred);
				trObject = getNode(n);
				if ((predicate.equals(Node.ANY) || ((predicate != Node.ANY) && predicate
						.equals(trPredicate)))
						&& (object.equals(Node.ANY) || ((object != Node.ANY) && object
								.equals(trObject))))
					tr = Triple.create(trSubject, trPredicate, trObject);
			} else if (object.isConcrete()) {
				trSubject = getNode(n);
				trPredicate = getNode(pred);
				trObject = getNode(row);
				if ((subject.equals(Node.ANY) || ((subject != Node.ANY) && subject
						.equals(trSubject)))
						&& (predicate.equals(Node.ANY) || ((predicate != Node.ANY) && predicate
								.equals(trPredicate))))
					tr = Triple.create(trSubject, trPredicate, trObject);
			} else if (predicate.isConcrete()) {
				trSubject = getNode(row);
				trPredicate = getNode(pred);
				trObject = getNode(n);
				if ((subject.equals(Node.ANY) || ((subject != Node.ANY) && subject
						.equals(trSubject)))
						&& (object.equals(Node.ANY) || ((object != Node.ANY) && object
								.equals(trObject))))
					tr = Triple.create(trSubject, trPredicate, trObject);
			}
			// Clear memory
			n = null;
		}
		return tr;
	}

	/**
	 * A method that converts a string representation into a Node
	 * 
	 * @param strNode
	 *            - the string representation as fetched from the HTable
	 * @return a Node representation of the given string
	 */
	public static Node getNode(String strNode) {
		Node node = null;
		if (strNode.startsWith("\"", 0)) {
			String remParts = strNode.substring(strNode.lastIndexOf("\"") + 1);
			strNode = strNode.substring(1, strNode.lastIndexOf("\""));
			if (remParts.equalsIgnoreCase(""))
				node = Node.createLiteral(strNode);
			else {
				String[] parts = remParts.split("\\^\\^");
				String lang = parts[0].replaceFirst("@", "");
				String type = null;
				if (parts.length == 2)
					type = parts[1];
				else
					type = "";
				node = Node.createLiteral(strNode, lang, TypeMapper
						.getInstance().getTypeByName(type));
			}
		} else if (strNode.startsWith("_", 0)
				|| (JenaParameters.disableBNodeUIDGeneration == true && strNode
						.startsWith("A")))
			node = Node.createAnon(new AnonId(strNode));
		else
			node = Node.createURI(strNode);

		return node;
	}

	private Triple obj = null;

	public void close() {
	}

	public boolean hasNext() {
		if (obj == null)
			obj = _next();
		return obj != null;
	}

	public Triple next() {
		if (obj == null) {
			obj = _next();
			if (obj == null) {
				throw new NoSuchElementException();
			}
		}
		Triple result = obj;
		obj = null;
		return result;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	public Triple removeNext() {
		throw new UnsupportedOperationException();
	}

	public List<Triple> toList() {
		List<Triple> list = new ArrayList<Triple>();
		while (hasNext()) {
			list.add(next());
		}
		return list;
	}

	public Set<Triple> toSet() {
		Set<Triple> set = new HashSet<Triple>();
		while (hasNext()) {
			set.add(next());
		}
		return set;
	}

	public <X extends Triple> ExtendedIterator<Triple> andThen(Iterator<X> other) {
		// TODO Auto-generated method stub
		return null;
	}

	public ExtendedIterator<Triple> filterKeep(Filter<Triple> f) {
		// TODO Auto-generated method stub
		return null;
	}

	public ExtendedIterator<Triple> filterDrop(Filter<Triple> f) {
		// TODO Auto-generated method stub
		return null;
	}

	public <U> ExtendedIterator<U> mapWith(Map1<Triple, U> map1) {
		// TODO Auto-generated method stub
		return null;
	}
}
