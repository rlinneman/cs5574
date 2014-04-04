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

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.shared.impl.JenaParameters;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NiceIterator;

class ResultTripleIterator implements Iterator<Triple> {
	final Node subject, predicate, object;
	final Result result;
	final Iterator<byte[]> columns;
	final String rowKey;

	public ResultTripleIterator(Result result, Node subject, Node predicate,
			Node object, TableName tableName) {
		this.subject = subject;
		this.predicate = predicate.isConcrete() ? predicate
				: getNode(tableName);
		this.object = object;
		this.result = result;
		// We use existence checks rather than values in the storage of this
		// schema, so the values in getFamilyMap will always be an empty
		// string. The actual value comes from the column qualifier
		this.columns = result.getFamilyMap("nodes".getBytes()).keySet()
				.iterator();
		this.rowKey = Bytes.toString(result.getRow());
	}

	//
	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#hasNext()
	 */
	public boolean hasNext() {
		boolean hasNext = columns.hasNext();
		// System.out.printf("ResultTripleIterator:hasNext=%s\n", hasNext);
		return hasNext;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#next()
	 */
	public Triple next() {
		return moveNext();
	}

	/**
	 * 
	 */
	private Triple moveNext() {
		// System.out.println("ResultTripleIterator:moveNext");
		String columnName = Bytes.toString(columns.next());

		// S | P | O | ACTION
		// ------------------------------------------------------------
		// T | T | T | Get a subject table
		// F | T | T | Get an object table
		// T | T | F | Get a subject table
		// F | T | F | Scan a subject table
		// T | F | T | Get on ALL subject tables
		// T | F | F | Get on ALL subject tables
		// F | F | T | Get on ALL object tables
		// F | F | F | Scan ALL subject tables

		Triple triple;
		Node s, o;

		if (subject.isConcrete() || !object.isConcrete()) {
			// where fell on the subjects axis
			s = getNode(rowKey);
			o = getNode(columnName);
		} else {
			o = getNode(rowKey);
			s = getNode(columnName);
		}

		triple = Triple.create(s, predicate, o);

		return triple;
	}

	/**
	 * A method that converts a string representation into a Node courtesy of
	 * Copyright © 2010, 2011, 2012 Talis Systems Ltd.
	 * 
	 * @param strNode
	 *            - the string representation as fetched from the HTable
	 * @return a Node representation of the given string
	 */
	public static Node getNode(TableName tableName) {
		Node node = null;

		return node;
	}

	/**
	 * A method that converts a string representation into a Node courtesy of
	 * Copyright © 2010, 2011, 2012 Talis Systems Ltd.
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#remove()
	 */

	public void remove() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

}

class ResultScannerTripleIterator implements Iterator<Triple> {
	private final Node subject, predicate, object;
	private final TableName tableName;
	private final Iterator<Result> scanner;
	private ResultTripleIterator current;

	public ResultScannerTripleIterator(ResultScanner scanner, Node subject,
			Node predicate, Node object, TableName tableName) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.scanner = scanner.iterator();
		this.tableName = tableName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#hasNext()
	 */

	public boolean hasNext() {
		boolean hasNext = scanner.hasNext()
				|| (current != null && current.hasNext());
		// System.out.printf("ResultSetTripleIterator:hasNext=%s\n", hasNext);
		return hasNext;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#next()
	 */

	public Triple next() {
		// System.out.println("ResultSetTripleIterator:moveNext");
		if (current != null && current.hasNext())
			return current.next();
		else if (hasNext()) {
			current = new ResultTripleIterator(scanner.next(), subject,
					predicate, object, tableName);
			return next();
		} else
			return null;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.Iterator#remove()
	 */

	public void remove() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

}
