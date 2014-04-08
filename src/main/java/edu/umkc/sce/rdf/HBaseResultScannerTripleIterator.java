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
 */

package edu.umkc.sce.rdf;

import java.util.Iterator;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

/** Iterates a collection of rows from HBase */
class HBaseResultScannerTripleIterator implements Iterator<Triple> {
	private final Node subject, predicate, object;
	private final TableName tableName;
	private final Iterator<Result> scanner;
	private Iterator<Triple> current;

	public HBaseResultScannerTripleIterator(ResultScanner scanner, Node subject,
			Node predicate, Node object, TableName tableName) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.scanner = scanner.iterator();
		this.tableName = tableName;
	}

	public boolean hasNext() {
		boolean hasNext = scanner.hasNext()
				|| (current != null && current.hasNext());
		// System.out.printf("ResultSetTripleIterator:hasNext=%s\n", hasNext);
		return hasNext;
	}


	public Triple next() {
		// System.out.println("ResultSetTripleIterator:moveNext");
		if (current != null && current.hasNext())
			return current.next();
		else if (hasNext()) {
			current = new HBaseResultIterator(scanner.next(), subject,
					predicate, object, tableName);
			return next();
		} else
			return null;

	}

	public void remove() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

}