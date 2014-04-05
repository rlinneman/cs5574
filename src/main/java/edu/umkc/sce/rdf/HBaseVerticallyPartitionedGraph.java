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

import java.io.IOException;

import com.hp.hpl.jena.graph.BulkUpdateHandler;
import com.hp.hpl.jena.graph.Capabilities;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphEventManager;
import com.hp.hpl.jena.graph.GraphStatisticsHandler;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.TransactionHandler;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.SimpleEventManager;
import com.hp.hpl.jena.shared.AddDeniedException;
import com.hp.hpl.jena.shared.Command;
import com.hp.hpl.jena.shared.DeleteDeniedException;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.shared.impl.PrefixMappingImpl;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NiceIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.sun.tools.corba.se.idl.InvalidArgument;

/** Wraps the logic of translating a {@link Triple} to a {@link Partition} */
public class HBaseVerticallyPartitionedGraph implements
        com.hp.hpl.jena.graph.Graph {

    private static final PartitionAxis[] coreAxis = new PartitionAxis[] {
            PartitionAxis.Subject, PartitionAxis.Object };

    private HBaseStore hBaseStore;
    private PrefixMapping prefixMapping;
    private TransactionHandler transactionHandler;
    private GraphEventManager graphEventManager;

    public HBaseVerticallyPartitionedGraph(HBaseStore hBaseStore) {
        this.hBaseStore = hBaseStore;
        this.prefixMapping = new PrefixMappingImpl();
        this.transactionHandler = new /* SimpleTransactionHandler() */TransactionHandler() {

            @Override
            public boolean transactionsSupported() {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public Object executeInTransaction(Command c) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public void commit() {
                // TODO Auto-generated method stub

            }

            @Override
            public void begin() {
                // TODO Auto-generated method stub

            }

            @Override
            public void abort() {
                // TODO Auto-generated method stub

            }
        };
        this.graphEventManager = new SimpleEventManager(this);
    }

    /**
     * Inserts {@link Triple}'s indexed by Subject and again by Object in
     * {@link Partition}'s identified by Predicate and Graph
     * 
     * <p>
     * Note that as of this version only the default graph (unnamed graph) is
     * supported..
     * </p>
     */
    public void add(Triple t) throws AddDeniedException {
        Node s = t.getSubject(), p = t.getPredicate(), o = t.getObject();

        for (PartitionAxis axis : coreAxis) {
            Partition partition = null;
            try {
                partition = hBaseStore.getPartition(axis, p);
            } catch (IOException | InvalidArgument e) {
                e.printStackTrace();
            }
            if (partition == null)
                throw new AddDeniedException("Failed to get partition", t);

            partition.put(s, o);
        }
    }

    /**
     * Resolves the {@link Partition}(s) to process for a given query and
     * executes the given query against said Partition(s).
     */
    public ExtendedIterator<Triple> find(Node s, Node p, Node o) {
        ExtendedIterator<Triple> results = new NiceIterator<Triple>();
        PartitionAxis axis = PartitionAxis.Subject;

        // # | S | P | O | Axis
        // ------------------------------------------------------------
        // 0 | T | T | T | Subject
        // 1 | F | T | T | Object
        // 2 | T | T | F | Subject
        // 3 | F | T | F | Subject
        // 4 | T | F | T | *ALL* Subject
        // 5 | T | F | F | *ALL* Subject
        // 6 | F | F | T | *ALL* Object
        // 7 | F | F | F | *ALL* Subject

        // toggle the axis iif matching criteria {1,6}
        if (!s.isConcrete() && o.isConcrete())
            axis = PartitionAxis.Object;

        if (p.isConcrete()) {
            // criteria {0-3}
            try {
                Partition partition = hBaseStore.getPartition(axis, p);
                results = partition.get(s, o);
            } catch (Exception e) {
                // failed to garner query scope so bail with no results
                return NullIterator.instance();
            }
        } else {
            // criteria {4-7}
            for (Partition partition : hBaseStore.allPartitions(axis)) {
                ExtendedIterator<Triple> intermediate = partition.get(s, o);

                if (intermediate.getClass() == NullIterator.class) {
                    intermediate.close();
                    continue;
                }

                results = results.andThen(intermediate);
            }
        }
        if (results == null)
            results = NullIterator.instance();
        return results;
    }

    public ExtendedIterator<Triple> find(TripleMatch m) {
        return find(m.getMatchSubject(), m.getMatchPredicate(),
                m.getMatchObject());
    }

    public TransactionHandler getTransactionHandler() {
        return transactionHandler;
    }

    public BulkUpdateHandler getBulkUpdateHandler() {
        return null;
    }

    public GraphEventManager getEventManager() {
        return graphEventManager;
    }

    public PrefixMapping getPrefixMapping() {
        return this.prefixMapping;
    }

    public void close() {
        hBaseStore.sync();
    }

    public boolean dependsOn(Graph other) {
        throw new UnsupportedOperationException();
    }

    public Capabilities getCapabilities() {
        return null;
    }

    public GraphStatisticsHandler getStatisticsHandler() {
        throw new UnsupportedOperationException();
    }

    public void delete(Triple t) throws DeleteDeniedException {
        throw new UnsupportedOperationException();
    }

    public boolean isIsomorphicWith(com.hp.hpl.jena.graph.Graph g) {
        throw new UnsupportedOperationException();
    }

    public boolean contains(Node s, Node p, Node o) {
        throw new UnsupportedOperationException();
    }

    public boolean contains(Triple t) {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        throw new UnsupportedOperationException();
    }

    public void remove(Node s, Node p, Node o) {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    public int size() {
        throw new UnsupportedOperationException();
    }

    public boolean isClosed() {
        throw new UnsupportedOperationException();
    }
}
