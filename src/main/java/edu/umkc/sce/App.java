package edu.umkc.sce;

import org.apache.hadoop.hbase.HBaseConfiguration;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	Model m = ModelFactory.createDefaultModel();
    	HBaseConfiguration c;
        System.out.println( "Hello World!" );
    }
}
