package edu.umkc.sce.rdf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class Store {
	private static final String RDF_NAMESPACE = "rdf";
	private HBaseAdmin admin;
	private Configuration conf;
	
//	public StoreVP( HBaseRdfConnection connection, StoreDesc desc )
//    {
//        super( connection, desc, new QueryRunnerBase( desc.getStoreName(), connection, QueryRunnerVP.class ), 
//        	   new FmtLayoutVP( desc.getStoreName(), connection ), 
//        	   new LoaderTuplesNodes( desc.getStoreName(), connection, TupleLoaderVP.class ) ) ;
//        ( (LoaderTuplesNodes) this.getLoader() ).setStore( this ) ;
//    }    

	public Store(Configuration conf) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		this.conf = conf;
		this.admin = new HBaseAdmin(conf);
	}
		
	public void format() throws IOException{
		for (TableName tableName : admin.listTableNamesByNamespace(RDF_NAMESPACE)) {
			System.out.printf("Dropping %s\n", tableName.getNameAsString());
			if (admin.isTableEnabled(tableName)) {
				admin.disableTable(tableName);
			}
			admin.deleteTable(tableName);
		}		
	}
}
