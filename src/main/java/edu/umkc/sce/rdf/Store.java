package edu.umkc.sce.rdf;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class Store {
	private static final String RDF_NAMESPACE = "rdf";
	private final HBaseAdmin admin;
	private final Configuration conf;
	
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

    final ExecutorService executor = Executors.newFixedThreadPool(7);
		
	public void format() throws IOException{
		StopWatch sw = new StopWatch();
		sw.start();
		for (TableName tableName : admin.listTableNamesByNamespace(RDF_NAMESPACE)) {
			System.out.printf("Dropping %s\n", tableName.getNameAsString());
			deleteTable(tableName);
		}		
		try {
			System.out.print("Awaiting table drops to complete...");
			executor.shutdown();
			executor.awaitTermination(1, TimeUnit.HOURS);
			sw.stop();
			System.out.printf("%dS", sw.getTime()/1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void deleteTable(final TableName tableName){
		executor.execute(new Runnable(){

			public void run() {
try{
				if (admin.isTableEnabled(tableName)) {
					admin.disableTable(tableName);
				}
				admin.deleteTable(tableName);
}catch(IOException e){
}
			}
			
		});
	}
}
