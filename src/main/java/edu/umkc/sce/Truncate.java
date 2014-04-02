package edu.umkc.sce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umkc.sce.rdf.Store;

public class Truncate extends Configured implements Tool {
	private static final String MY_NAMESPACE = "foo";

	public static void main(String[] args) {		
		Configuration conf = new Configuration();
		int result;
		try {
			result = ToolRunner.run(conf, new Truncate(), args);
		} catch (Exception e) {
			e.printStackTrace();
			result = -1;
		}
		System.exit(result);
	}

	public int run(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {

		Store store = new Store(getConf());
		store.format();
		return 0;
	}
}
