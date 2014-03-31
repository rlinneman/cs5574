package edu.umkc.sce.rdf;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class PrefixMapping implements com.hp.hpl.jena.shared.PrefixMapping {

	private final HashMap<String, String> map = new HashMap<String, String>();
	private HBaseAdmin admin;
	private Configuration conf;

	public PrefixMapping(HBaseAdmin admin, Configuration conf) {
		this.admin = admin;
		this.conf = conf;
	}

	public com.hp.hpl.jena.shared.PrefixMapping setNsPrefix(String prefix,
			String uri) {
		if (map.containsKey(prefix)) {
			map.remove(prefix);
		}
		map.put(prefix, uri);
		return this;
	}

	public com.hp.hpl.jena.shared.PrefixMapping removeNsPrefix(String prefix) {
		if (map.containsKey(prefix)) {
			map.remove(prefix);
		}
		return this;
	}

	public com.hp.hpl.jena.shared.PrefixMapping setNsPrefixes(
			com.hp.hpl.jena.shared.PrefixMapping other) {
		setNsPrefixes(other.getNsPrefixMap());
		return this;
	}

	public com.hp.hpl.jena.shared.PrefixMapping setNsPrefixes(
			Map<String, String> map) {

		this.map.clear();
		for (String key : map.keySet()) {
			this.map.put(key, map.get(key));
		}
		return this;
	}

	public com.hp.hpl.jena.shared.PrefixMapping withDefaultMappings(
			com.hp.hpl.jena.shared.PrefixMapping map) {
		// TODO Auto-generated method stub
		return this;
	}

	public String getNsPrefixURI(String prefix) {
		return map.get(prefix);
	}

	public String getNsURIPrefix(String uri) {
		if (map.containsValue(uri)) {
			for (String key : map.keySet()) {
				if (map.get(key) == uri)
					return key;
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public Map<String, String> getNsPrefixMap() {
		return (Map<String, String>) map.clone();
	}

	public String expandPrefix(String prefixed) {
		// TODO Auto-generated method stub
		return null;
	}

	public String shortForm(String uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public String qnameFor(String uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public com.hp.hpl.jena.shared.PrefixMapping lock() {
		throw new NotImplementedException();
		//return this;
	}

	public boolean samePrefixMappingAs(
			com.hp.hpl.jena.shared.PrefixMapping other) {
		// TODO Auto-generated method stub
		return false;
	}

}
