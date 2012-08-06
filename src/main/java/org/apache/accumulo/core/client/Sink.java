package org.apache.accumulo.core.client;

import org.apache.accumulo.core.client.impl.ScanTask;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public interface Sink {

	void register(ScanTask task);
	
	void put(ScanTask task, Key key, Value value);
	
	//Entry<Key,Value> 

}
