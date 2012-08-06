package org.apache.accumulo.core.client.impl;

import org.apache.accumulo.core.client.ResultReceiverIfc;
import org.apache.accumulo.core.client.Sink;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

public class ResultReceiver implements ResultReceiverIfc{

	private static final Logger log = Logger.getLogger(ResultReceiver.class);
	protected Sink sink = null;
	
	//protected final ExecutorService queryThreadPoolRef = null;
	
	public ResultReceiver(
			final Sink sink)
	{
		this.sink = sink;
	}
	
	 public void receive(ScanTask task,Key key, Value value) {
	        sink.put(task,key,value);
        //resultsQueue.put(new KeyValue(key, value));
	      }

	 	
}
