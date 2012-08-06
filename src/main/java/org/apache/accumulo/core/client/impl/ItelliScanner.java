package org.apache.accumulo.core.client.impl;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ResultReceiverIfc;
import org.apache.accumulo.core.client.Sink;
import org.apache.accumulo.core.client.Source;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ArgumentChecker;

public class ItelliScanner extends ScannerOptions implements BatchScanner {
	 protected String table;
	  protected int numThreads;
	  protected ExecutorService queryThreadPool;
	  
	  protected Instance instance;
	  protected ArrayList<Range> ranges;
	  
	  protected AuthInfo credentials;
	  protected Authorizations authorizations = Constants.NO_AUTHS;
	  
	  protected static int nextBatchReaderInstance = 1;
	  
	  protected static synchronized int getNextBatchReaderInstance() {
	    return nextBatchReaderInstance++;
	  }
	  
	  protected int batchReaderInstance = getNextBatchReaderInstance();
	  
	  protected Class<? extends ResultReceiverIfc> receiver;
	  
	  protected Sink sink;
	  
	  protected Source source;
	  
	  protected class BatchReaderThreadFactory implements ThreadFactory {
	    
	    protected ThreadFactory dtf = Executors.defaultThreadFactory();
	    protected int threadNum = 1;
	    
	    public Thread newThread(Runnable r) {
	      Thread thread = dtf.newThread(r);
	      thread.setName("batch scanner " + batchReaderInstance + "-" + threadNum++);
	      thread.setDaemon(true);
	      return thread;
	    }
	    
	  }
	  
	  public ItelliScanner(
			  Instance instance, 
			  AuthInfo credentials, 
			  String table, 
			  Authorizations authorizations, 
			  int numQueryThreads,
			  Class<? extends ResultReceiverIfc> receiverClass,
			  Sink sink,
			  Source source) {
	    ArgumentChecker.notNull(instance, credentials, table, authorizations);
	    this.instance = instance;
	    this.credentials = credentials;
	    this.authorizations = authorizations;
	    this.receiver = receiverClass;
	    this.sink = sink;
	    this.source = source;
	    this.table = table;
	    this.numThreads = numQueryThreads;
	    
	    queryThreadPool = new ThreadPoolExecutor(numQueryThreads, numQueryThreads, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
	        new BatchReaderThreadFactory());
	    
	    ranges = null;
	  }
	  
	  public void close() {
	    queryThreadPool.shutdownNow();
	  }
	  
	  public void setRanges(Collection<Range> ranges) {
	    if (ranges == null || ranges.size() == 0) {
	      throw new IllegalArgumentException("ranges must be non null and contain at least 1 range");
	    }
	    
	    if (queryThreadPool.isShutdown()) {
	      throw new IllegalStateException("batch reader closed");
	    }
	    
	    this.ranges = new ArrayList<Range>(ranges);
	    
	  }
	  
	  @Override
	  public Iterator<Entry<Key,Value>> iterator() {
	    if (ranges == null) {
	      throw new IllegalStateException("ranges not set");
	    }
	    
	    if (queryThreadPool.isShutdown()) {
	      throw new IllegalStateException("batch reader closed");
	    }
	    
	    
	    ResultReceiverIfc rcvr;
		try {
			rcvr = receiver.getConstructor(Sink.class).newInstance(sink);
			
			 SonOfaBatchIterator iter = new SonOfaBatchIterator(
			    		rcvr,instance, credentials, table, authorizations, ranges, numThreads, queryThreadPool, this);
			 
			 
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	   
	    
	    return source.iterator();
	  }
}
