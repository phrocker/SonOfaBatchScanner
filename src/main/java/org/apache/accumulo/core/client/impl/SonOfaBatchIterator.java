package org.apache.accumulo.core.client.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.cloudtrace.instrument.TraceRunnable;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ItelliProcessor;
import org.apache.accumulo.core.client.ResultReceiverIfc;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.TableUtils;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;


public class SonOfaBatchIterator implements Iterator<Entry<Key,Value>>, ItelliProcessor {
	  
	  private static final Logger log = Logger.getLogger(TabletServerBatchReaderIterator.class);
	  
	  private final Instance instance;
	  private final AuthInfo credentials;
	  private final String table;
	  private Authorizations authorizations = Constants.NO_AUTHS;
	  private final int numThreads;
	  private final ExecutorService queryThreadPool;
	  private final ScannerOptions options;
	  
	  private ArrayBlockingQueue<Entry<Key,Value>> resultsQueue = new ArrayBlockingQueue<Entry<Key,Value>>(1000);
	  private Entry<Key,Value> nextEntry = null;
	  private Object nextLock = new Object();
	  
	  private long failSleepTime = 100;
	  
	  private volatile Throwable fatalException = null;
	  
	
	  
	  
	  
	  public SonOfaBatchIterator(
			  ResultReceiverIfc resRec,
			  Instance instance, 
			  AuthInfo credentials, String table, Authorizations authorizations, ArrayList<Range> ranges,
	      int numThreads, ExecutorService queryThreadPool, ScannerOptions scannerOptions) throws TableNotFoundException {
	    
	    this.instance = instance;
	    this.credentials = credentials;
	    this.table = TableUtils.getTableId(instance,table);
	    this.authorizations = authorizations;
	    this.numThreads = numThreads;
	    this.queryThreadPool = queryThreadPool;
	    this.options = new ScannerOptions(scannerOptions);
	    
	    if (options.fetchedColumns.size() > 0) {
	      ArrayList<Range> ranges2 = new ArrayList<Range>(ranges.size());
	      for (Range range : ranges) {
	        ranges2.add(range.bound(options.fetchedColumns.first(), options.fetchedColumns.last()));
	      }
	      
	      ranges = ranges2;
	    }
	    
	    
	    /*
	    
	      ResultReceiverIfc rr = resRec;
	      public void receive(Key key, Value value) {
	        try {
	          resultsQueue.put(new KeyValue(key, value));
	        } catch (InterruptedException e) {
	          if (SonOfaBatchIterator.this.queryThreadPool.isShutdown())
	            log.debug("Failed to add Batch Scan result for key " + key, e);
	          else
	            log.warn("Failed to add Batch Scan result for key " + key, e);
	          fatalException = e;
	          throw new RuntimeException(e);
	          
	        }
	      }
	      
	    };
	    */
	    
	    try {
	      lookup(ranges, resRec);
	    } catch (RuntimeException re) {
	      throw re;
	    } catch (Exception e) {
	      throw new RuntimeException("Failed to create iterator", e);
	    }
	  }
	  
	  public boolean hasNext() {
	    synchronized (nextLock) {
	      // check if one was cached
	      if (nextEntry != null)
	        return nextEntry.getKey() != null && nextEntry.getValue() != null;
	      
	      // don't have one cached, try to cache one and return success
	      try {
	        while (nextEntry == null && fatalException == null && !queryThreadPool.isShutdown())
	          nextEntry = resultsQueue.poll(1, TimeUnit.SECONDS);
	        
	        if (fatalException != null)
	          if (fatalException instanceof RuntimeException)
	            throw (RuntimeException) fatalException;
	          else
	            throw new RuntimeException(fatalException);
	        
	        if (queryThreadPool.isShutdown())
	          throw new RuntimeException("scanner closed");

	        return nextEntry.getKey() != null && nextEntry.getValue() != null;
	      } catch (InterruptedException e) {
	        throw new RuntimeException(e);
	      }
	    }
	  }
	  
	  public Entry<Key,Value> next() {
	    Entry<Key,Value> current = null;
	    
	    // if there's one waiting, or hasNext() can get one, return it
	    synchronized (nextLock) {
	      if (hasNext()) {
	        current = nextEntry;
	        nextEntry = null;
	      }
	    }
	    
	    return current;
	  }
	  
	  public void remove() {
	    throw new UnsupportedOperationException();
	  }
	  
	  private synchronized void lookup(List<Range> ranges, ResultReceiverIfc receiver) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
	    List<Column> columns = new ArrayList<Column>(options.fetchedColumns);
	    ranges = Range.mergeOverlapping(ranges);
	    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
	    
	    binRanges(TabletLocator.getInstance(instance, credentials, new Text(table)), ranges, binnedRanges);
	    
	    doLookups(binnedRanges, receiver, columns);
	  }
	  
	  private void binRanges(TabletLocator tabletLocator, List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException,
	      AccumuloSecurityException, TableNotFoundException {
	    
	    int lastFailureSize = Integer.MAX_VALUE;
	    
	    while (true) {
	      
	      binnedRanges.clear();
	      List<Range> failures = tabletLocator.binRanges(ranges, binnedRanges);
	      
	      if (failures.size() > 0) {
	        // tried to only do table state checks when failures.size() == ranges.size(), however this did
	        // not work because nothing ever invalidated entries in the tabletLocator cache... so even though
	        // the table was deleted the tablet locator entries for the deleted table were not cleared... so
	        // need to always do the check when failures occur
	        if (failures.size() >= lastFailureSize)
	          if (!Tables.exists(instance, table))
	            throw new TableDeletedException(table);
	          else if (Tables.getTableState(instance, table) == TableState.OFFLINE)
	            throw new TableOfflineException(instance, table);
	        
	        lastFailureSize = failures.size();
	        
	        if (log.isTraceEnabled())
	          log.trace("Failed to bin " + failures.size() + " ranges, tablet locations were null, retrying in 100ms");
	        try {
	          Thread.sleep(100);
	        } catch (InterruptedException e) {
	          throw new RuntimeException(e);
	        }
	      } else {
	        break;
	      }
	      
	    }
	    
	    // truncate the ranges to within the tablets... this makes it easier to know what work
	    // needs to be redone when failures occurs and tablets have merged or split
	    Map<String,Map<KeyExtent,List<Range>>> binnedRanges2 = new HashMap<String,Map<KeyExtent,List<Range>>>();
	    for (Entry<String,Map<KeyExtent,List<Range>>> entry : binnedRanges.entrySet()) {
	      Map<KeyExtent,List<Range>> tabletMap = new HashMap<KeyExtent,List<Range>>();
	      binnedRanges2.put(entry.getKey(), tabletMap);
	      for (Entry<KeyExtent,List<Range>> tabletRanges : entry.getValue().entrySet()) {
	        Range tabletRange = tabletRanges.getKey().toDataRange();
	        List<Range> clippedRanges = new ArrayList<Range>();
	        tabletMap.put(tabletRanges.getKey(), clippedRanges);
	        for (Range range : tabletRanges.getValue())
	          clippedRanges.add(tabletRange.clip(range));
	      }
	    }
	    
	    binnedRanges.clear();
	    binnedRanges.putAll(binnedRanges2);
	  }
	  
	  public void processFailures(Map<KeyExtent,List<Range>> failures, ResultReceiverIfc receiver, List<Column> columns) throws AccumuloException,
	      AccumuloSecurityException, TableNotFoundException {
	    if (log.isTraceEnabled())
	      log.trace("Failed to execute multiscans against " + failures.size() + " tablets, retrying...");
	    
	    UtilWaitThread.sleep(failSleepTime);
	    failSleepTime = Math.min(5000, failSleepTime * 2);
	    
	    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
	    List<Range> allRanges = new ArrayList<Range>();
	    
	    for (List<Range> ranges : failures.values())
	      allRanges.addAll(ranges);
	    
	    TabletLocator tabletLocator = TabletLocator.getInstance(instance, credentials, new Text(table));
	    
	    // since the first call to binRanges clipped the ranges to within a tablet, we should not get only
	    // bin to the set of failed tablets
	    binRanges(tabletLocator, allRanges, binnedRanges);
	    
	    doLookups(binnedRanges, receiver, columns);
	  }
	  
	  
	  
	  private void doLookups(Map<String,Map<KeyExtent,List<Range>>> binnedRanges, final ResultReceiverIfc receiver, List<Column> columns) {
	    // when there are lots of threads and a few tablet servers
	    // it is good to break request to tablet servers up, the
	    // following code determines if this is the case
	    int maxTabletsPerRequest = Integer.MAX_VALUE;
	    if (numThreads / binnedRanges.size() > 1) {
	      int totalNumberOfTablets = 0;
	      for (Entry<String,Map<KeyExtent,List<Range>>> entry : binnedRanges.entrySet()) {
	        totalNumberOfTablets += entry.getValue().size();
	      }
	      
	      maxTabletsPerRequest = totalNumberOfTablets / numThreads;
	      if (maxTabletsPerRequest == 0) {
	        maxTabletsPerRequest = 1;
	      }
	      
	    }
	    
	    Map<KeyExtent,List<Range>> failures = new HashMap<KeyExtent,List<Range>>();
	    
	    // randomize tabletserver order... this will help when there are multiple
	    // batch readers and writers running against accumulo
	    List<String> locations = new ArrayList<String>(binnedRanges.keySet());
	    // do not shuffle the binned ranges
	    //Collections.shuffle(locations);
	    
	    List<ScanTask> ScanTasks = new ArrayList<ScanTask>();
	    
	    for (final String tsLocation : locations) {
	      
	      final Map<KeyExtent,List<Range>> tabletsRanges = binnedRanges.get(tsLocation);
	      if (maxTabletsPerRequest == Integer.MAX_VALUE || tabletsRanges.size() == 1) {
	        ScanTask ScanTask = new ScanTask(tsLocation, tabletsRanges, failures, receiver, columns);
	        ScanTask.initialize(this, instance, credentials, tsLocation,options);
	        receiver.receive(ScanTask,null,null);
	        ScanTasks.add(ScanTask);
	      } else {
	        HashMap<KeyExtent,List<Range>> tabletSubset = new HashMap<KeyExtent,List<Range>>();
	        for (Entry<KeyExtent,List<Range>> entry : tabletsRanges.entrySet()) {
	          tabletSubset.put(entry.getKey(), entry.getValue());
	          if (tabletSubset.size() >= maxTabletsPerRequest) {
	            ScanTask ScanTask = new ScanTask(tsLocation, tabletSubset, failures, receiver, columns);
	            ScanTask.initialize(this, instance, credentials, tsLocation,options);
	            receiver.receive(ScanTask,null,null);
	            ScanTasks.add(ScanTask);
	            tabletSubset = new HashMap<KeyExtent,List<Range>>();
	          }
	        }
	        
	        if (tabletSubset.size() > 0) {
	          ScanTask ScanTask = new ScanTask(tsLocation, tabletSubset, failures, receiver, columns);
	          ScanTask.initialize(this, instance, credentials, tsLocation,options);
	          receiver.receive(ScanTask,null,null);
	          ScanTasks.add(ScanTask);
	        }
	      }
	    }
	    
	    final Semaphore semaphore = new Semaphore(ScanTasks.size());
	    semaphore.acquireUninterruptibly(ScanTasks.size());
	    
	    for (ScanTask ScanTask : ScanTasks) {
	      ScanTask.setSemaphore(semaphore, ScanTasks.size());
	      queryThreadPool.execute(new TraceRunnable(ScanTask));
	    }
	  }
	  
}