package org.apache.accumulo.core.client.impl;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.accumulo.core.client.Sink;
import org.apache.accumulo.core.client.Source;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class MemoryStore implements Source, Sink, Iterator<Entry<Key,Value>> {

	protected SortedMap<ScanTask,Set<Entry<Key,Value>>> resultMap;
	
	protected BlockingQueue<Entry<Key,Value>> queue;
	
	public Iterator<Entry<Key, Value>> iterator() {
		return this;
	}
	
	public MemoryStore()
	{
		queue = new LinkedBlockingDeque<Map.Entry<Key,Value>>(1000);
		resultMap = new TreeMap<ScanTask, Set<Entry<Key,Value>>>();
	}
	
	

	

	public void put(ScanTask task, Key key, Value value) {
		
		Set<Entry<Key,Value>> coll = resultMap.get(task);
		if (coll == null)
		{
			coll = new LinkedHashSet<Entry<Key,Value>>();
			resultMap.put(task,coll);
		}
		
		
		if (key != null)
			coll.add(new AbstractMap.SimpleEntry<Key,Value>(key,value));
		
		
	}

	public boolean hasNext() {
		
		boolean hasNext = false;
		synchronized(resultMap)
		{
			for(ScanTask task : resultMap.keySet())
			{
				while(task.isRunning())
				{
					// wait until it finishes
				}
					
				
				if (task.isFinished())
				{
					Set<Entry<Key,Value>> coll = resultMap.get(task);

					if (coll.size() > 0)
					{
						queue.addAll(coll);
						coll.clear();
						resultMap.put(task,coll);
						hasNext = true;
						break;
					}
					else
					{
						
						// continue to the next result set. nothing from here
						
						hasNext = (queue.size() > 0);
					}
				}
				else 
				{

					hasNext = false;
				}
			}
		}
		return hasNext;
	}

	public Entry<Key, Value> next() {
		return queue.remove();
	}

	public void remove() {
		// do nothing
	}

	public void register(ScanTask task) {
		put(task,null,null);
		
	}

}
