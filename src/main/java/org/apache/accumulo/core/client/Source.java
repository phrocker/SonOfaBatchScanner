package org.apache.accumulo.core.client;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Source for the intermittent data store
 * @author marc
 *
 */
public interface Source {

	Iterator<Entry<Key,Value>> iterator();

}
