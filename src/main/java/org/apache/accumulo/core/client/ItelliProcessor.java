package org.apache.accumulo.core.client;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;

public interface ItelliProcessor {

	public void processFailures(Map<KeyExtent,List<Range>> failures, ResultReceiverIfc receiver, List<Column> columns) throws AccumuloException,
    AccumuloSecurityException, TableNotFoundException;

}
