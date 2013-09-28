package org.apache.accumulo.core.client.impl;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestScanner {

	@Test
	public void testScan() throws AccumuloException, AccumuloSecurityException,
			TableNotFoundException, TableExistsException {

		MockInstance instance = new MockInstance("mawk");
		
		Connector connector = instance
				.getConnector("root", "".getBytes());

		if (!connector.tableOperations().exists("test"))
			connector.tableOperations().create("test");

		BatchWriter writer = connector.createBatchWriter("test",
				128L * 1024L * 1024L, 10000L, 11);

		byte[] emptyBytes = {};

		for (int i = 0; i < 100; i++) {
			Mutation m = new Mutation("" + i);

			m.put(new Text("cf"), new Text("cq"), new Value(emptyBytes));
			writer.addMutation(m);
		}

		writer.close();

		ByteBuffer buff = ByteBuffer.wrap("secret".getBytes());

		MemoryStore memStore = new MemoryStore();

		ItelliScanner scanner = new ItelliScanner(instance, new AuthInfo(
				"root", buff, instance.getInstanceID()), "test", connector
				.securityOperations().getUserAuthorizations("root"), 5,
				ResultReceiver.class, memStore, memStore);

		scanner.setRanges(Collections.singleton(new Range()));

		Iterator<Entry<Key, Value>> iter = scanner.iterator();

		while (iter.hasNext()) {
			Entry<Key, Value> kv = iter.next();
			new Long(kv.getKey().getRow().toString());
		}
	}

}
