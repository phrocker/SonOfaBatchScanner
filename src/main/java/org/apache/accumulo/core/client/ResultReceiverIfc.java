package org.apache.accumulo.core.client;

import org.apache.accumulo.core.client.impl.ScanTask;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public interface ResultReceiverIfc {
    void receive(ScanTask task,Key key, Value value);
  }