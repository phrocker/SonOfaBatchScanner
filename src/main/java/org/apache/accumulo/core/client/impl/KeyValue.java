package org.apache.accumulo.core.client.impl;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class KeyValue implements Entry<Key,Value> {
    
	  protected Key key;
    protected Value value;
    
    KeyValue(Key key, Value value) {
      this.key = key;
      this.value = value;
    }
    
    public Key getKey() {
      return key;
    }
    
    public Value getValue() {
      return value;
    }
    
    public Value setValue(Value value) {
      throw new UnsupportedOperationException();
    }
    
  }