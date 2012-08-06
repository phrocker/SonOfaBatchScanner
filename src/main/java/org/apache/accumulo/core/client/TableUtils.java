package org.apache.accumulo.core.client;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.master.state.tables.TableState;

public class TableUtils {

	public static String getTableId(final Instance instance, final String tableName) throws TableNotFoundException {
	    String tableId = Tables.getTableId(instance, tableName);
	    if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
	      throw new TableOfflineException(instance, tableId);
	    return tableId;
	  }

}
