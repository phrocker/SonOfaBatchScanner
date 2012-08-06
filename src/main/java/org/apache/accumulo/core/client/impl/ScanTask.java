package org.apache.accumulo.core.client.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ItelliProcessor;
import org.apache.accumulo.core.client.ResultReceiverIfc;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.InitialMultiScan;
import org.apache.accumulo.core.data.thrift.MultiScanResult;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TKeyValue;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ScanTask implements Runnable, Comparable<ScanTask> {

	protected Instance instance = null;
	protected AuthInfo credentials = null;
	protected String table = null;
	protected Authorizations authorizations = Constants.NO_AUTHS;

	protected AtomicBoolean isRunning;

	protected AtomicBoolean isFinished;

	protected String tsLocation = null;
	protected Map<KeyExtent, List<Range>> tabletsRanges = null;
	protected ResultReceiverIfc receiver = null;
	protected Semaphore semaphore = null;
	protected Map<KeyExtent, List<Range>> failures = null;
	protected List<Column> columns = null;
	protected int semaphoreSize = 0;
	private ScannerOptions options = null;

	protected long id;

	protected ItelliProcessor failureProcessor;
	private volatile Throwable fatalException = null;

	protected static final Logger log = Logger.getLogger(ScanTask.class);

	public ScanTask(String tsLocation,
			Map<KeyExtent, List<Range>> tabletsRanges,
			Map<KeyExtent, List<Range>> failures, ResultReceiverIfc receiver,
			List<Column> columns) {
		
		isRunning = new AtomicBoolean(false);
		isFinished = new AtomicBoolean(false);
		isRunning.set(true);
		this.tsLocation = tsLocation;
		this.tabletsRanges = tabletsRanges;
		this.receiver = receiver;
		this.columns = columns;
		this.failures = failures;
	}

	public void initialize(final ItelliProcessor failureProcessor,
			final Instance instance, final AuthInfo credentials,
			final String table,ScannerOptions options) {
		this.failureProcessor = failureProcessor;
		this.instance = instance;
		this.credentials = credentials;
		this.options = options;
		this.table = table;
	}

	void setSemaphore(Semaphore semaphore, int semaphoreSize) {
		this.semaphore = semaphore;
		this.semaphoreSize = semaphoreSize;
	}

	public void run() {

		isRunning.set(true);
		isFinished.set(false);
		id = Thread.currentThread().getId();

		String threadName = Thread.currentThread().getName();

		Thread.currentThread().setName(
				threadName + " looking up " + tabletsRanges.size()
						+ " ranges at " + tsLocation);
		Map<KeyExtent, List<Range>> unscanned = new HashMap<KeyExtent, List<Range>>();
		Map<KeyExtent, List<Range>> tsFailures = new HashMap<KeyExtent, List<Range>>();
		try {
			doLookup(this, tsLocation, tabletsRanges, tsFailures, unscanned,
					receiver, columns, credentials, options, authorizations,
					instance.getConfiguration());
			if (tsFailures.size() > 0) {
				TabletLocator tabletLocator = TabletLocator.getInstance(
						instance, credentials, new Text(table));
				tabletLocator.invalidateCache(tsFailures.keySet());
				synchronized (failures) {
					failures.putAll(tsFailures);
				}
			}

		} catch (IOException e) {
			synchronized (failures) {
				failures.putAll(tsFailures);
				failures.putAll(unscanned);
			}

			TabletLocator.getInstance(instance, credentials, new Text(table))
					.invalidateCache(tsLocation);
			log.debug(e.getMessage(), e);
		} catch (AccumuloSecurityException e) {
			log.debug(e.getMessage(), e);

			Tables.clearCache(instance);
			if (!Tables.exists(instance, table))
				fatalException = new TableDeletedException(table);
			else
				fatalException = e;
		} catch (Throwable t) {

			log.debug(t.getMessage(), t);

			fatalException = t;
		} finally {
			semaphore.release();
			Thread.currentThread().setName(threadName);
			if (semaphore.tryAcquire(semaphoreSize)) {
				// finished processing all queries
				if (fatalException == null && failures.size() > 0) {
					// there were some failures
					try {
						failureProcessor.processFailures(failures, receiver,
								columns);
					} catch (TableNotFoundException e) {
						log.debug(e.getMessage(), e);
						fatalException = e;
					} catch (AccumuloException e) {
						log.debug(e.getMessage(), e);
						fatalException = e;
					} catch (AccumuloSecurityException e) {
						log.debug(e.getMessage(), e);
						fatalException = e;
					} catch (Throwable t) {
						log.debug(t.getMessage(), t);
						fatalException = t;
					}

					if (fatalException != null) {

						// we are finished with this batch query
						receiver.receive(this, null, null);
						// if (!resultsQueue.offer(new KeyValue(null, null))) {
						// log.debug("Could not add to result queue after seeing fatalException in processFailures",
						// fatalException);
					}
				}
			} else {
				// we are finished with this batch query
				if (fatalException != null) {
					// if (!resultsQueue.offer(new KeyValue(null, null))) {
					// log.debug("Could not add to result queue after seeing fatalException",
					// fatalException);
					// }
				} else {
					receiver.receive(this, null, null);
					// resultsQueue.put(new KeyValue(null, null));
				}
			}
		}
		isRunning.set(false);
		isFinished.set(true);
	}

	static void trackScanning(Map<KeyExtent, List<Range>> failures,
			Map<KeyExtent, List<Range>> unscanned, MultiScanResult scanResult) {

		// translate returned failures, remove them from unscanned, and add them
		// to failures
		Map<KeyExtent, List<Range>> retFailures = Translator.translate(
				scanResult.failures, Translator.TKET,
				new Translator.ListTranslator<TRange, Range>(Translator.TRT));
		unscanned.keySet().removeAll(retFailures.keySet());
		failures.putAll(retFailures);

		// translate full scans and remove them from unscanned
		HashSet<KeyExtent> fullScans = new HashSet<KeyExtent>(
				Translator.translate(scanResult.fullScans, Translator.TKET));
		unscanned.keySet().removeAll(fullScans);

		// remove partial scan from unscanned
		if (scanResult.partScan != null) {
			KeyExtent ke = new KeyExtent(scanResult.partScan);
			Key nextKey = new Key(scanResult.partNextKey);

			ListIterator<Range> iterator = unscanned.get(ke).listIterator();
			while (iterator.hasNext()) {
				Range range = iterator.next();

				if (range.afterEndKey(nextKey)
						|| (nextKey.equals(range.getEndKey()) && scanResult.partNextKeyInclusive != range
								.isEndKeyInclusive())) {
					iterator.remove();
				} else if (range.contains(nextKey)) {
					iterator.remove();
					Range partRange = new Range(nextKey,
							scanResult.partNextKeyInclusive, range.getEndKey(),
							range.isEndKeyInclusive());
					iterator.add(partRange);
				}
			}
		}
	}

	static int sumSizes(Collection<List<Range>> values) {
		int sum = 0;

		for (List<Range> list : values) {
			sum += list.size();
		}

		return sum;
	}

	static void doLookup(ScanTask scanTask, String server,
			Map<KeyExtent, List<Range>> requested,
			Map<KeyExtent, List<Range>> failures,
			Map<KeyExtent, List<Range>> unscanned, ResultReceiverIfc receiver,
			List<Column> columns, AuthInfo credentials, ScannerOptions options,
			Authorizations authorizations, AccumuloConfiguration conf)
			throws IOException, AccumuloSecurityException,
			AccumuloServerException {

		if (requested.size() == 0) {
			return;
		}

		
		// copy requested to unscanned map. we will remove ranges as they are
		// scanned in trackScanning()
		for (Entry<KeyExtent, List<Range>> entry : requested.entrySet()) {
			ArrayList<Range> ranges = new ArrayList<Range>();
			for (Range range : entry.getValue()) {
				ranges.add(new Range(range));
			}
			unscanned.put(new KeyExtent(entry.getKey()), ranges);
		}

		TTransport transport = null;
		try {
			TabletClientService.Iface client = ThriftUtil.getTServerClient(
					server, conf);
			try {
				
				OpTimer opTimer = new OpTimer(log, Level.TRACE)
						.start("Starting multi scan, tserver=" + server
								+ "  #tablets=" + requested.size()
								+ "  #ranges=" + sumSizes(requested.values())
								+ " ssil=" + options.serverSideIteratorList
								+ " ssio=" + options.serverSideIteratorOptions);
				
				TabletType ttype = TabletType.type(requested.keySet());
				boolean waitForWrites = !ThriftScanner.serversWaitedForWrites
						.get(ttype).contains(server);

				Map<TKeyExtent, List<TRange>> thriftTabletRanges = Translator
						.translate(requested, Translator.KET,
								new Translator.ListTranslator<Range, TRange>(
										Translator.RT));
				InitialMultiScan imsr = client.startMultiScan(null,
						credentials, thriftTabletRanges, Translator.translate(
								columns, Translator.CT),
						options.serverSideIteratorList,
						options.serverSideIteratorOptions, ByteBufferUtil
								.toByteBuffers(authorizations
										.getAuthorizations()), waitForWrites);
				if (waitForWrites)
					ThriftScanner.serversWaitedForWrites.get(ttype).add(server);

				MultiScanResult scanResult = imsr.result;

				opTimer.stop("Got 1st multi scan results, #results="
						+ scanResult.results.size()
						+ (scanResult.more ? "  scanID=" + imsr.scanID : "")
						+ " in %DURATION%");
				
				
				
				for (TKeyValue kv : scanResult.results) {
					
					receiver.receive(scanTask, new Key(kv.key), new Value(
							kv.value));
				}
				trackScanning(failures, unscanned, scanResult);

				while (scanResult.more) {

					opTimer.start("Continuing multi scan, scanid="
							+ imsr.scanID);
					scanResult = client.continueMultiScan(null, imsr.scanID);
					opTimer.stop("Got more multi scan results, #results="
							+ scanResult.results.size()
							+ (scanResult.more ? "  scanID=" + imsr.scanID : "")
							+ " in %DURATION%");
					for (TKeyValue kv : scanResult.results) {
						
						receiver.receive(scanTask, new Key(kv.key), new Value(
								kv.value));
					}
					trackScanning(failures, unscanned, scanResult);
				}

				client.closeMultiScan(null, imsr.scanID);

			} finally {
				ThriftUtil.returnClient(client);
				
				
			}
		} catch (TTransportException e) {
			log.debug("Server : " + server + " msg : " + e.getMessage());
			throw new IOException(e);
		} catch (ThriftSecurityException e) {
			log.debug("Server : " + server + " msg : " + e.getMessage(), e);
			throw new AccumuloSecurityException(e.user, e.code, e);
		} catch (TApplicationException e) {
			log.debug("Server : " + server + " msg : " + e.getMessage(), e);
			throw new AccumuloServerException(server, e);
		} catch (TException e) {
			log.debug("Server : " + server + " msg : " + e.getMessage(), e);
			throw new IOException(e);
		} catch (NoSuchScanIDException e) {
			log.debug("Server : " + server + " msg : " + e.getMessage(), e);
			throw new IOException(e);
		} finally {
			ThriftTransportPool.getInstance().returnTransport(transport);
		}
	}

	/**
	 * 
	 * @return
	 */
	public long getId() {
		return id;
	}

	public boolean isRunning() {
		return isRunning.get();
	}

	public boolean isFinished()
	{
		return isFinished.get();
	}

	public int compareTo(ScanTask arg0) {
		if (tabletsRanges.isEmpty())
		{
			return -1;
		}
		
		KeyExtent ex1 = tabletsRanges.entrySet().iterator().next().getKey();
		KeyExtent ex2 = arg0.tabletsRanges.entrySet().iterator().next().getKey();
		return ex1.compareTo(ex2);
	}
	
}

