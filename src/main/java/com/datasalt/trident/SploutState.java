package com.datasalt.trident;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.trident.state.ReadOnlyState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.splout.db.common.SploutClient;

/**
 * A generic Trident-state with its factory for being able to query a Splout SQL backend (http://sploutsql.com)
 */
public class SploutState extends ReadOnlyState {

	// The Splout Java client
	private SploutClient sploutClient;
	// Whether we should throw a RuntimeException when failing a query or not (e.g. Splout SQL is down).
	// Because QueryFunction doesn't allow us to propagate explicit Exceptions upstream, we have to decide:
	// either we set "null" on a failure in the result or we throw a non-catched Exception upstream.
	private boolean failFast;

	@SuppressWarnings("serial")
	public static class Factory implements StateFactory {

		String[] qNodes;
		boolean failFast;

		public Factory(boolean failFast, String... qNodes) {
			this.qNodes = qNodes;
		}

		@SuppressWarnings("rawtypes")
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			return new SploutState(failFast, qNodes);
    }
	}

	public SploutState(boolean failFast, String... qNodes) {
		this.sploutClient = new SploutClient(qNodes);
		this.failFast = failFast;
	}

	/**
	 * Given a SQL query and a partition key, query Splout. We take advantage of Trident's batch and perform several
	 * queries in a row. This could actually be a lot more efficient if we used a thread-pool here since Splout can handle
	 * parallel queries quite well, but I kept it simple just for the sake of this example.
	 */
	public List<Object> querySplout(String tablespace, List<String> sql, List<String> keys) {
		List<Object> result = new ArrayList<Object>();
		for(int i = 0; i < sql.size(); i++) {
			String partitionKey = keys.get(i);
			String sqlQuery = sql.get(i);
			try {
				result.add(sploutClient.query(tablespace, partitionKey, sqlQuery, null));
			} catch(IOException e) {
				if(!failFast) {
					result.add(null);
				} else {
					throw new RuntimeException(e);
				}
			}
		}
		return result;
	}
}
