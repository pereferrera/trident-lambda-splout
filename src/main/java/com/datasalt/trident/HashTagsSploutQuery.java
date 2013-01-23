package com.datasalt.trident;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * Encapsulates the business logic of querying daily hashtag counts to Splout SQL (http://sploutsql.com)
 * using the Infochimp's dataset.   
 */
@SuppressWarnings("serial")
public class HashTagsSploutQuery extends BaseQueryFunction<SploutState, Object> {

	final static String TABLESPACE = "hashtags";

	@Override
	public List<Object> batchRetrieve(SploutState state, List<TridentTuple> args) {
		List<String> sqls = new ArrayList<String>();
		List<String> partitionKeys = new ArrayList<String>();

		// fill the data
		for(TridentTuple arg: args) {
			String hashTag = arg.getString(0);
			sqls.add("SELECT SUM(count), substr(date, 0, 9) as day FROM hashtags WHERE hashtag = '" + hashTag + "' GROUP BY day;");
			partitionKeys.add(hashTag);
		}

		return state.querySplout(TABLESPACE, sqls, partitionKeys);
	}

	@Override
	public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
		collector.emit(new Values(result));
	}
}
