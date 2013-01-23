package com.datasalt.trident;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.collections.MapUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.splout.db.qnode.beans.QueryStatus;

/**
 * This class implements, on one hand, the real-time view of the lambda architecture and on the other hand a DRCP
 * service for querying both the real-time and batch layer and merging the results, everything with Trident.
 */
@SuppressWarnings({ "serial", "unchecked" })
public class LambdaHashTagsTopology {

	/**
	 * The purpose of this function is to 1) emit only words that are hashtags and 2) emit them without the # character.
	 * It is called Filter but it is a Function since we are also emitting new values.
	 */
	public static class HashTagFilter extends BaseFunction {

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String word = tuple.getString(0);
			if(word.startsWith("#")) {
				// only emit hashtags, and emit them without the # character
				collector.emit(new Values(word.substring(1, word.length())));
			}
		}
	}

	/**
	 * This Combiner is a sophisticated Count(). It takes a date into account for generating a Map<String, Long> as value
	 * instead of a single Long, where the String is the date. It therefore has to be able to merge two maps with
	 * potentially different values.
	 */
	public static class CountByDate implements CombinerAggregator<Map<String, Long>> {

		@Override
		public Map<String, Long> init(TridentTuple tuple) {
			Map<String, Long> map = zero();
			map.put(tuple.getString(1), 1L);
			return map;
		}

		@Override
		// map merge
		public Map<String, Long> combine(Map<String, Long> val1, Map<String, Long> val2) {
			for(Map.Entry<String, Long> entry : val2.entrySet()) {
				val2.put(entry.getKey(), MapUtils.getLong(val1, entry.getKey(), 0L) + entry.getValue());
			}
			for(Map.Entry<String, Long> entry : val1.entrySet()) {
				if(val2.containsKey(entry.getKey())) {
					continue;
				}
				val2.put(entry.getKey(), entry.getValue());
			}
			return val2;
		}

		@Override
		// when there is no value it is interesting to return an empty map
		public Map<String, Long> zero() {
			return new HashMap<String, Long>();
		}
	}

	/**
	 * This function implements the business logic of merging the real-time layer results with the batch-layer results. We
	 * do it simply by overwriting any possible value of the real-time layer with the value from the batch layer. Then, we
	 * return a unified Map<String, Long> with the count for each date, sorted by date.
	 */
	public static class LambdaMerge extends BaseFunction {

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			Map<String, Long> resultRealTime = (Map<String, Long>) tuple.get(1);
			QueryStatus resultBatch = (QueryStatus) tuple.get(2);

			TreeMap<String, Long> consolidatedResult;

			if(resultRealTime != null) {
				consolidatedResult = new TreeMap<String, Long>(resultRealTime);
			} else {
				consolidatedResult = new TreeMap<String, Long>();
			}

			if(resultBatch != null) {
				if(resultBatch.getResult() != null) {
					for(Object rowBatch : resultBatch.getResult()) {
						Map<String, Object> mapRow = (Map<String, Object>) rowBatch;
						String day = (String) mapRow.get("day");
						// we do this since Splout may return Integer or Long depending on the value
						Long count = Long.parseLong(mapRow.get("SUM(count)").toString());
						// In the real-time map we set the values from the batch view
						// Therefore if there is a value coming from batch it will override any value from real-time
						// This is the usual decision for lambda architectures.
						consolidatedResult.put(day, count);
					}
				}
			}

			collector.emit(new Values(consolidatedResult));
		}
	}

	private final static DateTimeFormatter DAY_FORMAT = DateTimeFormat.forPattern("yyyyMMdd");

	public static StormTopology buildTopology(LocalDRPC drpc) {

		TridentTopology topology = new TridentTopology();

		String now = DAY_FORMAT.print(new Date().getTime());

		// This is just a dummy cyclic spout that only emits two tweets
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("tweet", "date"), 3, 
				new Values("#california is cool", now), 
				new Values("I like #california", now)
		);
		spout.setCycle(true);

		// In this state we will save the real-time counts per date for each hashtag
		StateFactory mapState = new MemoryMapState.Factory();

		// Real-time part of the system: a Trident topology that groups by hashtag and stores per-date counts
		TridentState hashTagCounts = topology
		    .newStream("spout1", spout)
		    // note how we carry the date around
		    .each(new Fields("tweet", "date"), new Split(), new Fields("word"))
		    .each(new Fields("word", "date"), new HashTagFilter(), new Fields("hashtag"))
		    .groupBy(new Fields("hashtag"))
		    .persistentAggregate(mapState, new Fields("hashtag", "date"), new CountByDate(),
		        new Fields("datecount"));

		// Batch part of the system:
		// We instantiate a Splout connector that doesn't fail fast so we can work without the batch layer.
		// This TridentState can be used to query Splout.
		TridentState sploutState = topology.newStaticState(new SploutState.Factory(false,
		    "http://localhost:4412"));

		// DRPC service:
		// Accepts a "hashtag" argument and queries first the real-time view and then the batch-view. Finally,
		// it uses a custom Function "LambdaMerge" for merging the results and projects the results back to the user.
		topology
		    .newDRPCStream("hashtags", drpc)
		    .each(new Fields("args"), new Split(), new Fields("hashtag"))
		    .groupBy(new Fields("hashtag"))
		    .stateQuery(hashTagCounts, new Fields("hashtag"), new MapGet(), new Fields("resultrt"))
		    .stateQuery(sploutState, new Fields("hashtag", "resultrt"), new HashTagsSploutQuery(),
		        new Fields("resultbatch"))
		    .each(new Fields("hashtag", "resultrt", "resultbatch"), new LambdaMerge(), new Fields("result"))
		    // Project allows us to keep only the interesting results
		    .project(new Fields("result"));

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);

		// This topology can only be run as local because it is a toy example
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hashtagCounter", conf, buildTopology(drpc));
		// Query 100 times for hashtag "california" for illustrating the effect of the lambda architecture
		for(int i = 0; i < 100; i++) {
			System.out.println("Result for hashtag 'california' -> " + drpc.execute("hashtags", "california"));
			Thread.sleep(1000);
		}
	}
}
