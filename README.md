trident-lambda-splout
=====================

A toy example of a ["Lambda architecture"](http://www.dzone.com/links/r/big_data_lambda_architecture.html) using Storm's [Trident](https://github.com/nathanmarz/storm/wiki/Trident-tutorial) 
as real-time layer and [Splout SQL](http://sploutsql.com) as batch layer.

The problem
===========

We want to implement counting the number of appearances of hashtags in tweets, grouped by date, and serve the data as a remote service,
 for example to be able to populate timelines in a website / mobile app (e.g. give me the evolution of mentions for hashtag "california" for
 the past 10 days).

The requirements for the solution are:
- It must scale (we want to process billions of tweets. Think as if we had access to the Firehouse!).
- It must be able to serve low-latency requests to potentially a lot of concurrent users asking for timelines.

Using Hadoop to store the tweets and a simple Hive query for grouping by hashtag and date seems good enough for calculating the counts.
However, we also want to add real-time to the system: we want to have the actual number of appearances for hashtags updated 
for today in seconds time. And we need to put the Hadoop counts in some really fast datastore for being able to query them. 

The solution
============

The solution proposed is to use a "lambda architecture" and implement a real-time layer using [Trident](https://github.com/nathanmarz/storm/wiki/Trident-tutorial), which is an API on top of Storm that eases
building real-time topologies and saving persistent state derived from them. 

For serving the batch layer we will use [Splout SQL](http://sploutsql.com) which is a high-performant SQL read-only data store that can pull and serve datasets
 from Hadoop very efficiently. Splout is fast like [ElephantDB](https://github.com/nathanmarz/elephantdb) but it also allows us to execute SQL queries. Using SQL for serving the batch
 layer is convenient as we might want to break-down the counts by hour, day, week, or any arbitrary date period.  
 
We will also use [Trident](https://github.com/nathanmarz/storm/wiki/Trident-tutorial) to implement the remote service using its DRPC capabilities. [Trident](https://github.com/nathanmarz/storm/wiki/Trident-tutorial) iself will query both the batch layer and the
real-time layer and merge the results.

How to try it
=============

1) Hadoop

Hadoop is a key component of this "lambda architecture" example so you must have it installed and its services
must be running. $HADOOP_HOME needs to be defined for Splout SQL.

2) Batch layer

For trying out this example you must first download Splout SQL an execute a one-node cluster locally.
You can download a distribution from Maven Central: http://search.maven.org/#browse%7C-1223220252
After uncompressing it:

	bin/splout-service.sh qnode start
	bin/splout-service.sh dnode start

Should bring you a one-node local cluster up at: http://localhost:4412

When that is finished you can load a toy dataset of hourly hashtag counts. 
You can use the data in this "trident-lambda-splout" repo under "sample-hashtags" folder.

Let's call $TRIDENT_LAMBDA_SPLOUT_HOME the path where you have cloned this repo, then, 
from $SPLOUT_HOME (the path where you uncompressed Splout) you just execute:

	hadoop fs -put $TRIDENT_LAMBDA_SPLOUT_HOME/sample-hashtags sample-hashtags
	hadoop jar splout-hadoop-*-hadoop.jar simple-generate -i sample-hashtags -o out-hashtags -pby hashtag -p 2 -s "label:string,date:string,count:int,hashtag:string" -t hashtags -tb hashtags
	hadoop jar splout-hadoop-*-hadoop.jar deploy -q http://localhost:4412 -root out-hashtags -ts hashtags
	
After these three statements you will have the data indexed, partitioned and loaded into [Splout SQL](http://sploutsql.com). 
You can check it by looking for Tablespace "hashtags" at the management webapp: http://localhost:4412

2) Real-time layer

Execute the class in this repo called "LambdaHashTagsTopology" from your favorite IDE. This class will:
- Start a dummy cyclic input Spout that emits always the same two tweets.   
- Start a Trident topology that counts hashtags by date in real-time.
- Start a DRPC server that accepts a hashtag as argument and queries both Splout (batch-layer) and Trident (real-time layer) and merges the
results.

If you want to execute it from command line you can use maven as follows:

	mvn clean install
	mvn dependency:copy-dependencies
	mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp target/classes:target/dependency/* com.datasalt.trident.LambdaHashTagsTopology"

You should see something like this:

	...
	Result for hashtag 'california' -> [[{"20091022":115,"20091023":115,"20091024":158,"20091025":19}]]
	Result for hashtag 'california' -> [[{"20091022":115,"20091023":115,"20091024":158,"20091025":19,"20130123":76}]]
	Result for hashtag 'california' -> [[{"20091022":115,"20091023":115,"20091024":158,"20091025":19,"20130123":136}]]
	Result for hashtag 'california' -> [[{"20091022":115,"20091023":115,"20091024":158,"20091025":19,"20130123":192}]]
	Result for hashtag 'california' -> [[{"20091022":115,"20091023":115,"20091024":158,"20091025":19,"20130123":232}]]
	Result for hashtag 'california' -> [[{"20091022":115,"20091023":115,"20091024":158,"20091025":19,"20130123":286}]]
	...
	
The first four dates come from the batch layer Splout SQL whereas the last date (whose count is being incremented in real-time) comes from the real-time layer.
The merging has been done with Trident at the DRPC service.

Conclusions
===========

We have shown a simple toy example of a "lambda architecture" that provides timelines for mentions of hashtags in tweets.
If you see the code, you will notice it is actually quite easy and straight-forward to implement the real-time part of this system
using [Trident](https://github.com/nathanmarz/storm/wiki/Trident-tutorial), mainly because of its high-level constructs (a-la-Cascading, each(), groupBy(), etc) and its wrappers around memory
state. There is quite an interesting amount of information on how to handle state properly with Trident here: https://github.com/nathanmarz/storm/wiki/Trident-state .
 
We have also seen [Splout SQL](http://sploutsql.com), a database that integrates tightly with Hadoop and provides real low-latency lookups over its data,
being the perfect solution for serving a batch layer in a highly concurrent website or mobile application.

For this example to be complete we have to clarify some things:

- We didn't implement actually crawling the Tweets, parsing them and feeding them into Storm. 
You would usually do that through a messaging / queue system such as Kestrel (see https://github.com/nathanmarz/storm-kestrel). 
Creating a scalable fetcher for Twitter that also outputs the Tweets to Hadoop's HDFS is too complex and out of scope for this example.

- We used a dataset of hourly counts that was already calculated by Hadoop but we didn't show how to do that. 
This part is quite straight-foward and you will find plenty of examples in the web on how to perform simple aggregation tasks 
using Hadoop, Pig, Hive, Cascading or even a lower-level API such as [Pangool](http://pangool.net/).

- We didn't talk about a "master coordinator" which is a quite important part of the architecture. This coordinator would be in charge
of triggering the Hadoop aggregation task, and triggering the Splout generation and deploy tasks of [Splout SQL](http://sploutsql.com) we saw above.
One important thing to keep in mind is that batch always overrides real-time in this example so the coordinator and the fetcher must
make sure that Hadoop only processes complete-hour data. Uncomplete hours should only be handled by the real-time layer.

- The real-time layer should expire old data from time to time in order to be efficient (e.g. keep only a rolling view of one week).
Keeping a rolling one-week view would mean that the batch layer could potentially be executed only once per week. 

For another toy example on Storm (even though it is a bit old: http://www.datasalt.com/2012/01/real-time-feed-processing-with-storm/)