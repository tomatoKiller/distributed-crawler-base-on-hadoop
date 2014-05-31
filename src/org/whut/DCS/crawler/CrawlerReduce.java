package org.whut.DCS.crawler;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.whut.DCS.utils.DCSUtils;

public class CrawlerReduce extends MapReduceBase implements Reducer<Text, Text, Text,MapWritable>{

	private static class CrawlerThread implements Runnable{

		private Crawler crawler=null;
		private OutputCollector<Text, MapWritable> collector=null;
		public CrawlerThread(Crawler crawler,OutputCollector<Text, MapWritable> collector) {
			this.crawler = crawler;
			this.collector=collector;
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			boolean isSuccessful=crawler.start();
			if(isSuccessful)
				synchronized (collector) {
					try {
						collector.collect(crawler.getEntry(),crawler.getResult());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
		}
	}
	
	private ExecutorService pool=null;
	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		pool=Executors.newFixedThreadPool(arg0.getInt("reduce.concurrency.value",DCSUtils.REDUCE_CONCURRENCY_VALUE));
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		pool.shutdown();
		
		while(!pool.isTerminated())
			try {
				TimeUnit.SECONDS.sleep(5);
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			}		
	}

	@Override
	public void reduce(Text arg0, Iterator<Text> arg1,OutputCollector<Text, MapWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
	
		Text entry=arg0;
		Text documentURL=arg1.next();
		pool.execute(new CrawlerThread(new Crawler(documentURL, entry),arg2));
	}
}
