package org.whut.DCS.duplicateremoval;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.whut.DCS.utils.DCSUtils;


public class BloomFilterMapper extends MapReduceBase implements Mapper<Text, Text, IntWritable, BloomFilter>{

	private BloomFilter bloomFilter;
	private OutputCollector<IntWritable,BloomFilter> collector=null;
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		super.close();
		collector.collect(new IntWritable(1),bloomFilter);
	}

	
	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		int bloomFilterVectorSize=job.getInt("bloom.filter.vector.size",DCSUtils.BLOOM_FILTER_VECTOR_SIZE);
		int bloomFilterHashFuncNum=job.getInt("bloom.filter.func.num", DCSUtils.BLOOM_FILTER_FUNC_NUM);
		bloomFilter=new BloomFilter(bloomFilterVectorSize, bloomFilterHashFuncNum, Hash.MURMUR_HASH);
	}

	@Override
	public void map(Text arg0, Text arg1,
			OutputCollector<IntWritable, BloomFilter> arg2, Reporter arg3)
			throws IOException {
		if(collector==null)
			collector=arg2;
		bloomFilter.add(new Key(arg1.toString().getBytes()));
		
		// TODO Auto-generated method stub
		
	}

}
