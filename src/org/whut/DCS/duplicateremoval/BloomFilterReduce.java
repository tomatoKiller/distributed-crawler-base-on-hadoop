package org.whut.DCS.duplicateremoval;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.hash.Hash;
import org.whut.DCS.utils.DCSUtils;

public class BloomFilterReduce extends MapReduceBase implements Reducer<IntWritable, BloomFilter, NullWritable, NullWritable>{

	private String bloomFilterFile=null;
	private BloomFilter bloomFilter=null;
	private JobConf job=null;
	
	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		int bloomFilterVectorSize=job.getInt("bloom.filter.vector.size",DCSUtils.BLOOM_FILTER_VECTOR_SIZE);
		int bloomFilterHashFuncNum=job.getInt("bloom.filter.func.num", DCSUtils.BLOOM_FILTER_FUNC_NUM);
		
		bloomFilter=new BloomFilter(bloomFilterVectorSize, bloomFilterHashFuncNum, Hash.MURMUR_HASH);
		bloomFilterFile=job.get("bloom_filter.file.name",DCSUtils.BLOOM_FILTER_FILE_NAME);
		this.job=job;
	}
	@Override
	public void reduce(IntWritable arg0, Iterator<BloomFilter> arg1,
			OutputCollector<NullWritable, NullWritable> arg2, Reporter arg3)
			throws IOException {
		
		// TODO Auto-generated method stub
		while(arg1.hasNext())
			bloomFilter.or(arg1.next());
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		Path file=new Path(bloomFilterFile);
		FileSystem hdfs=FileSystem.get(job);
		
		if(hdfs.exists(file)){
			FSDataInputStream in=hdfs.open(file);
			BloomFilter oldFilter=new BloomFilter();
			oldFilter.readFields(in);
			in.close();
			bloomFilter.or(oldFilter);
			hdfs.delete(file, true);
		}
		FSDataOutputStream out=hdfs.create(file);
		bloomFilter.write(out);
		out.close();
	}
}
