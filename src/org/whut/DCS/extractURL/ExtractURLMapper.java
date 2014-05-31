package org.whut.DCS.extractURL;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.whut.DCS.utils.DCSUtils;

public class ExtractURLMapper extends MapReduceBase implements Mapper<Text, MapWritable, Text, Text>{

	private static enum URLType{
		DUPLICATEURL,VALIDURL;
	}
	BloomFilter bloomFilter=new BloomFilter();
	@Override
	public void map(Text arg0, MapWritable arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		
		for(Entry<Writable, Writable> kv:arg1.entrySet()){
			Text URL=(Text) kv.getKey();
			Text entry=(Text) kv.getValue();
			boolean isExist=bloomFilter.membershipTest(new Key(URL.toString().getBytes()));
			if(!isExist){
				arg3.incrCounter(URLType.VALIDURL,1);
				arg2.collect(entry,URL);
			}else
				arg3.incrCounter(URLType.DUPLICATEURL,1);
		}
	}

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		try {			
			FileSystem hdfs=FileSystem.get(job);
			Path bloomFilterPath=new Path(job.get("bloom.filter.file.name", DCSUtils.BLOOM_FILTER_FILE_NAME));
			FSDataInputStream in=hdfs.open(bloomFilterPath);
			bloomFilter.readFields(in);
			in.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
