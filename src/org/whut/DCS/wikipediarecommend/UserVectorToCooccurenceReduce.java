package org.whut.DCS.wikipediarecommend;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UserVectorToCooccurenceReduce extends MapReduceBase implements Reducer<Text, Text, Text,MapWritable>{

	@Override
	public void reduce(Text arg0, Iterator<Text> arg1,
			OutputCollector<Text, MapWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		MapWritable map=new MapWritable();
		while(arg1.hasNext()){
			Text arg=arg1.next();
			if(!map.containsKey(arg))
				map.put(arg, new IntWritable(1));
			else{
				IntWritable value=(IntWritable) map.get(arg);
				map.put(arg, new IntWritable(value.get()+1));
			}			
		}
		arg2.collect(arg0, map);
	}	
}
