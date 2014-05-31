package org.whut.DCS.wikipediarecommend;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.whut.DCS.utils.TextArrayWritable;

public class UserVectorToCooccurenceMapper extends MapReduceBase implements Mapper<Text, TextArrayWritable,Text,Text>{
	
	@Override
	public void map(Text arg0, TextArrayWritable arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		Writable[] entries=arg1.get();
		for(int i=0;i<entries.length;i++)
			for(int j=0;j<entries.length;j++)
				arg2.collect((Text)entries[i],(Text)entries[j]);
			
	}
}
