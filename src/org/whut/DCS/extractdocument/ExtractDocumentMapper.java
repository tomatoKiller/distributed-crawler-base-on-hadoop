package org.whut.DCS.extractdocument;

import java.io.IOException;
import java.util.Collection;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.whut.DCS.utils.TextArrayWritable;


public class ExtractDocumentMapper extends MapReduceBase implements Mapper<Text, MapWritable,Text,TextArrayWritable>{

		
	private static enum DocumentType{
		DOCUMENTNUM,LINKNUM;
	}
	@Override
	public void map(Text arg0, MapWritable arg1,
			OutputCollector<Text, TextArrayWritable> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		arg3.incrCounter(DocumentType.DOCUMENTNUM,1);
		Collection<Writable> values=arg1.values();
		arg3.incrCounter(DocumentType.LINKNUM, values.size());
		TextArrayWritable array=new TextArrayWritable();
		array.set(values.toArray(new Writable[1]));
		arg2.collect(arg0, array);		
	}
	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		super.configure(job);		
	}
}
