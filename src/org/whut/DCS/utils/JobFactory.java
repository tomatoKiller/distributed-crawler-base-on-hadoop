package org.whut.DCS.utils;

import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.whut.DCS.crawler.CrawlerReduce;
import org.whut.DCS.duplicateremoval.BloomFilterMapper;
import org.whut.DCS.duplicateremoval.BloomFilterReduce;
import org.whut.DCS.extractURL.ExtractURLMapper;
import org.whut.DCS.extractdocument.ExtractDocumentMapper;
import org.whut.DCS.wikipediarecommend.UserVectorToCooccurenceMapper;
import org.whut.DCS.wikipediarecommend.UserVectorToCooccurenceReduce;

public class JobFactory {

	public static class FileFilter implements PathFilter{

		@Override
		public boolean accept(Path arg0) {
			// TODO Auto-generated method stub
			return !arg0.getName().equals("_SUCCESS")&&!arg0.getName().equals("_logs")&&!arg0.getName().equals("_temporary");
		}		
	}
	
	public static enum JobType{
		CRAWLER_JOB,UPDATEBLOOMFILTER_JOB,EXTRACTURL_JOB,EXTRACTDOCUMENT_JOB,COUNT_COOCURRENCE_JOB;
	}
	
	private JobFactory(){
		
	}
	public static Job createJob(Path in,Path out,Configuration conf,JobType type) throws IOException, URISyntaxException{
		
		JobConf job=new JobConf(conf);
		job.setJarByClass(JobFactory.class);
		
		FileInputFormat.addInputPath(job, in);
		FileInputFormat.setInputPathFilter(job, FileFilter.class);
		FileOutputFormat.setOutputPath(job, out);
		
		String crawlerCurrentDeep=String.valueOf(conf.getInt("crawler.current.deep",-1));
		if(type==JobType.CRAWLER_JOB){
			job.setJobName("crawler_stage_"+crawlerCurrentDeep);
			job.setMapperClass(IdentityMapper.class);
			job.setReducerClass(CrawlerReduce.class);
			job.setNumReduceTasks(2);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(MapWritable.class);
			
			job.setInputFormat(SequenceFileInputFormat.class);
			job.setOutputFormat(SequenceFileOutputFormat.class);
			
			return new Job(job);	
		}
		
		if(type==JobType.UPDATEBLOOMFILTER_JOB){
			job.setJobName("update_bloom_filter_stage_"+crawlerCurrentDeep);
			job.setMapperClass(BloomFilterMapper.class);
			job.setReducerClass(BloomFilterReduce.class);
			job.setNumReduceTasks(1);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(BloomFilter.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setInputFormat(SequenceFileInputFormat.class);
			job.setOutputFormat(NullOutputFormat.class);
			
			return new Job(job);
		}
		
		if(type==JobType.EXTRACTDOCUMENT_JOB){
			job.setJobName("extract_document_stage_"+crawlerCurrentDeep);
			job.setMapperClass(ExtractDocumentMapper.class);
			job.setReducerClass(IdentityReducer.class);
			job.setNumReduceTasks(1);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(TextArrayWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(TextArrayWritable.class);
			
			job.setInputFormat(SequenceFileInputFormat.class);
			job.setOutputFormat(SequenceFileOutputFormat.class);
			
			return new Job(job);
		}
		
		if(type==JobType.EXTRACTURL_JOB){
			job.setJobName("extract_URL_stage_"+crawlerCurrentDeep);
			
			job.setMapperClass(ExtractURLMapper.class);
			job.setReducerClass(IdentityReducer.class);
						
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setInputFormat(SequenceFileInputFormat.class);
			job.setOutputFormat(SequenceFileOutputFormat.class);
			
			return new Job(job);
		}
		
		if(type==JobType.COUNT_COOCURRENCE_JOB){
			job.setJobName("count_coocurrence_stage");
			
			job.setMapperClass(UserVectorToCooccurenceMapper.class);
			job.setReducerClass(UserVectorToCooccurenceReduce.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(MapWritable.class);
			
			job.setInputFormat(SequenceFileInputFormat.class);
			job.setOutputFormat(SequenceFileOutputFormat.class);
			
			return new Job(job);
			
		}
		
		return null;
	}
}
