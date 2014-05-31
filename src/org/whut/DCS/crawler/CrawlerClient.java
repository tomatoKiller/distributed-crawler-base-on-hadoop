package org.whut.DCS.crawler;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.whut.DCS.utils.DCSUtils;
import org.whut.DCS.utils.JobFactory;
import org.whut.DCS.utils.JobFactory.FileFilter;
import org.whut.DCS.utils.JobFactory.JobType;


public class CrawlerClient {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws URISyntaxException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		// TODO Auto-generated method stub
		String crawlerConfFilePath="crawler.xml";
		
		
		Configuration conf=new Configuration();
		
		
		conf.addResource(crawlerConfFilePath);
		
		
		int crawlerDeep=conf.getInt("crawler.deep",DCSUtils.CRAWLER_DEEP);
		
		
		long waitJobCompleteMillisecond=conf.getLong("wait.job.finished.millisecond",DCSUtils.WAIT_JOB_FINISHED_MILLISECOND);
		
		
		int crawlerCurrentDeep=0;
		
		
		conf.setInt("crawler.current.deep",crawlerCurrentDeep);
		
		
		String seedFilesDir=conf.get("seed.files.dir",DCSUtils.SEED_FILES_DIR);
		
		String seedFilesTmpDir=conf.get("seed.files.tmp.dir", DCSUtils.SEED_FILES_TMP_DIR);
		
	
		String crawlerResultTmpDir=conf.get("crawler.result.tmp.dir",DCSUtils.CRAWLER_RESULT_TMP_DIR);
		
		String updateBloomFilterJobOuputDir=conf.get("update.bloom.filter.job.output.dir", DCSUtils.UPDATE_BLOOM_FILTER_JOB_OUTPUT_DIR);
	
		String crawlerResultDir=conf.get("crawler.result.dir",DCSUtils.CRAWLER_RESULT_DIR);
		
		String crawlerStandResultDir=conf.get("crawler.stand.result.dir", DCSUtils.CRAWLER_STAND_RESULT_DIR);
		
		Job crawlerJob=null;
		Job updateBloomFlterJob=null;
		Job extractDocumentJob=null;
		Job extractURLJob=null;
		
		JobControl controlJob=null;
		
		FileSystem hdfs=FileSystem.get(conf);
		while(hdfs.listStatus(new Path(seedFilesDir))!=null&&crawlerCurrentDeep<crawlerDeep){
			
			conf.setInt("crawler.current.deep",crawlerCurrentDeep);
						
			crawlerJob=JobFactory.createJob(new Path(seedFilesDir),new Path(crawlerResultTmpDir), conf,JobType.CRAWLER_JOB);
			updateBloomFlterJob=JobFactory.createJob(new Path(seedFilesDir),new Path(updateBloomFilterJobOuputDir), conf,JobType.UPDATEBLOOMFILTER_JOB);
			
			extractDocumentJob=JobFactory.createJob(new Path(crawlerResultTmpDir),new Path(crawlerResultDir+"/"+String.valueOf(crawlerCurrentDeep)), conf, JobType.EXTRACTDOCUMENT_JOB);
			extractURLJob=JobFactory.createJob(new Path(crawlerResultTmpDir),new Path(seedFilesTmpDir), conf,JobType.EXTRACTURL_JOB);
		
			extractDocumentJob.addDependingJob(crawlerJob);
			extractURLJob.addDependingJob(updateBloomFlterJob);
			extractURLJob.addDependingJob(crawlerJob);
		
			controlJob=new JobControl("CrawlerJob_"+String.valueOf(crawlerCurrentDeep));
		
			controlJob.addJob(crawlerJob);
			controlJob.addJob(updateBloomFlterJob);
			controlJob.addJob(extractDocumentJob);
			controlJob.addJob(extractURLJob);
		
			long startTime=System.currentTimeMillis();
			new Thread(controlJob).start();			
			
			while(!controlJob.allFinished())			
				Thread.sleep(waitJobCompleteMillisecond);
						
			controlJob.stop();
			System.out.println("当前深度: "+crawlerCurrentDeep+" 运行时间"+(System.currentTimeMillis()-startTime)/1000+"秒");
			
			Path seedFilesDirPath=new Path(seedFilesDir);
			FileStatus[] statuses=hdfs.listStatus(seedFilesDirPath);
			for(FileStatus status:statuses)
				hdfs.delete(status.getPath(), true);
			Path seedFilesTmpDirPath=new Path(seedFilesTmpDir);
			statuses=hdfs.listStatus(seedFilesTmpDirPath, new FileFilter());			
			for(FileStatus status:statuses){
				FSDataOutputStream out=hdfs.create(new Path(seedFilesDir+"/"+status.getPath().getName()));
				IOUtils.copyBytes(hdfs.open(status.getPath()),out,4096,true);
			}
					
			Path crawlerResultDirPath=new Path(crawlerResultDir);
						
			statuses=hdfs.listStatus(crawlerResultDirPath);
			for(FileStatus status:statuses){
				FileStatus[] ss=hdfs.listStatus(status.getPath(),new FileFilter());
				for(FileStatus s:ss){
					FSDataOutputStream out=hdfs.create(new Path(crawlerStandResultDir+"/"+crawlerCurrentDeep+s.getPath().getName()));
					IOUtils.copyBytes(hdfs.open(s.getPath()), out, 4096,true);
				}
			}
			
			Path crawlerResultTmpDirPath=new Path(crawlerResultTmpDir);
			if(hdfs.exists(crawlerResultTmpDirPath))
				hdfs.delete(crawlerResultTmpDirPath, true);
			if(hdfs.exists(seedFilesTmpDirPath))
				hdfs.delete(seedFilesTmpDirPath, true);
			Path updateBloomFilterJobOuputDirPath=new Path(updateBloomFilterJobOuputDir);
			if(hdfs.exists(updateBloomFilterJobOuputDirPath))
				hdfs.delete(updateBloomFilterJobOuputDirPath, true);
			
			if(hdfs.exists(crawlerResultDirPath))
				hdfs.delete(crawlerResultDirPath, true);
			crawlerCurrentDeep++;			
		}
				
		String coocurrenceResultDir=conf.get("coocurrence.result.dir",DCSUtils.COOCURRENCE_RESULT_DIR);
		JobConf count_coocurrence_conf=JobFactory.createJob(new Path(crawlerStandResultDir), new Path(coocurrenceResultDir), conf,JobType.COUNT_COOCURRENCE_JOB).getJobConf();
		JobClient.runJob(count_coocurrence_conf);
				
	}
}
