package mr;

import java.util.UUID;

import mr.common.Constants.JOB_STATUS;
import mr.common.Constants.MSG_TP;
import mr.common.Msg;
import mr.common.Network;
import mr.core.*;
import conf.Config;

public class Job {
	
	Mapper mapper = null;
	String fileInputPath = null;
	String fileOutputPath = null;
	
	public void set_mapper(Mapper mapper)
	{
		this.mapper = mapper;
	}
	
	public void set_fileInputPath(String path)
	{
		this.fileInputPath = path;
	}
	
	public void set_fileOutputPath(String path)
	{
		this.fileOutputPath = path;
	}
	
	private void registerJob()
	{
		Long uuid = UUID.randomUUID().getMostSignificantBits();
		String job_id = String.valueOf(uuid);
		Msg msg = new Msg();
		msg.setJob_id(job_id);
		msg.setMsg_tp(MSG_TP.SET_JOB_STATUS);
		msg.setJob_stat(JOB_STATUS.STARTING);
		Msg ret_msg = Network.communicate(Config.MASTER_IP, Config.MASTER_PORT, msg);
		System.out.println(ret_msg.getContent());
	}
	
	public void submit()
	{
		// Register job in MetaDataTBL, set job status to Starting
		registerJob();
		// FileSplitter begin to split file (require block, read till meet split size, write to block)
		FileSplitter fp = new FileSplitter();
		fp.readFiles(this.fileInputPath);
		// Scheduler assign task to Compute Nodes
		
		// Set job status to Started in MetaDataRBL
		
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.set_fileInputPath("/Users/yanan/javapro/workspace/testfiles");
		job.set_fileOutputPath("s3://test");
		job.submit();
	}

}
