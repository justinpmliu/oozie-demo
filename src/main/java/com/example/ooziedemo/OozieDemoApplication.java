package com.example.ooziedemo;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob.Status;

@SpringBootApplication
public class OozieDemoApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(OozieDemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		//get a OozieClient for local Oozie
		OozieClient wc = new OozieClient("http://localhost:11000/oozie");

		//create workflow job configuration
		Properties conf = wc.createConfiguration();
		conf.setProperty(OozieClient.APP_PATH, "hdfs://quickstart.cloudera:8020/user/cloudera/examples/apps/map-reduce");
		conf.setProperty("nameNode", "hdfs://quickstart.cloudera:8020");
		conf.setProperty("jobTracker", "localhost:8032");
		conf.setProperty("examplesRoot", "examples");
		conf.setProperty("outputDir", "map-reduce");
		conf.setProperty("queueName", "default");
		conf.setProperty("user.name", "cloudera");

		//submit and start the workflow job
		try{
			String jobId = wc.run(conf);
			System.out.println("Workflow job submitted");

			//wait until the workflow job finishes
			while(wc.getJobInfo(jobId).getStatus() == Status.RUNNING){
				System.out.println("Workflow job running...");
				try{
					Thread.sleep(10*1000);
				}catch(InterruptedException e){
					e.printStackTrace();
				}
			}
			System.out.println("Workflow job completed!");
			System.out.println(wc.getJobId(jobId));
		}catch(OozieClientException e){
			e.printStackTrace();
		}
	}
}
