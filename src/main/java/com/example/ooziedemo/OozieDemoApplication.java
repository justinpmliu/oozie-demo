package com.example.ooziedemo;

import com.example.ooziedemo.config.JobProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob.Status;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(JobProperties.class)
public class OozieDemoApplication implements CommandLineRunner {

	@Autowired
	private JobProperties jobProperties;

	public static void main(String[] args) {
		SpringApplication.run(OozieDemoApplication.class, args);
	}

	@Override
	public void run(String... args) {
		//get a OozieClient for local Oozie
		OozieClient wc = new OozieClient(jobProperties.getOozieUrl());

		//create workflow job configuration
		Properties conf = wc.createConfiguration();
		conf.setProperty(OozieClient.APP_PATH, jobProperties.getNameNode() + jobProperties.getBasePath());
		conf.setProperty(OozieClient.USE_SYSTEM_LIBPATH, jobProperties.getUseSystemLibpath());
		conf.setProperty("nameNode", jobProperties.getNameNode());
		conf.setProperty("resourceManager",jobProperties.getResourceManager());
		conf.setProperty("jdbcUrl", jobProperties.getJdbcUrl());
		conf.setProperty("user.name", jobProperties.getUsername());
		conf.setProperty("password", jobProperties.getPassword());
		conf.setProperty("hiveSql", jobProperties.getBasePath() + "/" + jobProperties.getHiveSql());
		conf.setProperty("movieCategory", args[0]);


		//submit and start the workflow job
		try{
			String jobId = wc.run(conf);
			System.out.println("Workflow job submitted");

			//wait until the workflow job finishes
			Status status;
			while((status = wc.getJobInfo(jobId).getStatus()) == Status.RUNNING){
				System.out.println("Workflow job running...");
				try{
					Thread.sleep(10*1000);
				}catch(InterruptedException e){
					e.printStackTrace();
				}
			}
			System.out.println("Workflow job completed! Status is " + status);
			System.out.println(wc.getJobId(jobId));
		}catch(OozieClientException e){
			e.printStackTrace();
		}
	}
}
