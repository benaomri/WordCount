import java.io.IOException;
import java.util.StringTokenizer;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class WordCount {
    public static final String ENGLISH_3Grams = "s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data";
    private static final String AFTER_DELETION_BUCKET = "s3://after-deletion-bucket-omri";
    private static final String myKeyPair = "Omri_EMR";


    public static void main(String[] args) throws Exception {

//        createNewCluster();
        addNewStep();

    }

    public static void createNewCluster(){

        BasicConfigurator.configure();

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(credentialsProvider)
                .build();


        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://omri-jars-bucekt-assigment2/WordCount.jar") // This should be a full mapreduce application.
                .withMainClass("StepMain")
                .withArgs(ENGLISH_3Grams, AFTER_DELETION_BUCKET);

        StepConfig stepConfig = new StepConfig()
                .withName("word count")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("CANCEL_AND_WAIT");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M1Large.toString())
                .withSlaveInstanceType(InstanceType.M1Large.toString())
                .withHadoopVersion("2.6.2")
                .withEc2KeyName(myKeyPair)
                .withKeepJobFlowAliveWhenNoSteps(true)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withReleaseLabel("emr-4.2.0")
                .withName("OmriTests")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri(AFTER_DELETION_BUCKET + "/logs")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole");



        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);

        String jobFlowId = runJobFlowResult.getJobFlowId();

        System.out.println("Job flow id "+jobFlowId);

    }
    public static void addNewStep(){

        BasicConfigurator.configure();

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(credentialsProvider)
                .build();


        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://omri-jars-bucekt-assigment2/WordCount.jar") // This should be a full mapreduce application.
                .withMainClass("StepMain")
                .withArgs(ENGLISH_3Grams, AFTER_DELETION_BUCKET);

        StepConfig stepConfig = new StepConfig()
                .withName("word count")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("CANCEL_AND_WAIT");


// Add the step to the cluster
        AddJobFlowStepsRequest request = new AddJobFlowStepsRequest()
                .withJobFlowId("j-1W9R5URM3RVAF")
                .withSteps(stepConfig);
        AddJobFlowStepsResult runJobFlowResult =  mapReduce.addJobFlowSteps(request);

        String jobFlowId = runJobFlowResult.getStepIds().get(0);
        System.out.println("Job flow id "+jobFlowId);

    }

    public static void addNewStep3Gram(){

        BasicConfigurator.configure();

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(credentialsProvider)
                .build();


        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://omri-jars-bucekt-assigment2/.jar") // This should be a full mapreduce application.
                .withMainClass("StepMain")
                .withArgs(ENGLISH_3Grams, AFTER_DELETION_BUCKET);

        StepConfig stepConfig = new StepConfig()
                .withName("word count")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("CANCEL_AND_WAIT");


// Add the step to the cluster
        AddJobFlowStepsRequest request = new AddJobFlowStepsRequest()
                .withJobFlowId("j-1W9R5URM3RVAF")
                .withSteps(stepConfig);
        AddJobFlowStepsResult runJobFlowResult =  mapReduce.addJobFlowSteps(request);

        String jobFlowId = runJobFlowResult.getStepIds().get(0);
        System.out.println("Job flow id "+jobFlowId);

    }

}