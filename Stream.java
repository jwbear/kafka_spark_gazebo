package com.ibm;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;



import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function; 
import org.apache.spark.api.java.function.VoidFunction2;

import org.apache.spark.streaming.kafka.*;
import kafka.serializer.StringDecoder;

/*
 * Stream Class:
 *  Consumes sensor data from kafka producer located on management node/
 *  designated node, data is processed by Data class and used by application
 * Takes place of normal kafka consumer
 */
public class Stream {
	private Set<String> topicSet;
	private Map<String, String> kafkaParams; 
	private ArrayList<double[]> odometry;
	private ArrayList<ArrayList<double[]>> laserData;
	private Data d;
	
	public Stream(JavaSparkContext sc){
		//storage
		odometry = new ArrayList<double[]>();
		laserData = new ArrayList<ArrayList<double[]>>();
		d = new Data();
		
		//set batch size
		//streaming context
		JavaStreamingContext jssc = 
				new JavaStreamingContext(sc, Durations.seconds(1));
		
		//topic list
		topicSet = new HashSet<String>(Arrays.asList("odom", "laser"));
		//parameter list
	    kafkaParams = new HashMap<String, String>();
	    kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "ekf");
        //set at movement of robot
        kafkaParams.put("auto.commit.interval.ms", "1");
        kafkaParams.put("consumer.timeout.ms", "10");

	    
		//connect to tcp
		/*JavaReceiverInputDStream<String> lines = 
				jssc.socketTextStream("localhost", 10555);

		lines.print();*/
		
		JavaPairInputDStream<String, String> msg = 
				KafkaUtils.createDirectStream(
					        jssc,
					        String.class,
					        String.class,
					        StringDecoder.class,
					        StringDecoder.class,
					        kafkaParams,
					        topicSet
					    );
		
		//msg.print();
		//TODO: process data for consumption using rdds
		//add EKF code
		
		//Get lines from stream
	    JavaDStream<String> lines = 
	    	msg.map(e -> e._1+","+e._2);
	    //lines.print(); 
	    
	  //Filter Predicate to get topics 
       Function<String, Boolean> filterLaser =
    		   e -> e.contains("[") == true;
       Function<String, Boolean> filterOdom = 
    		   e -> e.contains("[") == false;
       JavaDStream<String> laserTuple = lines.filter(filterLaser);
       JavaDStream<String> odomTuple = lines.filter(filterOdom);
       //odomTuple.print();
       //laserTuple.print();
       
 
       //parse and store dstream
       odomTuple.foreachRDD(
    		   new VoidFunction2<JavaRDD<String>,Time>() {
			   	    public void call(JavaRDD<String> rdd, Time time) {
			   	    	rdd.saveAsTextFile("data/stream_odom_input/"
			   	    			+ time.toString().split(" ")[0]);
			   	    	for(String l: rdd.collect()){
					  	      double[] odom = new double[4];
					  	      String[] data = l.split(",");
					  	      odom[0] = Double.valueOf(data[0]);
					  	      odom[1] = Double.valueOf(data[1]);
					  	      odom[2] = Double.valueOf(data[2]);
					  	      odom[3] = Double.valueOf(data[3]);
					  	      odometry.add(odom);
			   	    	}d.addOdometryStreamData(odometry);
			  	    }
	  	  });
       
       laserTuple.foreachRDD(
    		   new VoidFunction2<JavaRDD<String>,Time>() {
			   	    public void call(JavaRDD<String> rdd, Time time) {
			   	    	rdd.saveAsTextFile("data/stream_laser_input/"
			   	    			+ time.toString().split(" ")[0]);
			   	    	for(String l: rdd.collect()){
					  	      double[] laser = new double[3];
					  	      String[] data0 = l.split("\\[");
					  	      String[] tmp = 
					  	    		  data0[1].replaceAll("\\]", "").split(",");
					  	      double[] ranges = new double[tmp.length];
					  	      for(int i = 0; i < tmp.length; i++){
					  	    	 ranges[i] =  Double.valueOf(tmp[i]);
					  	      }
					  	      //get laser vectors
					  	      tmp = data0[0].split(",");
					  	      laser[0] = Double.valueOf(tmp[0]);
					  	      laser[1] = Double.valueOf(tmp[1]);
					  	      laser[2] = Double.valueOf(tmp[2]);
					  	      //add to store
					  	      ArrayList<double []> t = new ArrayList<double[]>();
					  	      t.add(laser);
					  	      t.add(ranges);
					  	      laserData.add(t);
					  	      //System.out.println(laser[2]);
			   	    	}d.addLaserStreamData(laserData);
			  	    }
	  	  });
       
	    
	 // Start the computation
	    jssc.start();
	    jssc.awaitTermination();
	}
}
