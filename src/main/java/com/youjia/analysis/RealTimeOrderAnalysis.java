package com.youjia.analysis;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaInputDStream;

/**  
 * @Title:  RealTimeOrderAnalysis.java   
 * @Package com.youjia.analysis   
 * @Description:    (用一句话描述该文件做什么)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年8月1日 下午4:16:13   
 * @version V1.0 
 */ 
public class RealTimeOrderAnalysis extends DoAnalysisAbstract {

	public RealTimeOrderAnalysis(String name) {
		super(name);
		logger = Logger.getLogger(this.getClass());
		setLoggerLevel(Level.WARN);
	}

	@Override
	public void analysis(JavaInputDStream<ConsumerRecord<String, String>> stream) {
		// TODO Auto-generated method stub
	}
	
	public static void main(String[] args) {
		RealTimeOrderAnalysis analysis = new RealTimeOrderAnalysis("config.real-time-order");
		JavaInputDStream<ConsumerRecord<String, String>> stream = analysis.createDirectStream();
		analysis.analysis(stream);
		analysis.updateOffset(stream);
		analysis.getSm().sscRun();
	}
}