package com.youjia.analysis;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;

/**  
 * @Title:  IDoAnalysis.java   
 * @Package com.youjia.analysis   
 * @Description:    (流任务接口)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年8月1日 下午4:16:27   
 * @version V1.0 
 */ 
public interface IDoAnalysis {
	
	/**
	 * 初始化参数
	 */
	void init(String name);

	/**
	 * 获取DirectStream
	 * @return
	 */
	JavaInputDStream<ConsumerRecord<String, String>> createDirectStream();
	
	/**
	 * 处理分析
	 * @param stream
	 */
	void analysis(JavaInputDStream<ConsumerRecord<String, String>> stream);
	
	/**
	 * 更新offset到存储
	 * @param stream
	 */
	void updateOffset(JavaInputDStream<ConsumerRecord<String, String>> stream);
}
