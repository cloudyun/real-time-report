package com.youjia.analysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import com.youjia.StreamingManager;
import com.youjia.util.LoadConfig;
import com.youjia.util.LoadConfig.Config;

import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;

/**  
 * @Title:  AnalysisTest.java   
 * @Package com.youjia.analysis   
 * @Description:    TODO(用一句话描述该文件做什么)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年7月30日 下午5:41:40   
 * @version V1.0 
 */ 
public abstract class DoAnalysisAbstract implements IDoAnalysis {
	
	protected transient Logger logger;

	private StreamingManager sm;

	private Map<String, Object> kafkaParams;

	private String topic;

	private Collection<String> topics;

	// 创建一个ZKGroupTopicDirs对象，对保存
	private ZKGroupTopicDirs topicDirs;

	// 查询该路径下是否字节点（默认有字节点为我们自己保存不同partition时生成的）
	private int children;

	public DoAnalysisAbstract(String name) {
		init(name);
	}
	
	@Override
	public void init(String name) {
		logger = Logger.getLogger(this.getClass());
		
		Config config = LoadConfig.getInstance(name);
		sm = StreamingManager.getInstance(config.get("sparkJobName"));
		
		topic = config.get("topic");
		topics = Arrays.asList(topic);
		topicDirs = new ZKGroupTopicDirs(config.get("zk.group"), topic);
		children = sm.zkClient.countChildren(topicDirs.consumerOffsetDir());
		
		kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", sm.brokers);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", config.get("group.id"));
		kafkaParams.put("auto.offset.reset", config.get("auto.offset.reset"));
		kafkaParams.put("enable.auto.commit", false);
	}
	
	/**
	 * 设置日志级别
	 * @param level
	 */
	public void setLoggerLevel(Level level) {
		logger.setLevel(level);
	    Logger.getRootLogger().setLevel(level);
	    Logger.getLogger("org.apache.hadoop").setLevel(level);
	    Logger.getLogger("org.apache.spark").setLevel(level);
	    Logger.getLogger("org.apache.kafka").setLevel(level);
	    Logger.getLogger("org.apache.zookeeper").setLevel(level);
	}

	/**
	 * 获取DirectStream
	 * @return
	 */
	@Override
	public JavaInputDStream<ConsumerRecord<String, String>> createDirectStream() {
		JavaInputDStream<ConsumerRecord<String, String>> stream = null;
		if (children > 0) {
			Map<TopicPartition, Long> fromOffsets = new HashMap<>();
			for (int x = 0; x < children; x++) {
				Long offset = sm.zkClient.readData(topicDirs.consumerOffsetDir() + "/" + x);
				fromOffsets.put(new TopicPartition(topic, x), offset);
			}
			stream = KafkaUtils.createDirectStream(sm.jsc, LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<String, String>Assign(fromOffsets.keySet(), kafkaParams, fromOffsets));
		} else {
			stream = KafkaUtils.createDirectStream(sm.jsc, LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		}
		return stream;
	}

	/**
	 * 将offset更新到zookeeper
	 * @param stream
	 */
	@Override
	public void updateOffset(JavaInputDStream<ConsumerRecord<String, String>> stream) {
		stream.foreachRDD(rdd -> {
			OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			for (OffsetRange offsetRange : offsetRanges) {
				String zkPath = topicDirs.consumerOffsetDir() + "/" + offsetRange.partition();
				ZkUtils zkUtils = ZkUtils.apply(sm.zkClient, false);
				ArrayList<ACL> acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;
				zkUtils.updatePersistentPath(zkPath, String.valueOf(offsetRange.fromOffset()), acls);
			}
		});
	}
}