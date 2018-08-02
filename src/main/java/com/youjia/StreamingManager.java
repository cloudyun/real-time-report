package com.youjia;

import org.I0Itec.zkclient.ZkClient;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.youjia.util.LoadConfig;
import com.youjia.util.LoadConfig.Config;

public class StreamingManager {

	public Config config;

	public SparkConf sparkConf;

	public JavaStreamingContext jsc;

	public String brokers;

	public String zkHost;

	public ZkClient zkClient;

	private static StreamingManager instance;

	public static StreamingManager getInstance(String taskName) {
		if (instance == null) {
			instance = new StreamingManager(taskName);
		}
		return instance;
	}

	private StreamingManager(String taskName) {
		/**
		 * streaming 配置
		 */
		config = LoadConfig.getInstance("SparkStreaming");

		// 获取sparkstreaming
		sparkConf = new SparkConf().setAppName("Analysis-" + taskName);

		sparkConf.set("spark.streaming.unpersist", config.get("spark.streaming.unpersist"));

		// 设置一个批次从kafka拉取的数据
		sparkConf.set("spark.streaming.kafka.maxRatePerPartition", config.get("spark.streaming.kafka.maxRatePerPartition"));

		// sparkConf.setMaster("local[3]")

		sparkConf.set("spark.default.parallelism", config.get("spark.default.parallelism"));

		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

		sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC");

		long durations = Long.parseLong(config.get("Durations.seconds"));
		jsc = new JavaStreamingContext(sparkConf, Durations.seconds(durations));

		brokers = config.get("brokers");

		zkHost = config.get("zkHost");

		zkClient = new ZkClient(zkHost);
	}

	public void sscRun() {
		// 开启
		jsc.start();
		// 等待
		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
