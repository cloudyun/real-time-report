package com.youjia.util;

import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**  
 * @Title:  TaskConfig.java   
 * @Package com.wave.util   
 * @Description:    (读取任务配置)   
 * @author: gaoyun     
 * @edit by: 
 * @date:   2018年5月30日 下午3:20:25   
 * @version V1.0 
 */ 
public class LoadConfig {

	private static Map<String, Config> configs = null;
	
	public static Config getInstance(String name) {
		if (configs == null) {
			configs = new HashMap<String, Config>();
		}
		Config config = configs.get(name);
		if (config != null) {
			return config;
		}
		config = new Config(name);
		configs.put(name, config);
		return config;
	}
	
	public static class Config {
		
		private ResourceBundle bundle;
		
		public Config(String name) {
			bundle = ResourceBundle.getBundle(name);
		}
		
		public String get(String key) {
			try {
				return bundle.getString(key);
			} catch (Exception e) {
				return null;
			}
		}
		
		public String get(String key, String default_value) {
			String value = null;
			try {
				value = bundle.getString(key);
			} catch (Exception e) {
				value = null;
			}
			if (value != null) {
				return value;
			}
			return default_value;
		}
	}
}