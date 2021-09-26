package com.study.demo.client;

import org.I0Itec.zkclient.ZkClient;

/**
*
* @Description: ZkClient递归创建顺序节点
* @author leeSmall
* @date 2018年9月2日
*
*/
public class CreateNodeDemo {
	public static void main(String[] args) {
		ZkClient client = new ZkClient("localhost:2181", 5000);
		String path = "/zk-client/c1";
		// 递归创建顺序节点 true：先创建父节点/zk-client
		client.createPersistent(path, true);
	}
}
