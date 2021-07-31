package com.study.demo.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

//创建znode并监听事件
public class ZKOperateDemo implements Watcher {
	private static final CountDownLatch cdl = new CountDownLatch(1);

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		ZooKeeper zk = new ZooKeeper("localhost:2181", 5000, new ZKOperateDemo());
		cdl.await();

		// 创建一个临时节点(连接关闭后删除)
		String path1 = zk.create("/zk-test-", "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		System.out.println("Success create path: " + path1);

		// 创建一个临时顺序节点(连接关闭后删除)
		String path2 = zk.create("/zk-test-", "456".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("Success create path: " + path2);

		// 创建一个顺序节点(永久存在)
		String path3 = zk.create("/zk-test-", "789".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		System.out.println("Success create path: " + path3);

		// 创建一个永久节点(永久存在)
		String path4 = zk.create("/zk-test-permanent", "xxx".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("Success create path: " + path4);
	}

	//监听到事件时进行处理
	public void process(WatchedEvent event) {
		System.out.println("Receive watched event:" + event);
		if (KeeperState.SyncConnected == event.getState()) {
			cdl.countDown();
		}
	}
}
