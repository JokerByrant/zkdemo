package com.study.demo.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 改变子节点并监听事件
 * 注：重写Watcher的process方法达到了循环监听的效果，即一次监听事件触发后监听器仍然生效。
 * getChildren()方法上的提示如下：
 * 如果该监视为true，并且调用成功(没有抛出异常)，则将在给定路径的节点上留下一个监视。
 * 成功删除给定路径的节点或在该节点下创建/删除子节点的操作将触发监视。
 * 返回的子列表不进行排序，并且不保证其自然顺序或词法顺序。
 */
public class ZKChildrenDemo implements Watcher {
	private static final CountDownLatch cdl = new CountDownLatch(1);
	private static ZooKeeper zk = null;

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		zk = new ZooKeeper("localhost:2181", 5000, new ZKChildrenDemo());
		cdl.await();

		if (zk.exists("/zk-test", true) == null) {
			zk.create("/zk-test", "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		zk.getChildren("/zk-test", true);
		zk.create("/zk-test/c1", "456".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

		// 下面这几个操作都能触发监听器，因为重写了process方法，达到了循环监听的效果
		zk.create("/zk-test/c2", "789".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		zk.create("/zk-test/c3", "789".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		zk.create("/zk-test/c4", "789".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		zk.create("/zk-test/c5", "789".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

		Thread.sleep(Integer.MAX_VALUE);
	}

	//监听到事件时进行处理
	public void process(WatchedEvent event) {
		if (KeeperState.SyncConnected == event.getState())
			if (EventType.None == event.getType() && null == event.getPath()) {
				cdl.countDown();
			} else if (event.getType() == EventType.NodeChildrenChanged) {
				try {
					System.out.println("Child: " + zk.getChildren(event.getPath(), true));
				} catch (Exception e) {
				}
			}
	}
}
