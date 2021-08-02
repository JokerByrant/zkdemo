package com.study.demo.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 创建子节点：异步调用并完成回调
 * 相对于监听器，回调事件似乎更有针对性，监听器的生效还需要在对应操作前完成注册监听器的操作，回调事件直接在操作时将对应的回调事件传入即可。
 */
class ChildrenCallback implements AsyncCallback.Children2Callback {

	public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
		System.out.println(
				"Child: " + rc + ", path: " + path + ", ctx: " + ctx + ", children: " + children + ", stat: " + stat);
	}
}

public class ZKChildrenAsyncDemo implements Watcher {
	private static final CountDownLatch cdl = new CountDownLatch(1);
	private static ZooKeeper zk = null;

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		zk = new ZooKeeper("localhost:2181", 5000, new ZKChildrenAsyncDemo());
		cdl.await();

		if (zk.exists("/zk-test", true) == null) {
			zk.create("/zk-test", "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		// 第一次创建子节点，不会触发监听器
		zk.create("/zk-test/c1", "456".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

		zk.getChildren("/zk-test", true, new ChildrenCallback(), "ok");

		// 第二次创建子节点，触发监听器
		zk.create("/zk-test/c2", "789".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

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
