package com.study.demo.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

/**
 * 改变znode数据并监听事件。
 * 注意点：zookeeper监听事件只生效一次，比如下面代码中，在zk.exists()中设置了监听，zk.create()后会触发NodeCreated，然后失效。
 * 			在zk.getData()中设置了监听，zk.setData()后触发NodeDataChanged，然后失效。
 */
public class ZKDataDemo implements Watcher {
	private static final CountDownLatch cdl = new CountDownLatch(1);
	private static ZooKeeper zk = null;

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		zk = new ZooKeeper("localhost:2181", 30000, new ZKDataDemo());
		cdl.await();

		String path = "/zk-test";
		// 创建临时节点，创建节点前判空，判空操作中设置watch为true，表示开启节点监听，节点监听只会生效一次
		if (!isNodeExist(path)) {
			zk.create(path, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}

		// 修改节点信息，操作前先打印节点信息，调用getData()方法，其中设置watch为true，与上面作用一样
		printNodeInfo(path);
		zk.setData(path, "123".getBytes(), -1);

		Thread.sleep(Integer.MAX_VALUE);
	}

	/**
	 * 监听到事件时进行处理
	 * @param event
	 */
	public void process(WatchedEvent event) {
		if (KeeperState.SyncConnected == event.getState()) {
			if (EventType.None == event.getType() && null == event.getPath()) { // 初次连接
				cdl.countDown();
			} else if (event.getType() == EventType.NodeDataChanged) { // 修改节点事件
				System.out.println("================ 节点修改成功 ================");
				printNodeInfo(event.getPath());
			} else if (event.getType() == EventType.NodeCreated) { // 创建节点事件
				System.out.println("================ 节点创建成功 ================");
			}
		}
	}

	/**
	 * 打印节点信息
	 * @param path
	 */
	private static void printNodeInfo(String path) {
		try {
			System.out.println("--------------- 打印节点信息 ----------------");
			Stat stat = new Stat();
			// watch设置为true，监听当前节点变化
			System.out.println(new String(zk.getData(path, true, stat)));
			System.out.println(stat.getCzxid() + ", " + new Date(stat.getMtime()) + ", " + stat.getMzxid() + ", " + stat.getVersion());
		} catch (Exception e) {
		}
	}

	/**
	 * 判断节点是否存在
	 * @param nodePath
	 * @return
	 */
	private static boolean isNodeExist(String nodePath) {
		try {
			// watch设置为true，监听当前节点变化
			return zk.exists(nodePath, true) != null;
		} catch (Exception e) {
		}
		return false;
	}
}
