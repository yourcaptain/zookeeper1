package zooApp;

import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.*;
import org.apache.zookeeper.ZooKeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;

import org.apache.zookeeper.AsyncCallback;

public class client implements Watcher {

	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
	
	public static void main(String[] args)  {
		try{
			ZooKeeper client = new ZooKeeper("ubuntuserver1:2181", 5000, new client());
			System.out.println(client.getState());
			
			//等待客户端响应
			try{
				connectedSemaphore.await();
			} catch(InterruptedException e){
				System.out.println(e.getMessage());
			}
			
			System.out.println("ZooKeeper session established.");
			
			
			if (args[0].equals("create")){
				/*同步接口调用需要关注异常，但异步调用无需关注异常，所有的异常都会在
				 * 回调函数中通过Result Code来体现。*/
				
				/*
				//同步接口创建临时节点
				try{
					String path1 = client.create("/zk-test-ephemeral-", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
					System.out.println("Success create znode:" + path1);
				}catch(NodeExistsException e){
					System.out.println("Node:/zk-test-ephemeral- exists.  ");
					System.out.println(e.getMessage());
				}
				catch(Exception e){
					System.out.println(e.getMessage());
				}
				
				//同步接口创建临时顺序节点
				try{
					String path1 = client.create("/zk-test-ephemeral-", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
					System.out.println("Success create znode:" + path1);
				}
				catch(Exception e){
					System.out.println(e.getMessage());
				}
				*/
				
				client.create("/zk-test-ephemeral-", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, new IStringCallback(), "I am context.");
				//等待客户端响应
				//否则Callback不会被触发
				try{
					//Thread.sleep(Integer.MAX_VALUE);
					connectedSemaphore = new CountDownLatch(1);
					connectedSemaphore.await();
				} catch(InterruptedException e){
					System.out.println(e.getMessage());
				}
			}
		}catch(Exception ex){
			System.out.println(ex.getMessage());
		}

	}
	
	/*
	 * 异步调用接口回调函数
	 * */
	public void process(WatchedEvent event){
		System.out.println("Receive watched event:" + event);
		if (KeeperState.SyncConnected == event.getState()){
			connectedSemaphore.countDown();
		}
	}

}

/*
 * 异步调用接口回调函数
 * */
class IStringCallback implements AsyncCallback.StringCallback{
	@Override
	public void processResult(int rc, String path, Object ctx, String name){
		switch (KeeperException.Code.get(rc)){
	        case CONNECTIONLOSS:
	            //runForMaster();
	        	System.out.println("CONNECTIONLOSS");
	            break;
	        case NODEEXISTS:
	        	System.out.println("NODEEXISTS");
	            break;
	        case OK:
	        	System.out.println("OK");
	            break;
	        default:
	        	System.out.println("Something went wrong when running for master." );
		}
		System.out.println("Create path result: [" + rc + "." + path + ", " 
				+ ctx + ", real path name: " + name);
	}
}
