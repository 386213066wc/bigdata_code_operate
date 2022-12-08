package cn.bigdata.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

public class ZkOpt {
    public static void main(String[] args) throws Exception {

        //第一步需要连接上zookeeper集群  连接ip地址：2181

        ExponentialBackoffRetry exponentialBackoffRetry = new ExponentialBackoffRetry(1000, 3, Integer.MAX_VALUE);

        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("192.168.52.100:2181", exponentialBackoffRetry);

        curatorFramework.start();

        //创建一些节点的操作
/*
        String result = curatorFramework.create()
                .creatingParentsIfNeeded()  //如果没有父节点，先创建父节点
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath("/test02/node01", "123".getBytes());
*/

/*
        String result = curatorFramework.create()
                .creatingParentsIfNeeded()  //如果没有父节点，先创建父节点
                .withMode(CreateMode.EPHEMERAL)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath("/test02/node02", "123".getBytes());*/


      //  curatorFramework.delete().forPath("/test02/node01");

      /*  curatorFramework.delete()
                        .deletingChildrenIfNeeded()
                                .forPath("/test02");*/

       //设置监听机制


        TreeCache treeCache = new TreeCache(curatorFramework, "/test02");
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                if(data !=null){
                    switch (event.getType()){
                        case NODE_ADDED:
                            System.out.println("node added " +  data.getPath() + "数据为" + new String(data.getData()));
                            break;
                        case NODE_REMOVED:  //删除节点
                            System.out.println("NODE_REMOVED : "+ data.getPath() +"  数据:"+ new String(data.getData()));
                            break;

                        case NODE_UPDATED:  //修改节点
                            System.out.println("NODE_UPDATED : "+ data.getPath() +"  数据:"+ new String(data.getData()));

                            break;

                        default:

                            break;

                    }
                }


            }
        });

        treeCache.start();//开始监听

        Thread.sleep(Integer.MAX_VALUE);

        curatorFramework.close();



    }

}
