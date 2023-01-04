package cn.bigdata.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
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


        CuratorCache curatorCache = CuratorCache.build(curatorFramework, "/test02");

        curatorCache.listenable().addListener(new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                switch (type){
                    case NODE_CHANGED:
                        String node_name = data.getPath().replace("/test02", "");
                        System.out.println("变更节点名称为" + node_name);
                        System.out.println("变更节点数据为" + new String(data.getData()));

                        break;
                    case NODE_CREATED:
                        String node_name2 = data.getPath().replace("/test02", "");
                        System.out.println("变更节点名称为" + node_name2);
                        System.out.println("变更节点数据为" + new String(data.getData()));

                        break;
                    case NODE_DELETED:
                        String node_name3 = data.getPath().replace("/test02", "");
                        System.out.println("变更节点名称为" + node_name3);
                        System.out.println("变更节点数据为" + new String(data.getData()));
                        break;
                    default:
                        break;
                }
            }
        });

        curatorCache.start();
        Thread.sleep(Integer.MAX_VALUE);

        curatorFramework.close();



    }

}
