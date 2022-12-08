package cn.hdfs.opt;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsCode {


    /**
     * hdfs上面创建一个文件夹
     */
    @Test
    public void mkdiHDFSDir() throws IOException {

        //有一个核心类 FileSysteme
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS","hdfs://bigdata01:8020");
        //一定要获取到这个配置属性
        /**
         *  <property>
         *         <name>fs.defaultFS</name>
         *         <value>hdfs://bigdata01:8020</value>
         *     </property>
         */
        FileSystem fileSystem = FileSystem.get(configuration);

        Path path = new Path("/opt/dir1");

        fileSystem.mkdirs(path);

        //操作完了，一定要关闭文件系统
        fileSystem.close();

    }

    @Test
    public void mkdiHDFSDir2() throws IOException, URISyntaxException, InterruptedException {

        //有一个核心类 FileSysteme
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS","hdfs://bigdata01:8020");
        //一定要获取到这个配置属性
        /**
         *  <property>
         *         <name>fs.defaultFS</name>
         *         <value>hdfs://bigdata01:8020</value>
         *     </property>
         */
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata01:8020"), configuration, "test");


        Path path = new Path("/opt2/dir2");

        fileSystem.mkdirs(path);

        //操作完了，一定要关闭文件系统
        fileSystem.close();

    }


    /**
     * 设置文件夹的访问权限
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void mkdiHDFSDir3() throws IOException, URISyntaxException, InterruptedException {

        //有一个核心类 FileSysteme
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS","hdfs://bigdata01:8020");
        //一定要获取到这个配置属性
        /**
         *  <property>
         *         <name>fs.defaultFS</name>
         *         <value>hdfs://bigdata01:8020</value>
         *     </property>
         */
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata01:8020"), configuration, "test");


        Path path = new Path("/opt2/dir2");

        FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.READ, FsAction.READ);

        fileSystem.mkdirs(path,fsPermission);

        //操作完了，一定要关闭文件系统
        fileSystem.close();

    }

    /*
    上传文件到hdfs上面去
     */

    @Test
    public void uploadFile2HDFS() throws URISyntaxException, IOException, InterruptedException {

        //有一个核心类 FileSysteme
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS","hdfs://bigdata01:8020");
        //一定要获取到这个配置属性
        /**
         *  <property>
         *         <name>fs.defaultFS</name>
         *         <value>hdfs://bigdata01:8020</value>
         *     </property>
         */
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata01:8020"), configuration, "test");

        fileSystem.copyFromLocalFile(new Path("file:///C:\\Users\\Administrator\\Desktop\\新建文件夹\\a.txt"),new Path("/"));


        fileSystem.close();

    }

    @Test
    public void downLoadFile() throws IOException {

        //有一个核心类 FileSysteme
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS","hdfs://bigdata01:8020");
        //一定要获取到这个配置属性
        /**
         *  <property>
         *         <name>fs.defaultFS</name>
         *         <value>hdfs://bigdata01:8020</value>
         *     </property>
         */
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyToLocalFile(new Path("hdfs://bigdata01:8020/a.txt"),new Path("file:///C:\\Users\\Administrator\\Desktop\\新建文件夹\\b.txt"));

        fileSystem.close();

    }

    /**
     * 获取文件的详情信息
     */
    @Test
    public void listFiles() throws IOException {

        //有一个核心类 FileSysteme
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS","hdfs://bigdata01:8020");
        //一定要获取到这个配置属性
        /**
         *  <property>
         *         <name>fs.defaultFS</name>
         *         <value>hdfs://bigdata01:8020</value>
         *     </property>
         */
        FileSystem fileSystem = FileSystem.get(configuration);

        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("hdfs://bigdata01:8020/a.txt"), true);

        while(listFiles.hasNext()){

            LocatedFileStatus status = listFiles.next();

            String name = status.getPath().getName();
            System.out.println(name);

            long len = status.getLen();
            System.out.println(len);

            FsPermission permission = status.getPermission();
            System.out.println(permission);

            System.out.println(status.getGroup());

            //文件有多少个block块
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }


        }


        fileSystem.close();


    }


    /**
     * hdfs上面创建一个文件
     *
     */
    @Test
    public void hdfsMkFile() throws IOException {

        //有一个核心类 FileSysteme
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS","hdfs://bigdata01:8020");
        //一定要获取到这个配置属性
        /**
         *  <property>
         *         <name>fs.defaultFS</name>
         *         <value>hdfs://bigdata01:8020</value>
         *     </property>
         */
        FileSystem fileSystem = FileSystem.get(configuration);

        FileInputStream fis = new FileInputStream(new File("C:\\Users\\Administrator\\Desktop\\新建文件夹\\a.txt"));

        FSDataOutputStream fos = fileSystem.create(new Path("hdfs://bigdata01:8020/helloabc.txt"));

        //进行流数据的拷贝
        IOUtils.copy(fis,fos);
        //关闭流操作
        IOUtils.close(fis);
        IOUtils.close(fos);

        fileSystem.close();

    }


    @Test
    public void mergeFile() throws IOException {
        //有一个核心类 FileSysteme
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS","hdfs://bigdata01:8020");
        //一定要获取到这个配置属性
        /**
         *  <property>
         *         <name>fs.defaultFS</name>
         *         <value>hdfs://bigdata01:8020</value>
         *     </property>
         */
        FileSystem fileSystem = FileSystem.get(configuration);

        //hdfs上面创建一个文件
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("hdfs://bigdata01:8020/hellobcd.txt"));


        //获取本地文件系统  将本地文件，追加到hdfs的文件上面去
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("file:///C:\\Users\\Administrator\\Desktop\\新建文件夹"));

        for (FileStatus fileStatus : fileStatuses) {

            Path path = fileStatus.getPath();
            FSDataInputStream fsDataInputStream = localFileSystem.open(path);
            IOUtils.copy(fsDataInputStream,fsDataOutputStream);
            IOUtils.close(fsDataInputStream);

        }
        IOUtils.close(fsDataOutputStream);
        localFileSystem.close();
        fileSystem.close();
        //获取hdfs分布式文件系统
        //将本地文件数据，上传到hdfs的同一个文件里面去




    }





}
