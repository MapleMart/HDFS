package com.hyf.hdfs;

import java.io.IOException;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
/**
 * HDFS操作:(1、连接到hadoop2.7.1的HDFS 2、在HDFS中创建一个文件夹，并上传一个文件  3、打印testhdfs1和derby.log的元数据 4、删除目录)
 * 在hadoop2.7.1解压出来,hadoop1.x.x的话是不一样的(Hadoop1.x.x跟目录下的jar包和lib目录下的jar包) 
 * 在hadoop-2.7.1\share\hadoop\common 下导入以下包：
 *  --hadoop-common-2.7.1.jar --hadoop-common-2.7.1-tests.jar
 * 	--hadoop-nfs-2.7.1.jar 
 * 在hadoop-2.7.1\share\hadoop\common\lib 下导入所有的包(63个) 
 * 	--... 
 * 在hadoop-2.7.1\share\hadoop\hdfs 下导入以下包
 *  --hadoop-hdfs-2.7.1.jar
 * 	--hadoop-hdfs-2.7.1-tests.jar
 *  --hadoop-hdfs-nfs-2.7.1.jar 
 * )
 * @author HuangYongFeng
 */
public class MyHDFS
{
	public static void main(String[] args)
	{
		// 1、连接到hadoop2.7.1的HDFS
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.1.20:9000");// 如果不写就只能本地操作了 hdfs://master:9000(也可以这样写,不过要做主机名master映射)
		conf.set("hadoop.job.ugi", "root,root");// 如果不写系统将按照默认的用户进行操作
		FileSystem fs = null;
		FSDataInputStream in = null;
		FSDataOutputStream out = null;
		try
		{
			// 2、在HDFS中创建一个文件夹，并上传一个文件
			fs = FileSystem.get(conf);
			// 创建文件夹
			fs.mkdirs(new Path("hdfs://192.168.1.20:9000/test1"));
			// 上传文件到规定文件夹下 (第一个参数是本地运行的文件路径,如果在linux运行,那就要按linux系统的路径/usr/local/derby.log,第二个参数是把文件放在hadoop HDFS 的路径)
			fs.copyFromLocalFile(new Path("E:\\b.txt"), new Path("hdfs://192.168.1.20:9000/test1/b.txt"));

			// 3、打印testhdfs1和derby.log的元数据(写完后可以去hadoop服务器上用命令[root@master sbin]# hadoop fs -ls -R /]查看)
			FileStatus fst = fs.getFileStatus(new Path("hdfs://192.168.1.20:9000/test1"));// 获取hadoop的HDFS中的testhdfs1目录
			if (fst.isDir())
			{
				System.out.println("这是个目录");
			}
			System.out.println("目录路径：" + fst.getPath());
			System.out.println("目录长度：" + fst.getLen());
			System.out.println("目录修改日期：" + new Timestamp(fst.getModificationTime()).toString());
			System.out.println("上次目录访问日期：" + new Timestamp(fst.getAccessTime()).toString());
			System.out.println("目录备份数：" + fst.getReplication());
			System.out.println("目录块大小：" + fst.getBlockSize());
			System.out.println("目录所有者：" + fst.getOwner());
			System.out.println("目录所在分组：" + fst.getGroup());
			System.out.println("目录权限：" + fst.getPermission().toString());
			
			System.out.println("-------------------------------------------------");
			
			fst = fs.getFileStatus(new Path("hdfs://192.168.1.20:9000/test1/b.txt"));// 获取hadoop的HDFS中的testhdfs1目录的derby.log文件
			if (!fst.isDir())
			{
				System.out.println("这是个文件");
			}
			System.out.println("文件路径：" + fst.getPath());
			System.out.println("文件长度：" + fst.getLen());
			System.out.println("文件修改日期：" + new Timestamp(fst.getModificationTime()).toString());
			System.out.println("上次文件访问日期：" + new Timestamp(fst.getAccessTime()).toString());
			System.out.println("文件备份数：" + fst.getReplication());
			System.out.println("文件块大小：" + fst.getBlockSize());
			System.out.println("文件所有者：" + fst.getOwner());
			System.out.println("文件所在分组：" + fst.getGroup());
			System.out.println("文件权限：" + fst.getPermission().toString());

			//3.1写文件
			in = fs.open(new Path("hdfs://192.168.1.20:9000/test1/b.txt"));
			in.seek(0);
			out = fs.create(new Path("hdfs://192.168.1.20:9000/test1/b.txt"), new Progressable()
			{
				@Override
				public void progress()
				{
					System.out.println(".");
				}
			});
			IOUtils.copyBytes(in, out, 4096, true);

			// 4、删除目录
//			fs.delete(new Path("hdfs://192.168.1.20:9000/testhdfs1"));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				IOUtils.closeStream(in);
				IOUtils.closeStream(out);
				fs.close();
			}
			catch (IOException e1)
			{
				fs = null;
				e1.printStackTrace();
			}
		}
	}
}
