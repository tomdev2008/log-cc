package baselogtransaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 从日志数据里， 得到用户事务路径数据（从会话中剥离）， 期间不对URL处理
 * 
 * 利用图的深度优先遍历DirectedGraphDFS
 * 
 * @author hadoop
 */
public class TransactionPath {

	public static class Map extends Mapper<Object, Text, Text, Text> {

		private Text keyOut = new Text();
		private Text valueOut = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// 对一行日志记录 拆分
			String[] logArray = value.toString().split("\t");
			// 对request,去掉GET等
			String[] requestStr = logArray[8].split(" ");
			// 限定条件
			if (logArray[41].equals("normal") && logArray[15].equals("-1")
					&& requestStr[0].equals("GET")
					&& logArray[10].equals("200")) {

				// key: ip~国家
				keyOut.set(logArray[1] + "~" + logArray[36]);

				// 去掉referer中的 http:// 和 https:// 和 HTTP：//
				String referer = logArray[9].replace("http://", "")
						.replace("https://", "").replace("HTTP://", "");

				// request(fourdomain+)
				String request = logArray[14] + "made-in-china.com"
						+ requestStr[1];

				// value：时间～referer～request
				valueOut.set(logArray[4] + "~" + referer + "~" + request);

				context.write(keyOut, valueOut);
			}
		}// map
	}// Map

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// 将一个key的所用路段 都存储起来，接下来按时间排序
			ArrayList<String> tmpEdgeList = new ArrayList<String>();
			for (Text text : values) {
				tmpEdgeList.add(text.toString());
			}
			Object[] sortedEdgeList = tmpEdgeList.toArray();
			Arrays.sort(sortedEdgeList);

			// 声明边表数组
			String[][] edgeArray = new String[sortedEdgeList.length][2];
			// set用于去重，之后用来最终生成vertex数组，
			HashSet<String> vertexSet = new HashSet<String>();
			for (int i = 0; i < sortedEdgeList.length; i++) {
				String[] tmpEdgeSplit = ((String) sortedEdgeList[i]).split("~");
				// 直接构造边表
				edgeArray[i][0] = tmpEdgeSplit[1]; // referer
				edgeArray[i][1] = tmpEdgeSplit[2]; // request
				// 利用set构造点表
				vertexSet.add(tmpEdgeSplit[1]);
				vertexSet.add(tmpEdgeSplit[2]);
			}

			// 构造点表，保证起点，其他点的顺序无所谓
			// 声明顶点表
			String[] vertexArray = new String[vertexSet.size()];
			vertexSet.remove(edgeArray[0][0]); // 删除set中的起点
			vertexArray[0] = edgeArray[0][0];// 存储起点
			// 其他点随便存
			Iterator<String> ihs = vertexSet.iterator();
			int j = 1;
			while (ihs.hasNext()) {
				vertexArray[j] = ihs.next(); //
				j++;
			}

			// 调用DirectedGraphDFS, 利用顶点表和边表,创建图
			DirectedGraphDFS pG = new DirectedGraphDFS(vertexArray, edgeArray);
			// 用于存储返回的事务
			ArrayList<String> TransactionL = new ArrayList<String>();
			TransactionL = pG.DFS();
			// 输出事务
			for (int m = 0; m < TransactionL.size(); m++) {
				context.write(new Text(TransactionL.get(m)), null);
			}
		}// reduce
	}// Reduce

	public static int run(String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		// 初始化job
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "TransactionPath");
		// 设置处理类
		job.setJarByClass(TransactionPath.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		long startTime = System.currentTimeMillis();
		String input = "hdfs://master1:9000/jlog/";
		String output = "hdfs://master1:9000/alog/TransactionPath";

		int exitCode = TransactionPath.run(input, output);
		if (exitCode == 0) {
			System.out.println("Done!");
		} else {
			System.out.println("Failure!");
		}

		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");
	}
}//

