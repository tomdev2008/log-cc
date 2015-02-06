package baselog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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
 * 点击流路径（会话）：ip~国家~2014-08-10 15:18:25 \t url1~url2~
 * 
 * @author hadoop
 */

public class ClickPath {

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
			// 存储会话开始时间
			String startTime = new String();

			ArrayList<String> tmpEdgeList = new ArrayList<String>();
			// 将一个key的所用路段 都存储起来，接下来按时间排序
			for (Text text : values) {
				tmpEdgeList.add(text.toString());
			}
			Object[] sortedEdgeList = tmpEdgeList.toArray();
			Arrays.sort(sortedEdgeList);

			// 构造点击流pathList
			ArrayList<String> pathList = new ArrayList<String>();
			// 第一个为path的第一部分,t~a~b >>t a b
			String[] edgeSplit = ((String) sortedEdgeList[0]).split("~");
			startTime = edgeSplit[0]; // 只需要赋值一次(第一次)
			pathList.add(edgeSplit[1]);
			pathList.add(edgeSplit[2]);
			for (int i = 1; i < sortedEdgeList.length; i++) {
				String[] tmpEdgeSplit = ((String) sortedEdgeList[i]).split("~");
				// a,b-- b,c >> a,b,c
				if (tmpEdgeSplit[1].equals(pathList.get(pathList.size() - 1))) {
					pathList.add(tmpEdgeSplit[2]);
				} else {
					// a,b,c-- b,f >> a,b,c,b,f
					pathList.add(tmpEdgeSplit[1]);
					pathList.add(tmpEdgeSplit[2]);
				}
			}

			// pathList存放的是a,b,c,b,f，g,h,a,m
			// pathList >>path
			String path = new String("");
			for (int i = 0; i < pathList.size(); i++) {
				path += pathList.get(i) + "~";
			}

			// 输出key \t a~b~c~b~f~g~h~a~m~
			// 在key中加上会话开始时间,用于接下来统计Session
			context.write(new Text(key.toString() + "~" + startTime), new Text(
					path));

		}// reduce
	}// Reduce

	@SuppressWarnings("deprecation")
	public static int run(String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "ClickPath");
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setJarByClass(ClickPath.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		return job.waitForCompletion(true) ? 0 : 1 ;

	}

	public static void main(String[] args) throws Exception {

		long startTime = System.currentTimeMillis();

//		if (args.length != 2) {
//			System.err.println("Usage: wordcount <in> <out>");
//			System.exit(2);
//		}
		
		String input = "hdfs://master1:9000/jlog/";
		String output = "hdfs://master1:9000/alog/ClickPath";

		int exitCode = ClickPath.run(input, output);
		if (exitCode == 0) {
			System.out.println("Done!");
		}else{
			System.out.println("Failure!");
		}

		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");

	}

}// 