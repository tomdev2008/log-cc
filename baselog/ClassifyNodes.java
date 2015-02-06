package baselog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import basetool.ReadFromMongo;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
//node

/**
 * @author hadoop
 * 
 */
// 基于 log文件
// 集合 nodes
// url : inDegree: outDegree: level1：leavel2：level3：
public class ClassifyNodes {

 // 用来存储从mongodb中读取的正则表
	public static String[][] REGEX;

	public static class Map extends Mapper<Object, Text, Text, Text> {

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

				// 去掉referer中的 http:// 和 https:// 和 HTTP：//
				String referer = logArray[9].replace("http://", "")
						.replace("https://", "").replace("HTTP://", "");

				String request = logArray[14] + "made-in-china.com"
						+ requestStr[1];
				// URL中的？
				// referer，request的出度入度统计
				context.write(new Text(referer), new Text("O"));
				context.write(new Text(request), new Text("I"));
			}
		}// map
	}// Map

	// reduce
	public static class Reduce extends
			Reducer<Text, Text, ObjectId, BSONObject> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String url = key.toString();

			// 一个URL属于多个id的情况
			ArrayList<Integer> level3 = new ArrayList<Integer>();

			int sumInDegree = 0;
			int sumOutDegree = 0;

			for (Text val : values) {
				if (val.toString().equals("I")) {
					sumInDegree++;
				} else if (val.toString().equals("O")) {
					sumOutDegree++;
				}
			}

			// 根据正则表， 判断url所属level
			
			for (int j = 0; j < REGEX.length; j++) {
				Matcher m = Pattern.compile(REGEX[j][0]).matcher(url);
				while (m.find()) {
					level3.add(Integer.parseInt(REGEX[j][1]));
					// break;
				}
			}

			BSONObject b = new BasicBSONObject();
			b.put("url", url);
			b.put("inDegree", sumInDegree);
			b.put("outDegree", sumOutDegree);		
			b.put("level3", level3);
			context.write(new ObjectId(), b);

		}
	}

	public static int run(String input, String output,String[][] regexTable) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		REGEX=regexTable;   //传入规则表
		Configuration conf = new Configuration();
		MongoConfigUtil.setOutputURI(conf, output);

		@SuppressWarnings("deprecation")
		final Job job = new Job(conf, "ClassifyNode");
		FileInputFormat.addInputPath(job, new Path(input));
		
		job.setJarByClass(ClassifyNodes.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(ObjectId.class);
		job.setOutputValueClass(BSONObject.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		//正则表 regex,ID 
		String[][] regexTable = ReadFromMongo.regexFromMongo("regexDB", "level3");

		String input = "hdfs://master1:9000/jlog/";
		String output = "mongodb://localhost:27017/jiaodian.nodes";
		
		int exitCode = ClassifyNodes.run(input,output,regexTable);	
		if (exitCode == 0) {
			System.out.println("Done!");
		}else{
			System.out.println("Failure!");
		}
	
		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");
	}// main
}//
