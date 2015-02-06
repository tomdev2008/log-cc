package baselog;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

/***
 * @author hadoop
 * 
 */
// hdfs >>> mongodb
// links
// referer: request: sum:
public class Links {

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {

		private static IntWritable one = new IntWritable(1);

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

				// 边的统计
				context.write(new Text(referer + "~" + request), one);
			}

		}// map
	}// Map

	public static class Reduce extends
			Reducer<Text, IntWritable, ObjectId, BSONObject> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			String[] url = key.toString().split("~");

			BSONObject b = new BasicBSONObject();
			b.put("referer", url[0]);
			b.put("request", url[1]);
			b.put("sum", sum);
			context.write(new ObjectId(), b);

		}
	}

	public static int run(String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		MongoConfigUtil.setOutputURI(conf, output);
		@SuppressWarnings("deprecation")
		final Job job = new Job(conf, "Links");
		FileInputFormat.addInputPath(job, new Path(input));
		
		job.setJarByClass(Links.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(ObjectId.class);
		job.setOutputValueClass(BSONObject.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	// main
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		String input = "hdfs://master1:9000/jlog/";
		String output = "mongodb://localhost:27017/jiaodian.links";
		
		int exitCode = Links.run(input,output);	
		if (exitCode == 0) {
			System.out.println("Done!");
		}else{
			System.out.println("Failure!");
		}
	
		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");
	}//
}//
