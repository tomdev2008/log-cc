package basepath;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

/**
 * 
 * @author hadoop
 * 
 */
// 基于 点击流路径数据(会话数据) 计算整站
// 主要是判断 路径第一个URL的类别
// 存入集合 jump
// 站外进来的会话 jumpIn
// 直接进来的会话（-） directAccess
// mic本地会话 mic
// 产生寻盘会话 inquiry
// 没有产生寻盘会话(算是跳出) noInquiry
// 总的会话 countAll
//
// 整站 站外跳入率  jumpIn/countAll
// 无交互跳出率    noinquiry/countAll

public class JumpCount {
	public static class Map extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// 点击流路径拆分成 key urls , line[0] = key,line[1]是urls
			String[] line = value.toString().split("\t");

			// 判断 会话中是否有交互行为(询盘),如果没有交互，該会话就算 跳出out
			String regexInquiry = "made-in-china.com(/sendInquiry/|/inquiry.do|/inquirybasket|/inquiry-basket|/inquiryResult.do)";
			Matcher m = Pattern.compile(regexInquiry).matcher(line[1]);
			if (m.find()) {
				context.write(new Text("inquiry"), one);
			} else {
				context.write(new Text("noInquiry"), one);
			}

			// line[1]是url集合
			String[] urlArray = line[1].toString().split("~");

			// 判断sourcePage
			if (urlArray[0].equals("-")) { // 直接访问
				context.write(new Text("directAccess"), one);
			} else if (urlArray[0].contains(".made-in-china.com")) { // 站内
				context.write(new Text("mic"), one);
			} else { // 站外
				context.write(new Text("jumpIn"), one);
			}

			// 统计 会话数目
			context.write(new Text("countAll"), one);

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
			BSONObject b = new BasicBSONObject();
			b.put("type", key.toString());
			b.put("sum", sum);
			context.write(new ObjectId(), b);

		}// reduce
	}// Reduce

	public static int run(String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		MongoConfigUtil.setOutputURI(conf, output);

		@SuppressWarnings("deprecation")
		final Job job = new Job(conf, "JumpCount");
		FileInputFormat.addInputPath(job, new Path(input));

		job.setJarByClass(JumpCount.class);
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

	public static void main(String[] args) throws Exception {

		long startTime = System.currentTimeMillis();
		String input = "hdfs://master1:9000/alog/ClickPath";
		String output = "mongodb://localhost:27017/jiaodian.jump";

		int exitCode = JumpCount.run(input, output);
		if (exitCode == 0) {
			System.out.println("Done!");
		} else {
			System.out.println("Failure!");
		}
		
		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");

	}
}// 