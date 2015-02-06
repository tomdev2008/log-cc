package basepath;

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

/**
 * 
 * @author hadoop
 */
// 基于路径数据，计算会话数目，和会话开始时间(startTime)
// 统计每个小时的会话数目
// date:2014-08-10
// hour:17
// num: 数目

public class SessionCount {

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {

		private IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// 点击流路径拆分成 key urls , line[0] = key
			String[] line = value.toString().split("\t");
			// 1.0.181.143~THAILAND~2014-08-10 15:18:25
			String[] keySplit = line[0].split("~");

			// 对时间 2014-08-10 01:06:44 拆分为 2014-08-10 01
			String[] timeSplit = keySplit[2].split(":");

			context.write(new Text(timeSplit[0]), one);

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
			// 将key2014-08-10 01拆分为日期和小时
			String[] timeSplit = key.toString().split(" ");

			BSONObject b = new BasicBSONObject();
			b.put("date", timeSplit[0]);
			b.put("hour", timeSplit[1]);
			b.put("sum", sum);
			context.write(new ObjectId(), b);

		}// reduce
	}// Reduce

	public static int run(String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		MongoConfigUtil.setOutputURI(conf, output);

		@SuppressWarnings("deprecation")
		final Job job = new Job(conf, "SessionCount");
		job.setJarByClass(SessionCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(ObjectId.class);
		job.setOutputValueClass(BSONObject.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		String input = "hdfs://master1:9000/alog/ClickPath";
		String output = "mongodb://localhost:27017/jiaodian.session";
		
		int exitCode = SessionCount.run(input, output);
		if (exitCode == 0) {
			System.out.println("Done!");
		} else {
			System.out.println("Failure!");
		}
		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");
	}
}