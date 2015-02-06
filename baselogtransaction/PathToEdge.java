package baselogtransaction;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @author hadoop Top20 path 转换为 边数据
 * 把路径数据，转换成便数据，
 */

public class PathToEdge {

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {

		private static IntWritable count = new IntWritable();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] line = value.toString().split("\t");
			String[] arrs = line[1].toString().split("~");
			count.set(Integer.parseInt(line[0]));
			for (int i = 0; i < arrs.length - 1; i++) {
				context.write(new Text(arrs[i] + "," + arrs[i + 1]), count);
			}

		}// map
	}// Map

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(new Text(key.toString() + "," + sum), null);
		}// reduce
	}// Reduce

	// main
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration();

		String input = "hdfs://master1:9000/alog/Top20level3";
		String output = "hdfs://master1:9000/alog/Top20edge";

		// 初始化job
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "PathToEdge");
		job.setJarByClass(PathToEdge.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
		System.out.println("DONE!");
		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");

	}
}//