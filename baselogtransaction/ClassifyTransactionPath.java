package baselogtransaction;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import basetool.ReadFromMongo;

/**
 * 用户事务路径数据,利用正则表，把URL换成ID
 *
 * @author hadoop
 */
public class ClassifyTransactionPath {
	public static String[][] REGEX;

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// 查看是否有询盘行为

			
			
			
			// 针对一条路径 拆分
			String[] urlArray = value.toString().split("~");

			//长度太短,不要
			// if (arrs.length < 4) {
			// return;
			// }
			//
			// 根据正则表， 判断url所属ID
			for (int i = 0; i < urlArray.length; i++) {

				for (int j = 0; j < REGEX.length; j++) {

					Matcher m = Pattern.compile(REGEX[j][0]).matcher(
							urlArray[i]);
					while (m.find()) {
						urlArray[i] = Integer.parseInt(REGEX[j][1]) + "";
						break;// 只分一个
					}
				}
			}
			// 再拼接成路径
			String path = "";
			for (int i = 0; i < urlArray.length; i++) {
				Matcher m = Pattern.compile("\\d+").matcher(urlArray[i]);
				if (!m.matches()) {
					urlArray[i] = "null"; // 如果未知分类
				}
				path = path + urlArray[i] + "~";
			}
			context.write(new Text(path), one);

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
			context.write(new Text(sum + "\t" + key.toString()), null);

		}// reduce
	}// Reduce

	public static int run(String input, String output, String[][] regexTable)
			throws IOException, ClassNotFoundException, InterruptedException {

		REGEX = regexTable; // 传入规则表
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "ClassifyTransactionPath");
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setJarByClass(ClassifyTransactionPath.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		
		long startTime = System.currentTimeMillis();

		// 获得正则表
		String[][] regexTable = ReadFromMongo.regexFromMongo("regexDB",
				"level3");
		String input = "hdfs://master1:9000/alog/TransactionPath";
		String output = "hdfs://master1:9000/alog/level3";

		int exitCode = ClassifyTransactionPath.run(input, output, regexTable);
		if (exitCode == 0) {
			System.out.println("Done!");
		} else {
			System.out.println("Failure!");
		}

		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");

	}
}//

