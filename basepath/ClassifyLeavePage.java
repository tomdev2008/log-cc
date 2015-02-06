package basepath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
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

import basetool.ReadFromMongo;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * 基于路径数据， 识别跳离页（source,land……leave(多个)……leave） 集合 leave
 * 
 * @author hadoop
 */

public class ClassifyLeavePage {
	public static String[][] REGEX; // 用来存储从mongodb中读取的正则表

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {

		private IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// 跳离页数组
			ArrayList<String> leavePageList = new ArrayList<String>();

			// 非已知分组ID
			String leaveID = "0";

			// 点击流路径拆分成 key urls , line[0] = key
			String[] line = value.toString().split("\t");
			// line[1]是url集合
			String[] urlArray = line[1].toString().split("~");

			// 尾部跳离页
			leavePageList.add(urlArray[urlArray.length - 1]);

			// 寻找中间的跳离页，路径如果出现回溯，那么前一个页面就被认为是跳离页
			HashSet<String> urlSet = new HashSet<String>();
			int gap = 1;
			for (int i = 0; i < urlArray.length;) {
				urlSet.add(urlArray[i]);
				if (urlSet.size() - i == gap) {
					i++;
				} else {
					leavePageList.add(urlArray[i - 1]);
					gap--;
					i++;
				}
			}

			for (int i = 0; i < leavePageList.size(); i++) { // 跳离页数组
				for (int j = 0; j < REGEX.length; j++) {
					Matcher m = Pattern.compile(REGEX[j][0]).matcher(
							leavePageList.get(i));
					while (m.find()) {
						leaveID = REGEX[j][1];
						break;
					}
				}
			}

			context.write(new Text(leaveID), one); // ID ,1

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
			b.put("leaveID", key.toString());
			b.put("sum", sum);
			context.write(new ObjectId(), b);

		}// ~reduce
	}// ~Reduce

	@SuppressWarnings("deprecation")
	public static int run(String input, String output, String[][] regexTable)
			throws IOException, ClassNotFoundException, InterruptedException {

		REGEX = regexTable; // 传入规则表

		Configuration conf = new Configuration();
		MongoConfigUtil.setOutputURI(conf, output);

		final Job job = new Job(conf, "ClassifyLeavePage");
		FileInputFormat.addInputPath(job, new Path(input));

		job.setJarByClass(ClassifyLeavePage.class);
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

		// 获得正则表
		String[][] regexTable = ReadFromMongo.regexFromMongo("regexDB",
				"level3");

		String input = "hdfs://master1:9000/jout/ClickPath";

		String output = "mongodb://localhost:27017/jiaodian.leave";

		int exitCode = ClassifyLeavePage.run(input, output, regexTable);
	
		if (exitCode == 0) {
			System.out.println("Done!");
		} else {
			System.out.println("Failure!");
		}

		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");
	}
}//