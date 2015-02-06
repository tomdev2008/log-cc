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
 * 从日志数据里 得到关键词，统计词频
 * 
 * @author hadoop
 * 
 */
public class Keywords {

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

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

				// 17 search_word referer中查询的关键词
				// 21 search_word_new request中查询的关键词
				// 如果是-1表示没有查询关键词
				if (!(logArray[17].equals("-1"))) {
					context.write(new Text(logArray[17]), one);
				}
				if (!(logArray[21].equals("-1"))) {
					context.write(new Text(logArray[21]), one);
				}

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

			// 查询关键词中有很多其他字符，可以进一步处理，
			BSONObject b = new BasicBSONObject();
			b.put("keywords", key.toString());
			b.put("sum", sum);

			context.write(new ObjectId(), b);
		}
	}


	public static int run(String input,String output) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		MongoConfigUtil.setOutputURI(conf,output);  //必须放在job之前	
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Keywords");
		FileInputFormat.addInputPath(job, new Path(input));

		
		job.setJarByClass(Keywords.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(ObjectId.class);
		job.setOutputValueClass(BSONObject.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

	
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	// main
	public static void main(String[] args) throws Exception {
		
		long startTime = System.currentTimeMillis();
		
		String input = "hdfs://master1:9000/jlog/";
		String output = "mongodb://localhost:27017/jiaodian.keywords";


		int exitCode = Keywords.run(input,output);	
		if (exitCode == 0) {
			System.out.println("Done!");
		}else{
			System.out.println("Failure!");
		}

		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");
	}

}// 