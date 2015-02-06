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
 * @author hadoop
 */
// 这里的正则表写死，数据量太小，不需要另存为一张表

// 基于点击流路径数据,
// 识别来源页 URLArray[0]
// 集合source
// source:15个搜索引擎/unknown/mic
// type： 0 /1 /2 (用来表示三大类)
// sum:

public class ClassifySourcePage {

	public static String[] SearchEngine = { "google", "so.360.cn", "sogou.com",
			"sousou.com", "baidu.com", "bing.com", "yahoo.com", "yandex.com",
			"ask.com", "SearchEngine.tb.ask.com", "webcrawler.com",
			"us.wow.com", "aol.com", "go.mail.ru", "daum.net" };

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		private static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String url = null;
			String source = "unknown"; // 默认属于未知来源

			// 点击流路径拆分成 key urls , line[0] = key
			String[] line = value.toString().split("\t");

			String[] urlArray = line[1].toString().split("~");

			// 来源页
			url = urlArray[0];
			if ((url.equals("-") || url.contains(".made-in-china.com"))) {
				source = "mic";
			} else {
				// 属于已知搜索引擎
				for (int i = 0; i < SearchEngine.length; i++) {
					if (url.contains(SearchEngine[i])) {
						source = SearchEngine[i];
					}
				}

			}
			context.write(new Text(source), one);
		}// map
	}// Map

	public static class Reduce extends
			Reducer<Text, IntWritable, ObjectId, BSONObject> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int type = 3; // 0,1,2
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			// 用type标识三大类，方便取数据
			for (int i = 0; i < SearchEngine.length; i++) {
				if (key.toString().contains(SearchEngine[i])) {
					type = 0;
				}
			}
			if (key.toString().equals("unknown")) {
				type = 1;
			}
			if (key.toString().equals("mic")) {
				type = 2;
			}

			BSONObject b = new BasicBSONObject();
			b.put("source", key.toString());
			b.put("type", type);
			b.put("sum", sum);
			context.write(new ObjectId(), b);
		}
	}

	public static int run(String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();

		MongoConfigUtil.setOutputURI(conf, output);
		// 初始化job
		@SuppressWarnings("deprecation")
		final Job job = new Job(conf, "ClassifySourcePage");
		job.setJarByClass(ClassifySourcePage.class);
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
		String output = "mongodb://localhost:27017/jiaodian.source";

		int exitCode = ClassifySourcePage.run(input, output);

		if (exitCode == 0) {
			System.out.println("Done!");
		} else {
			System.out.println("Failure!");
		}
		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");
	}
}