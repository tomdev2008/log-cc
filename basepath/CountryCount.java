package basepath;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

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

import com.mongodb.DBObject;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.util.JSON;

/***
 * 基于会话数据
 * 
 * country: sum: 多少次会话访问
 * 
 * @author hadoop
 * 
 */
public class CountryCount {
	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		private static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// 点击流路径拆分成 key urls , line[0] = key
			String[] line = value.toString().split("\t");
			// 1.0.181.143~THAILAND~2014-08-10 15:18:25
			String[] str = line[0].split("~");
			// // 如果国家是-，
			// if (str[1].equals("-")) {
			// str[1] = getGeoInfo(str[0]).toUpperCase(); // 用ip得到国家
			// }
			context.write(new Text(str[1]), one);

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
			// KOREA, REPUBLIC OF >>>>> Korea, Repulic Of
			char[] charCountry = key.toString().toLowerCase().toCharArray();
			charCountry[0] = Character.toUpperCase(charCountry[0]);
			for (int i = 1; i < charCountry.length; i++) {
				if (charCountry[i - 1] == 32) {
					charCountry[i] = Character.toUpperCase(charCountry[i]);
				}
			}
			String country = new String(charCountry);
			BSONObject b = new BasicBSONObject();
			b.put("country", country);
			b.put("sum", sum);
			context.write(new ObjectId(), b);
		}
	}

	public static int run(String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		MongoConfigUtil.setOutputURI(conf, output);

		@SuppressWarnings("deprecation")
		final Job job = new Job(conf, "CountryCount");
		FileInputFormat.addInputPath(job, new Path(input));

		job.setJarByClass(CountryCount.class);
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
		String output = "mongodb://localhost:27017/jiaodian.country";
		
		int exitCode = CountryCount.run(input, output);
		if (exitCode == 0) {
			System.out.println("Done!");
		} else {
			System.out.println("Failure!");
		}
		long endTime = System.currentTimeMillis();
		System.out.print("costTime:" + (endTime - startTime) / 1000 + "s");

	}

	// 根据ip获得国家,需要联网
	public static String getGeoInfo(String ip) {
		URL url = null;
		DBObject object = null;
		try {
			url = new URL("http://www.telize.com/geoip/" + ip);
			InputStream is = url.openStream();
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			int retVal;
			while ((retVal = is.read()) != -1) {
				os.write(retVal);
			}
			String output = os.toString();
			object = (DBObject) JSON.parse(output);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return (String) object.get("country");
	}

}
