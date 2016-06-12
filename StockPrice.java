package Demo;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//LongWritable相当于java的Long，Text相当于java的String，IntWritabe相当于java的Integer
//map函数有四个形参类型，分别制定输入函数的输入key，输入value；输出key，输出value
//context实例用于数据的写入
//hadoop一行一行读文件
public class StockPrice {

		public static class StockMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

			public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

				String record = value.toString();
				String[] parts = record.split(",");
				context.write(new Text(parts[0]), new IntWritable(Integer.parseInt(parts[1])));
			}
		}
//reducer函数的输入是map函数的输出；
		public static class StockReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

			public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

				int maxValue = Integer.MIN_VALUE;
				int sum = 0;
				int avg = 0, count = 0;
				//Looping and calculating Max for each year
				for (IntWritable val : values) {
					maxValue = Math.max(maxValue, val.get());
					//sum += val.get();
					//count++;
				}
				//avg = sum / count;
				context.write(key, new IntWritable(maxValue));
			}
		}

		public static void main(String[] args) throws Exception {
			// for running on cluster
			//Configuration conf = new Configuration();
			//Job job = Job.getInstance(conf, "Stock Price");
			
			/*args[0] = "/home/cloudera/workspace/Stock/stock";
			args[1] = "/home/cloudera/workspace/Stock/output/output1.txt";
            指定输入和输出数据的路径 FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job,new Path(args[1]));*/
			
			
			Job job = new Job();//job对象指定作业执行规范
			job.setJobName("StockPrice");
			
			job.setJarByClass(StockPrice.class); //hadoop利用StockPrice.class这个类来查找包含它的jar文件，进而找到要使用的jar文件，jar文件的名字并没有什么鸟用

			job.setMapOutputKeyClass(Text.class);//map函数的输出类型，key的类型是Text，value的类型是IntWritabe；
			job.setMapOutputValueClass(IntWritable.class);

			job.setOutputKeyClass(Text.class);//reduce函数的输出类型，key的类型是Text，value的类型是IntWritabe；
			job.setOutputValueClass(IntWritable.class);

			job.setMapperClass(StockMapper.class);//map和reduce类型;
			job.setReducerClass(StockReducer.class);

			job.setInputFormatClass(TextInputFormat.class);//输入的类型是TextInputFormat;
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));//input output 路径
			FileOutputFormat.setOutputPath(job,new Path(args[1]));

			job.waitForCompletion(true);//表示执行的成败;

		}
}
