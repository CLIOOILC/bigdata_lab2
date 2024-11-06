import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InterestAnalysis {

    public static class InterestMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text interestRange = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 3) { // 假设Interest_1_W在第3列
                try {
                    double interest = Double.parseDouble(fields[3]); // 获取一周利率
                    double transactionAmount = Double.parseDouble(fields[4]); // 假设交易金额在第4列

                    // 按照利率将数据分区
                    if (interest < 1.0) {
                        interestRange.set("0-1%");
                    } else if (interest < 2.0) {
                        interestRange.set("1-2%");
                    } else if (interest < 3.0) {
                        interestRange.set("2-3%");
                    } else {
                        interestRange.set("3%以上");
                    }
                    context.write(interestRange, new DoubleWritable(transactionAmount));
                } catch (NumberFormatException e) {
                    // 忽略格式错误的行
                }
            }
        }
    }

    public static class InterestReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double average = sum / count;
            context.write(key, new DoubleWritable(average));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Interest Analysis");

        job.setJarByClass(InterestAnalysis.class);
        job.setMapperClass(InterestMapper.class);
        job.setCombinerClass(InterestReducer.class);
        job.setReducerClass(InterestReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}