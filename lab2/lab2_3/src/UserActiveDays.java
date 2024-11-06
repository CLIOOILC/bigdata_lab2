import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class UserActiveDays {

    public static class ActiveDaysMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text userId = new Text();
        private final static IntWritable activeDay = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // 跳过标题行
            if (fields[0].equalsIgnoreCase("user_id")) {
                return;
            }

            if (fields.length > 8) {
                String id = fields[0];
                double directPurchaseAmt;
                double totalRedeemAmt;

                try {
                    directPurchaseAmt = Double.parseDouble(fields[7]);
                    totalRedeemAmt = Double.parseDouble(fields[9]);
                } catch (NumberFormatException e) {
                    return;
                }

                if (directPurchaseAmt > 0 || totalRedeemAmt > 0) {
                    userId.set(id);
                    context.write(userId, activeDay);
                }
            }
        }
    }

    public static class ActiveDaysReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // 自定义的 Mapper 输出，以便在排序时使用活跃天数进行排序
    public static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable activeDays = new IntWritable();
        private Text userId = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length == 2) {
                userId.set(fields[0]);
                activeDays.set(Integer.parseInt(fields[1]));
                context.write(activeDays, userId);
            }
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text userId : values) {
                context.write(userId, key);
            }
        }
    }

    // 自定义 Comparator 用于降序排序
    public static class DescendingIntComparator extends WritableComparator {
        protected DescendingIntComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -1 * a.compareTo(b); // 倒序排序
        }
    }

    public static void main(String[] args) throws Exception {
        // 第一个 Job: 计算每个用户的活跃天数
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "user active days count");
        job1.setJarByClass(UserActiveDays.class);
        job1.setMapperClass(ActiveDaysMapper.class);
        job1.setCombinerClass(ActiveDaysReducer.class);
        job1.setReducerClass(ActiveDaysReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path tempOutput = new Path("temp_output");
        FileOutputFormat.setOutputPath(job1, tempOutput);

        job1.waitForCompletion(true);

        // 第二个 Job: 根据活跃天数降序排序
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "sort by active days");
        job2.setJarByClass(UserActiveDays.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setSortComparatorClass(DescendingIntComparator.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, tempOutput);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
