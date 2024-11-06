import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WeeklyAvgMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text dayOfWeek = new Text();
    private SimpleDateFormat sdfInput = new SimpleDateFormat("yyyyMMdd");
    private SimpleDateFormat sdfOutput = new SimpleDateFormat("EEEE"); // 输出星期几

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        if (fields.length == 2) {
            String dateStr = fields[0];
            String[] amounts = fields[1].split(",");
            
            if (amounts.length == 2) {
                try {
                    Date date = sdfInput.parse(dateStr);
                    dayOfWeek.set(sdfOutput.format(date)); // 获取星期几
                    context.write(dayOfWeek, new Text(amounts[0] + "," + amounts[1]));
                } catch (Exception e) {
                    System.err.println("Invalid date format: " + dateStr);
                }
            }
        }
    }
}