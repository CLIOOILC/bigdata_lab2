import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WeeklyAvgReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long totalInflow = 0;
        long totalOutflow = 0;
        int count = 0;

        for (Text value : values) {
            String[] amounts = value.toString().split(",");
            if (amounts.length == 2) {
                totalInflow += Long.parseLong(amounts[0]);
                totalOutflow += Long.parseLong(amounts[1]);
                count++;
            }
        }

        long avgInflow = count == 0 ? 0 : totalInflow / count;
        long avgOutflow = count == 0 ? 0 : totalOutflow / count;
        context.write(key, new Text(avgInflow + "," + avgOutflow));
    }
}