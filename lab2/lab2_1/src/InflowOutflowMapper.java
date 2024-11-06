import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class InflowOutflowMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        
        if (fields.length >= 9) {
            String date = fields[1]; // report_date
            String totalPurchaseAmt = fields[3].isEmpty() ? "0" : fields[3]; // total_purchase_amt
            String totalRedeemAmt = fields[8].isEmpty() ? "0" : fields[8]; // total_redeem_amt
            
            context.write(new Text(date + "_purchase"), new Text(totalPurchaseAmt));
            context.write(new Text(date + "_redeem"), new Text(totalRedeemAmt));
        }
    }
}