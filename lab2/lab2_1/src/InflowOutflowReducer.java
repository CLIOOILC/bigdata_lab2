import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InflowOutflowReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, Long> purchaseMap = new HashMap<>();
    private Map<String, Long> redeemMap = new HashMap<>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] parts = key.toString().split("_");
        String date = parts[0];
        String type = parts[1];
        
        long sum = 0;
        for (Text val : values) {
            try {
                sum += Long.parseLong(val.toString());
            } catch (NumberFormatException e) {
                System.err.println("Invalid number format: " + val);
            }
        }

        if (type.equals("purchase")) {
            purchaseMap.put(date, sum);
        } else if (type.equals("redeem")) {
            redeemMap.put(date, sum);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String date : purchaseMap.keySet()) {
            long purchaseAmt = purchaseMap.getOrDefault(date, 0L);
            long redeemAmt = redeemMap.getOrDefault(date, 0L);
            context.write(new Text(date), new Text(purchaseAmt + "," + redeemAmt));
        }
    }
}