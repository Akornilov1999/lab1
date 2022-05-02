package bdtc.lab1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;

public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable();                // Value for <K,V> output mapper
    private Text word = new Text();                                          // Key for <K,V> output mapper
    private HashMap<String, String> idsMap = new HashMap<String, String>() { // Constant Map from id
                                                                             // to humanReadableName
        {
        put("1", "Node1CPU"  );
        put("2", "Node2RAMmb");
        put("3", "Node3"     );
        }
    };

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(", ");            // from value into tokens
        one.set(Integer.parseInt(tokens[2]));                            // set value into IntWritable container
        Calendar calendar = Calendar.getInstance();                      // parse date into calendar without seconds
                                                                         // and milliseconds
        calendar.setTime(new java.util.Date(Long.parseLong(tokens[1]))); // from timestamp value to date
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        word.set(idsMap.get(tokens[0])+", "+calendar.getTimeInMillis()); // make key value from id humanReadableName
                                                                         // and timestamp without seconds
        context.write(word, one);
    }
}