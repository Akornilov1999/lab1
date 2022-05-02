package bdtc.lab1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import java.io.IOException;

public class HW1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
        Counter counter;
        if (context.getCounter(CounterType.Node1CPU).getName().equals("Node1CPU")) {
            counter = context.getCounter(CounterType.Node1CPU);
        }
        else if (context.getCounter(CounterType.Node2RAMmb).getName().equals("Node2RAMmb")) {
            counter = context.getCounter(CounterType.Node2RAMmb);
        }
        else {
            counter = context.getCounter(CounterType.Node3);
        }
        int sum = 0;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
            counter.increment(1);
        }
        context.write(key, new IntWritable(sum/(int)counter.getValue()));
        counter.setValue(0);
    }
}