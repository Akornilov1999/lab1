import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

public class MapReduceTest {

    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable( 10), new Text("1, 1510670916247, 10" ))
                .withInput(new LongWritable( 50), new Text("1, 1510670916249, 50" ))
                .withInput(new LongWritable( 50), new Text("1, 1510670916251, 50" ))
                .withInput(new LongWritable(200), new Text("2, 1510670916253, 200"))
                .withInput(new LongWritable( 10), new Text("1, 1510670916255, 10" ))
                .withInput(new LongWritable(  5), new Text("1, 1510670955155, 5"  ))
                .withInput(new LongWritable( 10), new Text("1, 1510670955155, 10" ))
                .withInput(new LongWritable( 15), new Text("1, 1510670955155, 15" ))
                .withOutput(new Text("Node1CPU, 1510670880000"  ), new IntWritable( 30))
                .withOutput(new Text("Node1CPU, 1510670940000"  ), new IntWritable( 10))
                .withOutput(new Text("Node2RAMmb, 1510670880000"), new IntWritable(200))
                .runTest();
    }
}