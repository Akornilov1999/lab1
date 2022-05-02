import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text("1, 1510670916247, 10" ))
                .withInput(new LongWritable(), new Text("1, 1510670916249, 50" ))
                .withInput(new LongWritable(), new Text("1, 1510670916251, 50" ))
                .withInput(new LongWritable(), new Text("2, 1510670916253, 200"))
                .withInput(new LongWritable(), new Text("1, 1510670916255, 10" ))
                .withInput(new LongWritable(), new Text("1, 1510670955155, 5"  ))
                .withInput(new LongWritable(), new Text("1, 1510670955155, 10" ))
                .withInput(new LongWritable(), new Text("1, 1510670955155, 15" ))
                .withOutput(new Text("Node1CPU, 1510670880000"  ), new IntWritable( 10))
                .withOutput(new Text("Node1CPU, 1510670880000"  ), new IntWritable( 50))
                .withOutput(new Text("Node1CPU, 1510670880000"  ), new IntWritable( 50))
                .withOutput(new Text("Node2RAMmb, 1510670880000"), new IntWritable(200))
                .withOutput(new Text("Node1CPU, 1510670880000"  ), new IntWritable( 10))
                .withOutput(new Text("Node1CPU, 1510670940000"  ), new IntWritable(  5))
                .withOutput(new Text("Node1CPU, 1510670940000"  ), new IntWritable( 10))
                .withOutput(new Text("Node1CPU, 1510670940000"  ), new IntWritable( 15))
                .runTest();
    }
}