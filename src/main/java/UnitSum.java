import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


import java.io.IOException;
// import java.text.DecimalFormat;

public class UnitSum {

    public static class PassMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: toPage\t unitMultiplication, key + \t + value is the default format settings
            String[] toIdsubPr = value.toString().trim().split("\t");
            context.write(new Text(toIdsubPr[0]), new Text(toIdsubPr[1]));
        }
    }

    public static class PRpreMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // input: Id\t PR N-1
            String[] prPre = value.toString().trim().split("\t");
            context.write(new Text(prPre[0]), new Text( prPre[1] + "=" + "pre" ));

        }
    }

    public static class SumReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        double beta;

        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            // conf.get(String), did not have getDouble() why?
            beta = Double.parseDouble(conf.get("beta", "0.2"));
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = toPage value = <unitMultiplication(fromId1), unitMultiplication(fromId2) ... PR N-1 >
            double sum =0.0;
            for (Text value: values){

                if (value.toString().contains("=")){
                    String[] prPre = value.toString().split("=");
                    sum += beta * Double.parseDouble(prPre[0]);
                }
                else{
                    sum += (1-beta) * Double.parseDouble(value.toString());
                }


            }

            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("beta", args[2]);
        Job job = Job.getInstance(conf);

        job.setJarByClass(UnitSum.class);

        job.setMapperClass(PassMapper.class);
        job.setMapperClass(PRpreMapper.class);
        job.setReducerClass(SumReducer.class);

        // the mapper and reducer has different output format
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // unitState N-1, before summation
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PassMapper.class);
        // PR N-1 from previous iteration
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRpreMapper.class);

        /*
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        not the correct way to define input when you have multiplyInputs
        */
        // output PR N
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        job.waitForCompletion(true);
    }
}
