import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage\ttoPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability

            String[] fromTos = value.toString().trim().split("\t");

            if (fromTos.length < 2){
                return;
            }

            String[] tos = fromTos[1].trim().split(",");
            String outputKey = fromTos[0];
            for (String to: tos){
                // outputKey: fromId
                // outputValue: toId=(1/num of tos)
                context.write(new Text(outputKey), new Text( to + "=" + (double) 1/tos.length ));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\tPageRank0
            //target: write to reducer
            String[] prInit = value.toString().trim().split("\t");
            //outputKey: websideId
            //outputValue: PR0 set to 1
            context.write(new Text(prInit[0]), new Text(prInit[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // the input combined the value of two mappers
            // inputKey   <fromPageId,     fromPageId,    .. , PageId>
            // inputValue <toPageId1=prob, toPageId2=prob,.. , fromPagePR0>

            // outputKey toPageId
            // outputValye: cellsubPR

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication

            // for same key, there can be multiple input values, toPageId1=prob, toPageId2=prob, .. fromPagePR0
            // thus the value is defined as iterable
            List<String> transitionCells = new ArrayList<String>();
            double prCell = 0.0; // for same key, the prCell is always the same

            for (Text value: values){
                if (value.toString().contains("=")){
                    transitionCells.add(value.toString());
                }
                else{
                    prCell = Double.parseDouble(value.toString());
                }
            }
            for (String cell: transitionCells){
                String toId = cell.split("=")[0];
                double probTM = Double.parseDouble(cell.split("=")[1]);
                double subPr = prCell*probTM;
                context.write(new Text(toId), new Text(String.valueOf(subPr)));
            }

        }
    }

    public static void main(String[] args) throws Exception {
        // setup configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // assign the java class to the job,
        // help hadoop to find out that which jar it should send to nodes to performance map and reduce
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?
        // the subclass already in this class, no need to write UnitMultiplication.PRMapper.class
        job.setMapperClass(TransitionMapper.class);
        job.setMapperClass(PRMapper.class);
        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // two mapper needs two path as inputs
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);
        // the output is not text but save to file in the folder defined by the path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
