package summaries;

/**
 * Created by Toks on 27/12/2016.
 */

//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("average");
        job.setJarByClass(Main.class);

        job.setMapOutputValueClass(NumPair.class); //added to accommodate changes with new NumPair and Combine classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Combine.class);  //added for the combiner

        Path inputFilePath = new Path("/Users/Toks/IdeaProjects/applying_mapreduce/data/input/census.txt");
        Path outputFilePath = new Path("/Users/Toks/IdeaProjects/applying_mapreduce/data/output");

        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        return job.waitForCompletion(true) ? 0 : 1;

    }

        public static void main(String[] args) throws Exception {
            int exitCode = ToolRunner.run(new Main(), args);
            System.exit(exitCode);
        }


}
