package invertedindex;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Toks on 29/12/2016.
 */
public class Main extends Configured implements Tool {
    @Override
    public int run (String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJobName("invertedindex");
        job.setJarByClass(Main.class);

        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | "); // A config flag to indicate that a different separator (not the default tab) is used in the output file

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        Path inputFilePath = new Path("/Users/Toks/IdeaProjects/applying_mapreduce/src");
        Path outputFilePath = new Path("/Users/Toks/IdeaProjects/applying_mapreduce/data/output5");

        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        FileInputFormat.setInputDirRecursive(job, true); // The MapReduce job should look through all the sub-directories specified in the input

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }


}
