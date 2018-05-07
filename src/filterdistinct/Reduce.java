package filterdistinct;

/**
 * Created by Toks on 28/12/2016.
 */

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Reduce extends Reducer <Text, NullWritable, Text, NullWritable> {
    @Override
    public void reduce(final Text key,
                       final Iterable<NullWritable> values,
                       final Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());

    }
}
