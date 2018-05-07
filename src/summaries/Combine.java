package summaries;

/**
 * Created by Toks on 28/12/2016.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Combine extends Reducer<Text, NumPair, Text, NumPair> {

    @Override
    public void reduce(final Text key, final Iterable<NumPair> values, final Context context)
        throws IOException, InterruptedException {
        Double sum = 0.0;
        Integer count = 0;

        for (NumPair value : values) {
            sum += value.getFirst().get();
            count += value.getSecond().get();

        }

        context.write(key, new NumPair(sum, count));
    }
}
