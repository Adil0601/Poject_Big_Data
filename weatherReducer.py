package com.Weather;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        List<String> tempValues = new ArrayList<>();

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            if (parts.length == 3) {
                String tempMin = parts[0];
                String tempMax = parts[1];
                int increment = Integer.parseInt(parts[2]);

                count += increment;
                tempValues.add(tempMin + "," + tempMax);
            }
        }

        // Join all temp_min,temp_max pairs
        String joinedTemps = String.join(" | ", tempValues);
        result.set(count + "\t" + joinedTemps);

        // Write the result: key (weather) and aggregated data
        context.write(key, result);
    }
}
