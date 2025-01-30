package com.Weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherMapper extends Mapper<Object, Text, Text, Text> {

    private final Text weatherKey = new Text();
    private final Text weatherData = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Convert the input line to a string
        String line = value.toString().trim();

        // Split the line into columns
        String[] columns = line.split(",");

        // Ensure the line has the expected number of columns to avoid ArrayIndexOutOfBoundsException
        if (columns.length > 5) { // Adjust based on dataset
            try {
                // Parse temp_min and temp_max
                float tempMin = Float.parseFloat(columns[3].trim());
                float tempMax = Float.parseFloat(columns[2].trim());
                String weather = columns[5].trim();

                // Prepare the key-value pair
                weatherKey.set(weather);
                weatherData.set(tempMin + "," + tempMax + ",1");

                // Write the key-value pair to the context
                context.write(weatherKey, weatherData);
            } catch (NumberFormatException e) {
                // Skip rows with invalid numeric values
                System.err.println("Invalid number format: " + e.getMessage());
            }
        } else {
            // Log if the line doesn't have enough columns
            System.err.println("Invalid line format: " + line);
        }
    }
}
