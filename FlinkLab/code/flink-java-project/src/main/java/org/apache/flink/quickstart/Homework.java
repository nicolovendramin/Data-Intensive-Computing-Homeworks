package org.apache.flink.quickstart;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import java.util.Scanner;
import java.util.Calendar;
import java.util.TimeZone;

public class Homework{

    public enum JFKTerminal {
        TERMINAL_1(71436),
        TERMINAL_2(71688),
        TERMINAL_3(71191),
        TERMINAL_4(70945),
        TERMINAL_5(70190),
        TERMINAL_6(70686),
        NOT_A_TERMINAL(-1);

        int mapGrid;

        private JFKTerminal(int grid){
            this.mapGrid = grid;
        }

        public static JFKTerminal gridToTerminal(int grid){
            for(JFKTerminal terminal : values()){
                if(terminal.mapGrid == grid) return terminal;
            }
            return NOT_A_TERMINAL;
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // get the taxi ride data stream
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(
                        "/Users/nicolovendramin/flinkLab/flink-java-project/src/main/java/org/apache/flink/quickstart/data/nycTaxiRides.gz",
                        60, // Watermark
                        2000));

        // Extracting the timestamp for the events
        DataStream<TaxiRide> rides_ts = rides;
              /*  rides.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TaxiRide>(Time.seconds(300)) {

                    @Override
                    public long extractTimestamp(TaxiRide taxiRide) {
                        DateTime time;

                        if(JFKTerminal
                                .gridToTerminal(GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat))
                                != JFKTerminal.NOT_A_TERMINAL){

                            time = taxiRide.startTime;

                        }
                        else
                            time = taxiRide.endTime;

                        return time.getMillis();
                    }

                });*/

        // Reading from System.in

        Scanner reader = new Scanner(System.in);

        // We keep reading until we get a valid choice
        int n = -1;
        while(n<0 || n>3) {
            System.out.println(
                    "Enter the number of the task you want to print " +
                            "[1 -> terminal visit per hour, 2-> busiest terminal per hour, 3 -> busiest in exit per hour " +
                            "0 -> both]: ");
            n = reader.nextInt(); // Scans the next token of the input as an int.
        }
        reader.close();

        // Generating the result of task1
        DataStream<Tuple3<JFKTerminal, Integer, Integer>> terminal_rides = rides_ts
                .filter(new JFKFilter()) // filtering rides starting or arriving in JFK terminals
                .flatMap(new TerminalPresenceTimeFlatMapper()) // mapping each ride to the leave-arrive events
                .keyBy(2) // grouping the result by hour of the day
                .keyBy(0) // grouping inside each single grouping by terminal
                .timeWindow(Time.hours(1))  // defining a one hour time window
                .sum(1); // summing over the grouping in the desired time window

        // Generating the result of task2 from previous exercise
        DataStream<Tuple3<JFKTerminal, Integer, Integer>> terminal_rides_max = terminal_rides
                .keyBy(2) // grouping the previous result by hour of the day
                .timeWindowAll(Time.hours(1)) // selecting a time window of one hour across all nodes
                .max(1); // picking the max with respect to the counting

        // Generating the result of task2 considering only trips leaving the terminal
        DataStream<Tuple3<JFKTerminal, Integer, Integer>> terminal_rides_max_leaving = rides_ts
                .filter(new JFKFilterStart()) // filtering rides leaving from JFK terminals
                .map(new TerminalLeavingTimeMapper()) // mapping each ride to the leave events
                .keyBy(2) // grouping the result by hour of the day
                .keyBy(0) // grouping inside each single grouping by terminal
                .timeWindow(Time.hours(1))  // defining a one hour time window
                .sum(1) // summing over the grouping in the desired time window
                .keyBy(2) // grouping the previous result by hour of the day
                .timeWindowAll(Time.hours(1)) // selecting a time window of one hour across all nodes
                .max(1); // picking the max with respect to the counting

        // printing only the required results
        if(n == 1 || n == 0){
            terminal_rides.print();
        }
        if(n == 2 || n == 0) {
            terminal_rides_max.print();
        }
        if(n == 3 || n == 0) {
            terminal_rides_max_leaving.print();
        }

        env.execute();

    }

    /*
    Filters only those rides that either start or end in a JFK Terminal
     */
    public static class JFKFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {


            int gridStart = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
            int gridEnd = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);

            boolean conditionOne = JFKTerminal.gridToTerminal(gridStart) != JFKTerminal.NOT_A_TERMINAL;
            boolean conditionTwo = JFKTerminal.gridToTerminal(gridEnd) != JFKTerminal.NOT_A_TERMINAL;

            if(conditionOne || conditionTwo)
                return true;
            else return false;
        }
    }

    /*
    Filters only those rides that either start or end in a JFK Terminal
    */
    public static class JFKFilterStart implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {


            int gridStart = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);

            boolean conditionOne = JFKTerminal.gridToTerminal(gridStart) != JFKTerminal.NOT_A_TERMINAL;

            if(conditionOne)
                return true;
            else return false;
        }
    }

    /*
    This mapper extract from a stream of taxi rides the tuple containing the terminal they visited in exit,
    a one and the time of the day in which the visit happened.
    */
    public static class TerminalLeavingTimeMapper implements MapFunction<TaxiRide, Tuple3<JFKTerminal, Integer, Integer>> {

        @Override
        public Tuple3<JFKTerminal, Integer, Integer> map(TaxiRide taxiRide) throws Exception {

            int gridStart = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);

            Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            calendar.setTimeInMillis(taxiRide.startTime.getMillis());

            return new Tuple3<>(JFKTerminal.gridToTerminal(gridStart), 1, calendar.get(Calendar.HOUR_OF_DAY));

        }
    }

    /*
    This flat mapper extract from a stream of taxi rides the tuple containing the terminal they visited (either
    in entrance or exit), a one and the time of the day in which the visit happened. This cover also the case
    in which a ride both started and arrived in a terminal of the airport.
     */
    public static final class TerminalPresenceTimeFlatMapper implements FlatMapFunction<TaxiRide, Tuple3<JFKTerminal, Integer, Integer>> {

        @Override
        public void flatMap(TaxiRide taxiRide, Collector<Tuple3<JFKTerminal, Integer, Integer>> out) throws Exception {

            int gridStart = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
            int gridEnd = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);

            if(JFKTerminal.gridToTerminal(gridStart) != JFKTerminal.NOT_A_TERMINAL){

                Calendar calendar = Calendar.getInstance();
                calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
                calendar.setTimeInMillis(taxiRide.startTime.getMillis());

                out.collect(new Tuple3<>(JFKTerminal.gridToTerminal(gridStart), 1, calendar.get(Calendar.HOUR_OF_DAY)));
            }

            if(JFKTerminal.gridToTerminal(gridEnd) != JFKTerminal.NOT_A_TERMINAL){

                Calendar calendar = Calendar.getInstance();
                calendar.setTimeZone(TimeZone.getTimeZone( "America/New_York" ));
                calendar.setTimeInMillis(taxiRide.endTime.getMillis());

                out.collect(new Tuple3<>(JFKTerminal.gridToTerminal(gridEnd), 1, calendar.get(Calendar.HOUR_OF_DAY)));
            }
        }
    }
}
