package org.apache.flink.quickstart;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

import java.util.Calendar;
import java.util.TimeZone;

import org.apache.flink.api.java.DataSet;


public class AirportTrends {

    public enum JFKTerminal {
        TERMINAL_1(71436),
        TERMINAL_2(71688),
        TERMINAL_3(71191),
        TERMINAL_4(70945),
        TERMINAL_5(70190),
        TERMINAL_6(70686),
        NOT_A_TERMINAL(-1);

        int mapGrid;

        private JFKTerminal(int grid) {
            this.mapGrid = grid;
        }

        public static JFKTerminal gridToTerminal(int grid) {
            for (JFKTerminal terminal : values()) {
                if (terminal.mapGrid == grid) return terminal;
            }
            return NOT_A_TERMINAL;
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // get the taxi ride data stream - Note: you got to change the path to your local data file
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(
                        "/Users/nicolovendramin/flinkLab/flink-java-project/src/main/java/org/apache/flink/quickstart/data/nycTaxiRides.gz",
                        60, 2000)
        );

        DataStream<TaxiRide> filteredRides = rides.filter(new JFKFilter());

        DataStream<TaxiRide> timeRides = filteredRides.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TaxiRide>(Time.seconds(2)) {

            @Override
            public long extractTimestamp(TaxiRide element) {

                DateTime time;

                if(JFKTerminal.gridToTerminal(GeoUtils.mapToGridCell(element.startLon, element.startLat))
                        != JFKTerminal.NOT_A_TERMINAL){

                    time = element.startTime;

                }
                else
                    time = element.endTime;

                return time.getMillis();
            }
        });

        DataStream<Tuple3<JFKTerminal, Integer, Integer>> uncounted = filteredRides
                        .flatMap(new MapRideToTerminalAndHour())
                        .keyBy(0,2)
                        .timeWindow(Time.hours(1))
                        .sum(1);

        /**
         * Write your application here
         **/

        uncounted.print();
        env.execute();

    }

    public static class JFKFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {

            boolean cond_one = !(JFKTerminal.gridToTerminal(GeoUtils.mapToGridCell(
                    taxiRide.endLon, taxiRide.endLat)).equals(JFKTerminal.NOT_A_TERMINAL));

            boolean cond_two = !(JFKTerminal.gridToTerminal(GeoUtils.mapToGridCell(
                    taxiRide.startLon, taxiRide.startLat)).equals(JFKTerminal.NOT_A_TERMINAL));

            if (cond_one || cond_two) {
                return true;
            }
            else return false;
        }
    }

    public static final class MapRideToTerminalAndHour implements FlatMapFunction<TaxiRide, Tuple3<JFKTerminal, Integer, Integer>> {

        @Override
        public void flatMap(TaxiRide taxiRide, Collector<Tuple3<JFKTerminal, Integer, Integer>> out) {

            int gridIdStart = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
            JFKTerminal start = JFKTerminal.gridToTerminal(gridIdStart);

            int gridIdEnd = GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat);
            JFKTerminal end = JFKTerminal.gridToTerminal(gridIdEnd);

            long startTime = 0;
            long endTime = 0;

            if (!start.equals(JFKTerminal.NOT_A_TERMINAL)) {
                startTime = taxiRide.startTime.getMillis();
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
                calendar.setTimeInMillis(startTime);
                Integer hour = calendar.get(Calendar.HOUR_OF_DAY);
                out.collect(new Tuple3<>(start, new Integer(1), hour));
            }

            if (!end.equals(JFKTerminal.NOT_A_TERMINAL)) {
                endTime = taxiRide.endTime.getMillis();
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
                calendar.setTimeInMillis(endTime);
                Integer hour = calendar.get(Calendar.HOUR_OF_DAY);
                out.collect(new Tuple3<>(end, new Integer(1), hour));
            }

        }
    }
}


