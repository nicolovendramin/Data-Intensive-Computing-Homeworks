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

import java.util.Calendar;
import java.util.TimeZone;

public class Airport{

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

        // get the taxi ride data stream - Note: you got to change the path to your local data file
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("/Users/nicolovendramin/flinkLab/flink-java-project/src/main/java/org/apache/flink/quickstart/data/nycTaxiRides.gz", 60, 2000));

        DataStream<TaxiRide> rides_ts =
                rides.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TaxiRide>(Time.seconds(2)) {

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
                });

        DataStream<Tuple3<JFKTerminal, Integer, Integer>> terminal_rides = rides_ts
                .filter(new JFKFilter())
                .flatMap(new TerminalPresenceTimeFlatMapper())
                .keyBy(2,0)
                .timeWindow(Time.hours(1))
                .sum(1);

        DataStream<Tuple3<JFKTerminal, Integer, Integer>> terminal_rides_max = terminal_rides
                .keyBy(2)
                .timeWindowAll(Time.hours(1))
                .max(1);

        //terminal_rides.print();
        terminal_rides_max.print();

        env.execute();

    }

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
    This mapper extract from a stream of taxi rides the tuple containing the terminal they visited (either
    in entrance or exit), a one and the time of the day in which the visit happened.
     */
    public static class TerminalPresenceTimeMapper implements MapFunction<TaxiRide, Tuple3<JFKTerminal, Integer, Integer>> {

        @Override
        public Tuple3<JFKTerminal, Integer, Integer> map(TaxiRide taxiRide) throws Exception {

            int gridStart = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);
            int gridEnd = GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat);

            if(JFKTerminal.gridToTerminal(gridStart) != JFKTerminal.NOT_A_TERMINAL){

                Calendar calendar = Calendar.getInstance();
                calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
                calendar.setTimeInMillis(taxiRide.startTime.getMillis());

                return new Tuple3<>(JFKTerminal.gridToTerminal(gridStart), 1, calendar.get(Calendar.HOUR_OF_DAY));
            }

            else {

                Calendar calendar = Calendar.getInstance();
                calendar.setTimeZone(TimeZone.getTimeZone( "America/New_York" ));
                calendar.setTimeInMillis(taxiRide.endTime.getMillis());

                return new Tuple3<>(JFKTerminal.gridToTerminal(gridEnd), 1, calendar.get(Calendar.HOUR_OF_DAY));
            }
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
