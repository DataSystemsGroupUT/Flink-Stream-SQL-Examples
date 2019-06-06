package ee.ut.cs.dsg;

import ee.ut.cs.dsg.source.ImpressionSource;
import ee.ut.cs.dsg.source.SensorSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

public class FlinkStreamSQLExamples {

    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        // Stream windowing
//        DataStream<Tuple2<String, Long>> pageViews = env.addSource(new PageViewsSource(1000)).assignTimestampsAndWatermarks(new WatermarkAssigner());
//
//        tableEnv.registerDataStream("PageViews", pageViews, "nation, rowtime.rowtime");
//
//        Table resultHop = tableEnv.sqlQuery("SELECT nation , COUNT(*) FROM PageViews GROUP BY HOP( rowtime , INTERVAL '1' MINUTE, INTERVAL '1' SECOND) , nation");
//
//        Table resultSession = tableEnv.sqlQuery("SELECT nation , COUNT(*), SESSION_START(rowtime , INTERVAL '1' SECOND), SESSION_ROWTIME(rowtime , INTERVAL '1' SECOND)  FROM PageViews GROUP BY SESSION( rowtime , INTERVAL '1' SECOND) , nation");
//
//        TupleTypeInfo<Tuple2<String, Long>> tupleTypeHop = new TupleTypeInfo<>(
//                Types.STRING(),
//                Types.LONG());
//
//        TupleTypeInfo<Tuple4<String, Long, Long, Long>> tupleTypeSession = new TupleTypeInfo<>(
//                Types.STRING(),
//                Types.LONG(),
//                Types.SQL_TIMESTAMP(),
//                Types.SQL_TIMESTAMP());
//
//
//        DataStream<Tuple2<String, Long>> queryResultHop = tableEnv.toAppendStream(resultHop, tupleTypeHop);
//
//        queryResultHop.print();
//
//        DataStream<Tuple4<String, Long, Long, Long>> queryResultSession = tableEnv.toAppendStream(resultSession, tupleTypeSession);
//
//        queryResultSession.print();

        // Stream to table joins
//        DataStream<Tuple4<String, String, Double, Long>> sensorReadings = env.addSource(new SensorSource());
//
//        tableEnv.registerDataStream("SensorReadings", sensorReadings, "sensorID, lineID, readingValue, rowtime.rowtime");
//
//        CsvTableSource itemsSource = new CsvTableSource.Builder().path("data.csv").ignoreParseErrors().field("ItemID", Types.STRING())
//                .field("LineID", Types.STRING()).build();
//
//       // tableEnv.registerTableSource("Items", new ItemsSource());
//        tableEnv.registerTableSource("Items", itemsSource);
//
//        Table streamTableJoinResult = tableEnv.sqlQuery("SELECT S.sensorID , S.readingValue , I.ItemID FROM SensorReadings S LEFT JOIN Items I ON S.lineID = I.LineID");
//
//        //RowTypeInfo<Tuple3<String, Double,String>> streamTableJoinResultTypeInfo = new TupleTypeInfo<>(Types.STRING(),Types.DOUBLE(), Types.STRING());
//
//        DataStream<Tuple2<Boolean, Row>> joinQueryResult = tableEnv.toRetractStream(streamTableJoinResult, Row.class );
//
//
//        joinQueryResult.print();


        // Stream to Stream joins
        // Trial #1
//        DataStream<Tuple2<String, Long>> impressions = env.addSource(new ImpressionSource(500));
//        DataStream<Tuple2<String, Long>> clicks = env.addSource(new ImpressionSource(500));
//
//        tableEnv.registerDataStream("Impressions", impressions, "impressionID, impressionTime");
//        tableEnv.registerDataStream("Clicks", clicks, "clickID, clickTime");
//
//
//        Table streamStreamJoinResult = tableEnv.sqlQuery("SELECT i.impressionID, i.impressionTime, c.clickID, c.clickTime FROM Impressions i , Clicks c WHERE i.impressionID = c.clickID AND c.clickTime BETWEEN i.impressionTime - INTERVAL '1' SECOND AND i.impressionTime");
//       // TupleTypeInfo<Tuple4<String, Long, String, Long>> streamStreamJoinResultTypeInfo = new TupleTypeInfo<>(Types.STRING(),Types.LONG(), Types.STRING(), Types.LONG());
//
//
//        DataStream<Row> streamStreamJoinResultAsStream = tableEnv.toAppendStream(streamStreamJoinResult,Row.class );
//
//        streamStreamJoinResultAsStream.print();

        // Trial #2
        DataStream<Tuple2<String, Long>> impressions = env.addSource(new ImpressionSource(500));
        DataStream<Tuple2<String, Long>> clicks = env.addSource(new ImpressionSource(100));

        tableEnv.registerDataStream("Impressions", impressions, "impressionID, rowtime.rowtime");
        tableEnv.registerDataStream("Clicks", clicks, "clickID, rowtime.rowtime");


        Table streamStreamJoinResult = tableEnv.sqlQuery("SELECT i.impressionID, i.rowtime as impresstionTime, c.clickID, cast (c.rowtime as TIMESTAMP) as clickTime FROM Impressions i , Clicks c WHERE i.impressionID = c.clickID AND c.rowtime BETWEEN i.rowtime - INTERVAL '1' SECOND AND i.rowtime");
        TupleTypeInfo<Tuple4<String, TimeIndicatorTypeInfo, String, TimeIndicatorTypeInfo>> streamStreamJoinResultTypeInfo = new TupleTypeInfo<>(Types.STRING(),Types.SQL_TIMESTAMP(), Types.STRING(), Types.SQL_TIMESTAMP());


        DataStream<Tuple4<String, TimeIndicatorTypeInfo, String, TimeIndicatorTypeInfo>> streamStreamJoinResultAsStream = tableEnv.toAppendStream(streamStreamJoinResult,streamStreamJoinResultTypeInfo);

        streamStreamJoinResultAsStream.print();

        env.execute("Example SQL");
    }
}
