package com.tcghl.data;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ShowCreateTableTask {
    private static final Logger log = LoggerFactory.getLogger(ShowCreateTableTask.class);

    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.3.1.91")
                .port(3306)
                .username("root")
                .password("mysql8")
                .databaseList("tcg_loyalty", "tcg_identity")
                .tableList("tcg_loyalty.*", "tcg_identity.*")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Flink CDC MySql Source");

        Map<String, OutputTag<String>> outputTagMap = new HashMap<>();

        SingleOutputStreamOperator<String> process = dataStreamSource.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String row, ProcessFunction<String, String>.Context context, Collector<String> collector) {
                log.info("row-> [{}]", row);
                JSONObject rowJson = JSON.parseObject(row);

                String op = rowJson.getString("op");
                JSONObject source = rowJson.getJSONObject("source");
                String sourceTable = source.getString("sourceTable");
                String sourceDB = source.getString("db");

                String table = sourceDB + "_" + sourceTable;

                outputTagMap.putIfAbsent(table, new OutputTag<String>(table){});

                //only sync insert
                if ("c".equals(op)) {
                    String value = rowJson.getJSONObject("after").toJSONString();
                    context.output(outputTagMap.get(table), value);
                }
            }
        });

        outputTagMap.forEach((key, value) -> process.getSideOutput(value).sinkTo(buildDorisSink(key)));

//        process.getSideOutput(tableA).sinkTo(buildDorisSink(TABLE_A));
//        process.getSideOutput(tableB).sinkTo(buildDorisSink(TABLE_B));

        env.execute("Full Database Sync ");
    }

    public static DorisSink buildDorisSink(String table){
        log.info("buildDorisSink.target.table={}", table);

        DorisOptions.Builder dorisBuilder = DorisOptions.builder()
                .setFenodes("10.3.1.71:8030")
                .setTableIdentifier("cdctest." + table)
                .setUsername("root")
                .setPassword("");

        Properties pro = new Properties();
        //json data format
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        pro.setProperty("sink.enable-delete", "true");
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setDeletable(true)
                .setLabelPrefix("cdctest-" + System.currentTimeMillis()) //streamload label prefix,
                .setStreamLoadProp(pro).build();

        DorisSink.Builder<String> builder = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer()) //serialize according to string
                .setDorisOptions(dorisBuilder.build());

        return builder.build();
    }
}
