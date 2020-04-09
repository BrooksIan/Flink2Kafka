package com.flanksteak;

import com.flanksteak.utils.GeoEnrich;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Properties;

/*
 * Copyright 2020 CV-19 Bats
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This Class demonstrates how to connect to a Kafka topic from Flink and query its contents
 * Enjoy with coffee
 */
public class GenericKafkaQuery {

    //Class Member Static Variables
    static StreamExecutionEnvironment fsEnv;
    static StreamTableEnvironment fsTableEnv;
    static EnvironmentSettings fsSettings;

    /**
     * This is the main function of the class
     * @param args  There are completely ignored, thanks :)
     * @throws Exception Exceptions are largely ignored too.  One needs to live without caught exceptions.
     */
    public static void main(String[] args) throws Exception {

        //Testing Values
        String KSourceTopic = "rawInput";
        String KDestTopic = "enriched";
        String QTableName = "TaxiRides";
        String KafkaSinkName = "KafkaSink";

        // create execution environment
       fsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database")
                .build();

        fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // configure event-time and watermarks
        fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        fsEnv.getConfig().enableForceAvro();
        fsEnv.getConfig().setAutoWatermarkInterval(1000L);

        //Create Streaming Table Environment
        fsTableEnv  = StreamTableEnvironment.create(fsEnv, fsSettings);
        fsTableEnv.registerFunction("toCellId", new GeoEnrich.ToCellId());
       // TableEnvironment TableEnv = TableEnvironment.create(fsSettings);

        //Connect FlaNKStyle
        FLANKStyle(KSourceTopic, fsEnv);

        //Create Table on Kafka Topic
        KafkaConnectorStyle(QTableName, KSourceTopic, KDestTopic, KafkaSinkName);

        // Execute query and write to screen
            //querySelectAllTempTable(QtableName);
            //queryCountByMed(QtableName);

        // Execute query and write to Kafka topic
        queryAddTimeStampTempTable(QTableName, KafkaSinkName);

        fsEnv.execute("Kafka Read");
    }  // End of Main Function

    /**
     * This function is used to establish a basic connection to a Kakfa topic from Flink
     *   If a connection is established, then it will print the contents of the table to screen in all Strings
     * @param sourceTopic Name of the Kafka source topic to read from
     * @param env Flink Stream Execution Environment
     */
    private static void FLANKStyle(String sourceTopic,  StreamExecutionEnvironment env){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");                 // Consumer group ID
        properties.setProperty("auto.offset.reset", "earliest");       // Always read topic from start

        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(sourceTopic, new SimpleStringSchema(), properties);
        DataStream<String> source = env.addSource(consumer).name("Flink Kafka Source");
        source.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));        // debug to cluster logs   source.print();

        source.filter(new NotNullFilter());

        source.print();
    }  // End of Function

    /**
     * This function is used to establish a table connection to a Kafka topic from Flink with a user defined schema
     *      If a connection is established, then the table can be queried in the table stream environment
     * @param tableName Name of new temporary table in Flink that is linked to a Kafka topic
     * @param topicSourceName Name of the Kafka source topic
     * @param topicDestName Name of the Kafka destination topic
     * @param KafkaSinkName Name of the Kafka table sink
     */
    private static void KafkaConnectorStyle(String tableName, String topicSourceName, String topicDestName, String KafkaSinkName)
    {

        //Create Flink connection to Kafka source topic
        CreateKafkaJSONTempTable(tableName, topicSourceName);

        //Test Connection
        printDBs();
        printTbls();

        //Create Flink connection to Kafka destination topic
        WriteJSON2KafkaJSON(KafkaSinkName, topicDestName);

    }  // End of function

    /**
     * This function is used to
     * @param tableName Name of temporary Flink table
     * @param topicName name of Kafka topic as source
     */
    private static void CreateKafkaJSONTempTable(String tableName, String topicName )
    {

        // create table environment
        fsTableEnv.connect(
                new Kafka()
                        .version("universal")
                        .topic(topicName)
                        .startFromLatest()
                        .property("zookeeper.connect", "localhost:2181")
                        .property("bootstrap.servers", "localhost:9092")
                        .property("group.id", "test")
        )
                // declare a format for this system
                .withFormat(
                        new Json()

                )
                // declare the schema of the table
                .withSchema(
                        new Schema()
                                .field("medallion", DataTypes.STRING())
                                .field("licenseId", DataTypes.STRING())
                                .field("pickUpTime", DataTypes.STRING())
                                .field("dropOffTime", DataTypes.STRING())
                                .field("trip_time_in_secs", DataTypes.BIGINT())
                                .field("trip_distance", DataTypes.FLOAT())
                                .field("pickUpLon", DataTypes.FLOAT())
                                .field("pickUpLat", DataTypes.FLOAT())
                                .field("dropOffLon", DataTypes.FLOAT())
                                .field("dropOffLat", DataTypes.FLOAT())
                                .field("payment_type", DataTypes.STRING())
                                .field("fare_amount", DataTypes.FLOAT())
                                .field("surcharge", DataTypes.FLOAT())
                                .field("mta_tax", DataTypes.FLOAT())
                                .field("tip_amount", DataTypes.FLOAT())
                                .field("tolls_amount", DataTypes.FLOAT())
                                .field("total", DataTypes.FLOAT())
                )
                .inAppendMode()
                // create a table with given name
                .createTemporaryTable(tableName);


    } // End of Function

    /**
     *
     * @param tableName Name of temporary table in Flink that will be created
     * @param topicName Name of destination Kafka topic to write contents
     */
    private static void WriteJSON2KafkaJSON(String tableName, String topicName )
    {

        // create table environment
        fsTableEnv.connect(
                new Kafka()
                        .version("universal")
                        .topic(topicName)
                        .startFromLatest()
                        .property("zookeeper.connect", "localhost:2181")
                        .property("bootstrap.servers", "localhost:9092")
                        .property("group.id", "test")
        )
                // declare a format for this system
                .withFormat(
                        new Json()

                )
                // declare the schema of the table
                .withSchema(
                        new Schema()
                                .field("medallion", DataTypes.STRING())
                                .field("TimeStamp", DataTypes.TIMESTAMP(3) )
                                .field("Area", DataTypes.INT() )
                )
                .inAppendMode()
                // create a table with given name
                .createTemporaryTable(tableName);
    } // End of Function

    /**
     *  This function is used to print all the databases in the Flink Table Environment
     */
    private static void printDBs()
    {
        System.out.println("\nList Databases: ");

        String[] listDatabases = fsTableEnv.listDatabases();
        for (String element: listDatabases) {
            System.out.println(element);
        }

    }  // End of Function

    /**
     * This function is used to print all the tables in Flink database
     */
    private static void printTbls()
    {
        System.out.println("\nList Tables: ");

        String[] listTables = fsTableEnv.listTables();
        for (String element: listTables) {
            System.out.println(element);
        }

    } // End of Function

    /**
     * This function is used to query all from the table
     * @param tableName Name of table to query
     */
    private static void querySelectAllTempTable(String tableName)
    {

        // define SQL query to compute average total per area and hour
        Table result = fsTableEnv.sqlQuery(
                "SELECT " +
                        " * " +
                        "FROM " + tableName
        );

        // convert result table into a stream and print it
        fsTableEnv.toAppendStream(result, Row.class)
                .print();

    } // End of Function

    /**
     * This function is used to query table and add a time stamp & NYC Geo ID
     * This demonstrates transformation and enrichment on a stream
     * @param tableName Name of table to query
     */
    private static void queryAddTimeStampTempTable(String tableName, String tableSinkName)
    {

        // define SQL query to compute average total per area and hour
        Table result = fsTableEnv.sqlQuery(
                "SELECT " +
                        " medallion, CURRENT_TIMESTAMP, " +
                        " toCellId(dropOffLon, dropOffLat) AS area " +
                        " FROM " + tableName
        );

        result.insertInto(tableSinkName);
        // convert result table into an append stream and print it
        fsTableEnv.toAppendStream(result, Row.class)
                .print();

    } // End of Function

    /**
     * This function is used to query count the rides by different NYC drivers
     * Note - Medallion is an identifier for the taxi drivers
     * @param tableName Name of table to query
     */
    private static void queryCountByMed(String tableName)
    {

        Table result = fsTableEnv.sqlQuery(
                "SELECT " +
                        "  medallion," +
                        "  COUNT(total) AS countTotal " +
                        "FROM TaxiRides " +
                        " GROUP BY " +
                        "  medallion "
        );

        // convert result table into an append stream and print it
        fsTableEnv.toRetractStream(result, Row.class)
                .print();
        System.out.println();

    } // End of Function

    /**
     * This function is used to filter null values from raw stream
     */
    private static class NotNullFilter implements FilterFunction<String> {
        @Override
        public boolean filter(String string) throws Exception {
            if ( string == null || string.isEmpty() || string.trim().length() <=0) {
                return false;
            }

            return true;
        }
    } // End of Function

    /**
     * This function is used to filter null values from raw stream
     */
    private static class NotNullONFilter implements FilterFunction<ObjectNode> {
        @Override
        public boolean filter(ObjectNode ONvalue) throws Exception {
            if ( ONvalue.get("value") == null) {
                return false;
            }

            return true;
        }
    } // End of Function

} // End of Class