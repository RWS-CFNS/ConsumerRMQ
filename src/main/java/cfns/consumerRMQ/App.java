package cfns.consumerRMQ;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

public class App {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
    	Properties properties = new Properties();
    	try (InputStream input = new FileInputStream("config.properties")) {
    	    properties.load(input);
    	} catch (IOException e) {
    	    System.out.println("Sorry, unable to find config.properties");
    	    e.printStackTrace();
    	    return;
    	}

        // PostgreSQL Connection Info
        String dbUrl = properties.getProperty("db.url");
        String dbUsername = properties.getProperty("db.username");
        String dbPassword = properties.getProperty("db.password");

        // RabbitMQ Connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(properties.getProperty("rabbitmq.host"));
        factory.setUsername(properties.getProperty("rabbitmq.username"));
        factory.setPassword(properties.getProperty("rabbitmq.password"));

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // Callback to handle incoming messages
        channel.basicQos(1);
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");

            try {
                // Parse the JSON message
                JSONObject jsonTables = new JSONObject(message);

                // Prepare the SQL connection outside the loop
                try (java.sql.Connection dbConnection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword)) {
                    dbConnection.setAutoCommit(false);

                 // Handle conn table
                    if (jsonTables.has("conn")) {
                        JSONArray connArray = jsonTables.getJSONArray("conn");
                        String insertConnSQL = "INSERT INTO conn (id, mno, coordinates, rsrq, rsrp, sinr, mastid, tijd, rssi, lat, long, type, cell_plmn, tac_lac, cell_utran_id) " +
                                "VALUES (?, ?, ST_GeogFromText(?), ?, ?, ?, ?, CAST(? AS timestamp), ?, ?, ?, ?, ?, ?, ? ) " +
                                "ON CONFLICT (id, tijd) DO UPDATE SET " +
                                "rsrq = EXCLUDED.rsrq, " +
                                "rsrp = EXCLUDED.rsrp, " +
                                "sinr = EXCLUDED.sinr, " +
                                "rssi = EXCLUDED.rssi, " +
                                "lat = EXCLUDED.lat, " +
                                "long = EXCLUDED.long, " +
                                "type = EXCLUDED.type, " +
                                "cell_plmn = EXCLUDED.cell_plmn, " +
                                "tac_lac = EXCLUDED.tac_lac, " +
                                "cell_utran_id = EXCLUDED.cell_utran_id";
                               // "lat_mast = EXCLUDED.lat_mast, " +
                              //  "long_mast = EXCLUDED.long_mast";

                        try (PreparedStatement pstmt = dbConnection.prepareStatement(insertConnSQL)) {
                            for (int i = 0; i < connArray.length(); i++) {
                                JSONObject jsonRow = connArray.getJSONObject(i);
                                pstmt.setInt(1, jsonRow.getInt("id"));
                                pstmt.setString(2, jsonRow.getString("mno"));
                                pstmt.setString(3, jsonRow.getString("coordinates"));
                                pstmt.setFloat(4, jsonRow.getFloat("rsrq"));
                                pstmt.setFloat(5, jsonRow.getFloat("rsrp"));
                                pstmt.setFloat(6, jsonRow.getFloat("sinr"));
                                pstmt.setInt(7, jsonRow.getInt("mastid"));
                                pstmt.setString(8, jsonRow.getString("tijd"));
                                pstmt.setInt(9, jsonRow.getInt("rssi"));
                                pstmt.setFloat(10, jsonRow.getFloat("lat"));
                                pstmt.setFloat(11, jsonRow.getFloat("long"));
                                pstmt.setString(12, jsonRow.getString("type"));
                                
                                pstmt.setInt(13, jsonRow.getInt("cell_plmn"));
                                pstmt.setInt(14, jsonRow.getInt("tac_lac"));
                                pstmt.setInt(15, jsonRow.getInt("cell_utran_id"));
                                //pstmt.setFloat(16, jsonRow.getFloat("lat_mast"));
                                //pstmt.setFloat(17, jsonRow.getFloat("long_mast"));
                                pstmt.addBatch();
                            }
                            pstmt.executeBatch();
                        }
                    }
                    
                    // Handle weather table
                    if (jsonTables.has("weather")) {
                        JSONArray weatherArray = jsonTables.getJSONArray("weather");
                        String insertWeatherSQL = "INSERT INTO weather (id, temp, humid, winddir, windspeed, dauw, druk, time) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS timestamp)) " +
                                "ON CONFLICT (id, time) DO UPDATE SET " +
                                "temp = EXCLUDED.temp, " +
                                "humid = EXCLUDED.humid, " +
                                "winddir = EXCLUDED.winddir, " +
                                "windspeed = EXCLUDED.windspeed, " +
                                "dauw = EXCLUDED.dauw, " +
                                "druk = EXCLUDED.druk";

                        try (PreparedStatement pstmt = dbConnection.prepareStatement(insertWeatherSQL)) {
                            for (int i = 0; i < weatherArray.length(); i++) {
                                JSONObject jsonRow = weatherArray.getJSONObject(i);
                                pstmt.setInt(1, jsonRow.getInt("id"));
                                pstmt.setFloat(2, jsonRow.getFloat("temp"));
                                pstmt.setInt(3, jsonRow.getInt("humid"));
                                pstmt.setInt(4, jsonRow.getInt("winddir"));
                                pstmt.setFloat(5, jsonRow.getFloat("windspeed"));
                                pstmt.setFloat(6, jsonRow.getFloat("dauw"));
                                pstmt.setFloat(7, jsonRow.getFloat("druk"));
                                pstmt.setString(8, jsonRow.getString("time"));
                                pstmt.addBatch();
                            }
                            pstmt.executeBatch();
                        }
                    }

                    dbConnection.commit();
                    System.out.println(" [o] Data inserted into destination database");

                } catch (SQLException e) {
                    System.out.println("Failed to insert data: " + e.getMessage());
                    e.printStackTrace();
                }
                
//                startMastLocatieVerwerkerScript();

            } catch (Exception e) {
                System.out.println("Error processing message: " + e.getMessage());
                e.printStackTrace();
            }
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> { });
    }
    
//    private static void startMastLocatieVerwerkerScript() {
//        try {
//            Path tempScript = Files.createTempFile("mastLocatieVerwerker", ".py");
//            try (InputStream scriptStream = App.class.getResourceAsStream("/resources/mastLocatieVerwerker.py")) {
//                if (scriptStream == null) {
//                    throw new FileNotFoundException("Python script not found in resources");
//                }
//                Files.copy(scriptStream, tempScript, StandardCopyOption.REPLACE_EXISTING);
//            }
//
//            // Windows-specific command to open a new command prompt window
//            ProcessBuilder processBuilder = new ProcessBuilder("cmd.exe", "/c", "start", "python", tempScript.toAbsolutePath().toString());
//            processBuilder.start();
//
//        } catch (IOException e) {
//            System.out.println("Failed to execute Python script: " + e.getMessage());
//            e.printStackTrace();
//        }
//    }
    
}