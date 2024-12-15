package in.anirbansinha.bigtableOps;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Use Google Bigtable to store and analyze sensor data.
 */
public class Bigtable {

    // TODO: Fill in information for your database
    public final String projectId = "lateral-vision-438701-u5";
    public final String instanceId = "g23ai2084";
    public final String COLUMN_FAMILY = "sensor";
    public final String tableId = "weather"; 

    private BigtableDataClient dataClient;
    private BigtableTableAdminClient adminClient;

    public static void main(String[] args) throws Exception {
        Bigtable testbt = new Bigtable();
        testbt.run();
    }

    public void connect() throws IOException {
        // Initialize the data client for Bigtable
        BigtableDataSettings dataSettings = BigtableDataSettings.newBuilder()
                .setProjectId(projectId)
                .setInstanceId(instanceId)
                .build();
        dataClient = BigtableDataClient.create(dataSettings);

        // Initialize the admin client for Bigtable
        BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
                .setProjectId(projectId)
                .setInstanceId(instanceId)
                .build();
        adminClient = BigtableTableAdminClient.create(adminSettings);

        System.out.println("Connected to Bigtable successfully.");
    }

    public void run() throws Exception {
        connect();

        // TODO: Comment or uncomment these as you proceed. Once data is loaded, comment
        // them out.
        // deleteTable();
        createTable();
        loadData();

        int temp = query1();
        System.out.println("Temperature: " + temp);

        int windspeed = query2();
        System.out.println("Windspeed: " + windspeed);

        ArrayList<Object[]> data = query3();
        StringBuilder buf = new StringBuilder();
        for (Object[] vals : data) {
            for (Object val : vals) {
                buf.append(val.toString()).append(" ");
            }
            buf.append("\n");
        }
        System.out.println(buf.toString());

        temp = query4();
        System.out.println("Temperature: " + temp);

        close();
    }

    /**
     * Close data and admin clients.
     */
    public void close() {
        if (dataClient != null) {
            dataClient.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
    }

    public void createTable() {
        try {
            // Check if the table already exists
            if (adminClient.exists(tableId)) {
                System.out.println("Table " + tableId + " already exists.");
                return;
            }

            // Define the table structure with a column family
            CreateTableRequest createTableRequest = CreateTableRequest.of(tableId)
                    .addFamily(COLUMN_FAMILY);

            // Create the table
            adminClient.createTable(createTableRequest);
            System.out.println("Table " + tableId + " created successfully with column family: " + COLUMN_FAMILY);
        } catch (Exception e) {
            System.err.println("Error creating table: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Loads data into the database.
     * Data is in CSV files. Note that it must be converted to hourly data.
     * Take the first reading in an hour and ignore any others.
     */
    public void loadData() throws Exception {
        String path = "data/";
        String[] stationIds = { "Pseudo-Julian-Date","Date","Time","Temperature","Dewpoint","Relhum","Speed","Gust","Pressure" }; // Station IDs for SeaTac, Vancouver, Portland
        String[] fileNames = { "seatac.csv", "vancouver.csv", "portland.csv" }; // CSV filenames

        try {
            for (int i = 0; i < stationIds.length; i++) {
                String stationId = stationIds[i];
                String fileName = fileNames[i];
                System.out.println("Loading data for " + stationId);

                // Read the data from the CSV file
                BufferedReader reader = new BufferedReader(new FileReader(path + fileName));
                String line;
                int lineNumber = 0;

                BulkMutation bulkMutation = BulkMutation.create(tableId);

                while ((line = reader.readLine()) != null) {
                    lineNumber++;

                    // Skip header row
                    if (lineNumber == 1) {
                        continue;
                    }

                    // Parse the CSV data
                    String[] fields = line.split(",");
                    if (fields.length < 7) {
                        System.err.println("Skipping malformed line: " + line);
                        continue;
                    }

                    String date = fields[0];
                    String hour = fields[1];
                    int temperature = Integer.parseInt(fields[2]);
                    int dewPoint = Integer.parseInt(fields[3]);
                    int humidity = Integer.parseInt(fields[4]);
                    int windSpeed = Integer.parseInt(fields[5]);
                    int pressure = Integer.parseInt(fields[6]);

                    // Use the station ID, date, and hour to create a unique row key
                    String rowKey = stationId + "#" + date + "#" + hour;

                    // Add the mutation for the row
                    bulkMutation.add(
                            rowKey,
                            Mutation.create()
                                    .setCell(COLUMN_FAMILY, "temperature", temperature)
                                    .setCell(COLUMN_FAMILY, "dew_point", dewPoint)
                                    .setCell(COLUMN_FAMILY, "humidity", humidity)
                                    .setCell(COLUMN_FAMILY, "wind_speed", windSpeed)
                                    .setCell(COLUMN_FAMILY, "pressure", pressure));
                }

                // Apply the bulk mutation to Bigtable
                dataClient.bulkMutateRows(bulkMutation);
                System.out.println("Data for " + stationId + " loaded successfully.");
                reader.close();
            }
        } catch (IOException e) {
            throw new Exception("Error reading or loading data: " + e.getMessage(), e);
        }
    }

    /**
     * Query returns the temperature at Vancouver on 2022-10-01 at 10 a.m.
     *
     * @return Temperature value
     * @throws Exception if an error occurs
     */
    public int query1() throws Exception {
        String stationId = "YVR"; // Vancouver station ID
        String date = "2022-10-01";
        String hour = "10";

        // Construct the row key for Vancouver on the given date and hour
        String rowKey = stationId + "#" + date + "#" + hour;

        try {
            // Read the row with the constructed row key
            Row row = dataClient.readRow(tableId, rowKey);

            if (row == null) {
                System.out.println("No data found for Vancouver on " + date + " at " + hour + " a.m.");
                return -1; // Return -1 to indicate no data found
            }

            // Extract the "temperature" cell value from the row
            for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                return Integer.parseInt(cell.getValue().toStringUtf8());
            }

            System.out.println("Temperature cell not found in the row.");
            return -1; // Return -1 if the temperature column is not present
        } catch (Exception e) {
            System.err.println("Error executing query1: " + e.getMessage());
            throw new Exception("Failed to execute query1", e);
        }
    }

    /**
     * Query returns the highest wind speed in the month of September 2022 in
     * Portland.
     *
     * @return Maximum wind speed
     * @throws Exception if an error occurs
     */
    public int query2() throws Exception {
        String stationId = "PDX"; // Portland station ID
        String startRowKey = stationId + "#2022-09-01#00"; // Start of September
        String endRowKey = stationId + "#2022-09-30#23"; // End of September
        int maxWindSpeed = 0;

        try {
            // Create a query to read rows within the specified range
            Query query = Query.create(tableId)
                    .range(startRowKey, endRowKey);

            // Execute the query and iterate over the rows
            ServerStream<Row> rows = dataClient.readRows(query);
            for (Row row : rows) {
                // Get the wind speed value from the row
                for (RowCell cell : row.getCells(COLUMN_FAMILY, "wind_speed")) {
                    int windSpeed = Integer.parseInt(cell.getValue().toStringUtf8());
                    // Update the maximum wind speed
                    if (windSpeed > maxWindSpeed) {
                        maxWindSpeed = windSpeed;
                    }
                }
            }

            System.out.println("Highest wind speed in Portland during September 2022: " + maxWindSpeed);
            return maxWindSpeed;
        } catch (Exception e) {
            System.err.println("Error executing query2: " + e.getMessage());
            throw new Exception("Failed to execute query2", e);
        }
    }

    /**
     * Query returns all the readings for SeaTac for October 2, 2022. Return as an
     * ArrayList of object arrays.
     * Each object array should have fields: date (string), hour (string),
     * temperature (int), dewpoint (int), humidity (string), windspeed (string),
     * pressure (string).
     *
     * @return ArrayList of readings
     * @throws Exception if an error occurs
     */
    public ArrayList<Object[]> query3() throws Exception {
        String stationId = "SEA"; // SeaTac station ID
        String date = "2022-10-02"; // Specific date
        String startRowKey = stationId + "#" + date + "#00"; // Start of the day
        String endRowKey = stationId + "#" + date + "#23";   // End of the day
    
        ArrayList<Object[]> data = new ArrayList<>();
    
        try {
            // Create a query to fetch rows for SeaTac on the given date
            Query query = Query.create(tableId)
                    .range(startRowKey, endRowKey);
    
            // Execute the query and process the results
            ServerStream<Row> rows = dataClient.readRows(query);
            for (Row row : rows) {
                String rowKey = row.getKey().toStringUtf8();
                String[] keyParts = rowKey.split("#");
    
                if (keyParts.length != 3) {
                    System.err.println("Skipping malformed row key: " + rowKey);
                    continue;
                }
    
                String hour = keyParts[2]; // Extract the hour from the row key
                int temperature = 0, dewPoint = 0, humidity = 0, windSpeed = 0, pressure = 0;
    
                // Retrieve sensor readings from the row
                for (RowCell cell : row.getCells(COLUMN_FAMILY)) {
                    String qualifier = cell.getQualifier().toStringUtf8();
                    int value = Integer.parseInt(cell.getValue().toStringUtf8());
    
                    switch (qualifier) {
                        case "temperature":
                            temperature = value;
                            break;
                        case "dew_point":
                            dewPoint = value;
                            break;
                        case "humidity":
                            humidity = value;
                            break;
                        case "wind_speed":
                            windSpeed = value;
                            break;
                        case "pressure":
                            pressure = value;
                            break;
                        default:
                            System.err.println("Unexpected column qualifier: " + qualifier);
                    }
                }
    
                // Add the row's data as an object array to the list
                data.add(new Object[] { date, hour, temperature, dewPoint, humidity, windSpeed, pressure });
            }
    
            System.out.println("Readings for SeaTac on " + date + " retrieved successfully.");
            return data;
        } catch (Exception e) {
            System.err.println("Error executing query3: " + e.getMessage());
            throw new Exception("Failed to execute query3", e);
        }
    }
    

    /**
     * Query returns the highest temperature at any station in the summer months of
     * 2022 (July (7), August (8)).
     *
     * @return Highest temperature
     * @throws Exception if an error occurs
     */
    public int query4() throws Exception {
        String startRowKey = "AAA#2022-07-01#00"; // Start of summer (station-independent prefix)
        String endRowKey = "ZZZ#2022-08-31#23";  // End of summer (station-independent prefix)
        int maxTemperature = Integer.MIN_VALUE;
    
        try {
            // Create a query to read rows within the summer months range
            Query query = Query.create(tableId)
                    .range(startRowKey, endRowKey);
    
            // Execute the query and process the results
            ServerStream<Row> rows = dataClient.readRows(query);
            for (Row row : rows) {
                // Extract the temperature from each row
                for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                    int temperature = Integer.parseInt(cell.getValue().toStringUtf8());
                    // Update the maximum temperature
                    if (temperature > maxTemperature) {
                        maxTemperature = temperature;
                    }
                }
            }
    
            System.out.println("Highest temperature in summer 2022: " + maxTemperature);
            return maxTemperature;
        } catch (Exception e) {
            System.err.println("Error executing query4: " + e.getMessage());
            throw new Exception("Failed to execute query4", e);
        }
    }
    


    /**
     * Delete the table from Bigtable.
     */
    public void deleteTable() {
        System.out.println("\nDeleting table: " + tableId);
        try {
            adminClient.deleteTable(tableId);
            System.out.printf("Table %s deleted successfully%n", tableId);
        } catch (NotFoundException e) {
            System.err.println("Failed to delete a non-existent table: " + e.getMessage());
        }
    }
}
