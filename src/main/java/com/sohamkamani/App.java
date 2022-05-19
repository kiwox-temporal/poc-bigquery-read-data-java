package com.sohamkamani;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DmlStats;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.TableResult;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;

// Sample to query in a table
public class App {

    public static final String STORAGE_PROJECT_CLARO_TEST_ID = "claro-test-332211";
    public static final String TABLE_CLARO_TEST = "`claro-test-332211.test.CPEHistoricData`";

    public static final String STORAGE_PROJECT_ID = "test-project-350020";

    public static final String DATA_SET = "Pruebas";
    public static final String TABLE = "AllTables";

    public static final String[] ATTRIBUTES_FROM_TABLE = new String[] { "BusinessEntityID", "rowguid", "ModifiedDate" };
    public static final String[] FIELDS_FOR_ORDER = new String[] { "ModifiedDate" };

    public static final Map<String, Object> CRITERIA_FILTER = new HashMap<String, Object>() {
        {
            put("PersonType", "'EM'");
            // put("key", "value");
            // etc
        }
    };

    public static final String TABLE_TARGET = String.format("`%s.%s.%s`", STORAGE_PROJECT_ID, DATA_SET, TABLE);

    private static void insertSampleData() {
        // Step 1: Initialize BigQuery service
        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(STORAGE_PROJECT_ID)
                .build().getService();
        System.out.println(bigquery);

        // Step 2: Create insertAll (streaming) request
        InsertAllRequest insertAllRequest = getInsertRequest();

        // Step 3: Insert data into table
        InsertAllResponse response = bigquery.insertAll(insertAllRequest);

        // Step 4: Check for errors and print results
        if (response.hasErrors()) {
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors()
                    .entrySet()) {
                System.out.printf("error in entry %d: %s", entry.getKey(),
                        entry.getValue().toString());
            }
            return;
        }
        System.out.println("inserted successfully");
    }

    // To create a streaming insert request, we need to specify the table and
    // dataset id
    // and create the rows we want to insert
    private static InsertAllRequest getInsertRequest() {
        String datasetId = "sample_dataset";
        String tableId = "vegetables";
        return InsertAllRequest.newBuilder(datasetId, tableId).addRow(getRow(1, "carrot"))
                .addRow(getRow(2, "beans")).build();

    }

    // each row is a map with the row name as the key and row value as the value
    // since the value type is "Object" it can take any arbitrary type, based on
    // the datatype of the row defined on BigQuery
    private static Map<String, Object> getRow(int id, String vegetableName) {
        Map<String, Object> rowMap = new HashMap<String, Object>();
        rowMap.put("id", id);
        rowMap.put("name", vegetableName);
        return rowMap;
    }

    public static void main(String... args) throws Exception {
        // insertSampleData();
        // loadSimpleQuery();
        //loadComplexQuery();
        loadQueryTest();
    }

    private static String getCriteriaFilterFromMap(Map<String, Object> criteriaMap) {
        String result = "";
        Iterator<Map.Entry<String, Object>> itr = criteriaMap.entrySet()
                .iterator();
        while (itr.hasNext()) {
            Map.Entry<String, Object> entry = itr.next();
            result += String.format("%s=%s", entry.getKey(), String.valueOf(entry.getValue()));
            if (itr.hasNext()) {
                result += " AND ";
            }
        }
        System.out.println(result);
        return result;
    }

    private static String getAttributesFromArray(String[] attributes) {
        return String.join(",", Arrays.asList(attributes));
    }

    private static String getOrderByFromArray(String[] fields, String direction, int limit) {
        String fieldsForOrderBy = String.join(",", Arrays.asList(fields));
        return String.format("%s %s LIMIT %s", fieldsForOrderBy, direction, limit);
    }

    private static void loadQueryTest() throws Exception {

        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(STORAGE_PROJECT_CLARO_TEST_ID)
                .build().getService();

        String GET_WORD_COUNT = "SELECT * FROM " + TABLE_CLARO_TEST;

        System.out.println(GET_WORD_COUNT);

        Instant start = Instant.now();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(GET_WORD_COUNT)
                .build();

        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());
        queryJob = queryJob.waitFor();
        // the waitFor method blocks until the job completes
        // and returns `null` if the job doesn't exist anymore
        if (queryJob == null) {
            throw new Exception("job no longer exists");
        }
        // once the job is done, check if any error occured
        if (queryJob.getStatus().getError() != null) {
            throw new Exception(queryJob.getStatus().getError().toString());
        }

        System.out.println(String.format("%s\t%s\t\t%s", "id", "value", "cpe"));
        System.out.println("------------------------------------------------------------------------------------");
        TableResult result = queryJob.getQueryResults();
        for (FieldValueList row : result.iterateAll()) {
            // We can use the `get` method along with the column
            // name to get the corresponding row entry
            String id = row.get("id").getStringValue();
            String value = row.get("value").getStringValue();
            Long cpe = row.get("cpe").getLongValue();

            System.out.printf("%s\t%s\t%s\n", id, value, cpe);
        }
        Instant end = Instant.now();
        System.out.println(String.format("TIME EXECUTED FOR QUERY: %d", Duration.between(start, end).getSeconds()));
    }

    private static void loadSimpleQuery() throws Exception {

        // Step 1: Initialize BigQuery service
        // Here we set our project ID and get the `BigQuery` service object
        // this is the interface to our BigQuery instance that
        // we use to execute jobs on
        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(STORAGE_PROJECT_ID)
                .build().getService();

        // Step 2: Prepare query job
        // A "QueryJob" is a type of job that executes SQL queries
        // we create a new job configuration from our SQL query and
        String GET_WORD_COUNT = "SELECT  " + getAttributesFromArray(ATTRIBUTES_FROM_TABLE) +
                " FROM " + TABLE_TARGET +
                // " WHERE " + getCriteriaFilterFromMap(CRITERIA_FILTER) +
                " ORDER BY " + getOrderByFromArray(FIELDS_FOR_ORDER, "DESC", 20000) + "";

        System.out.println(GET_WORD_COUNT);

        Instant start = Instant.now();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(GET_WORD_COUNT)
                .build();

        // Step 3: Run the job on BigQuery
        // create a `Job` instance from the job configuration using the BigQuery service
        // the job starts executing once the `create` method executes
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());
        queryJob = queryJob.waitFor();
        // the waitFor method blocks until the job completes
        // and returns `null` if the job doesn't exist anymore
        if (queryJob == null) {
            throw new Exception("job no longer exists");
        }
        // once the job is done, check if any error occured
        if (queryJob.getStatus().getError() != null) {
            throw new Exception(queryJob.getStatus().getError().toString());
        }

        // Step 4: Display results
        // Print out a header line, and iterate through the
        // query results to print each result in a new line
        System.out.println(String.format("%s\t%s\t\t%s", ATTRIBUTES_FROM_TABLE[0], ATTRIBUTES_FROM_TABLE[1],
                ATTRIBUTES_FROM_TABLE[2]));
        System.out.println("------------------------------------------------------------------------------------");
        TableResult result = queryJob.getQueryResults();
        for (FieldValueList row : result.iterateAll()) {
            // We can use the `get` method along with the column
            // name to get the corresponding row entry
            Object firstColumn = row.get(ATTRIBUTES_FROM_TABLE[0]).getValue();
            Object secondColumn = row.get(ATTRIBUTES_FROM_TABLE[1]).getValue();
            long threeColumn = row.get(ATTRIBUTES_FROM_TABLE[2]).getTimestampValue();

            Date dateValue = new Date(new Timestamp(threeColumn).getTime());
            System.out.printf("%s\t%s\t%s\n", firstColumn, secondColumn, dateValue);
        }
        Instant end = Instant.now();
        System.out.println(String.format("TIME EXECUTED FOR QUERY: %d", Duration.between(start, end).getSeconds()));
    }

    private static void loadComplexQuery() throws Exception {
        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(STORAGE_PROJECT_ID)
                .build().getService();

        String complexQuerySQL = "SELECT * " +
                "FROM `test-project-350020.Pruebas.BusinessEntity` t1 " +
                "INNER JOIN `test-project-350020.Pruebas.Person` t2 " +
                "ON t1.BusinessEntityID = t2.BusinessEntityID " +
                "INNER JOIN `test-project-350020.Pruebas.PersonPhone` t3 " +
                "ON t3.BusinessEntityID = t2.BusinessEntityID " +
                "INNER JOIN `test-project-350020.Pruebas.PhoneType` t4 " +
                "ON t3.PhoneNumberTypeID = t4.PhoneNumberTypeID " +
                // "WHERE PersonType='EM' "+
                "ORDER BY t1.ModifiedDate DESC LIMIT 20000;";

        Instant start = Instant.now();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(complexQuerySQL)
                .build();
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());
        queryJob = queryJob.waitFor();

        if (queryJob == null) {
            throw new Exception("job no longer exists");
        }
        // once the job is done, check if any error occured
        if (queryJob.getStatus().getError() != null) {
            throw new Exception(queryJob.getStatus().getError().toString());
        }
        System.out.println(String.format("%s\t%s\t\t%s", ATTRIBUTES_FROM_TABLE[0], ATTRIBUTES_FROM_TABLE[1],
                ATTRIBUTES_FROM_TABLE[2]));
        System.out.println("------------------------------------------------------------------------------------");
        TableResult result = queryJob.getQueryResults();
        for (FieldValueList row : result.iterateAll()) {
            Object firstColumn = row.get(ATTRIBUTES_FROM_TABLE[0]).getValue();
            Object secondColumn = row.get(ATTRIBUTES_FROM_TABLE[1]).getValue();
            long threeColumn = row.get(ATTRIBUTES_FROM_TABLE[2]).getTimestampValue();
            Date dateValue = new Date(new Timestamp(threeColumn).getTime());
            System.out.printf("%s\t%s\t%s\n", firstColumn, secondColumn, dateValue);
        }
        Instant end = Instant.now();
        System.out.println(String.format("TIME EXECUTED FOR QUERY: %d", Duration.between(start, end).getSeconds()));

    }

    private static void insertViaQuery() throws Exception {

        // Step 1: Initialize BigQuery service
        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("test-project-350020")
                .build().getService();

        // Step 2: Prepare query job
        final String INSERT_VEGETABLES = "INSERT INTO `test-project-350020.sample_dataset.vegetables` (id, name) VALUES (1, 'carrot'), (2, 'beans');";
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(INSERT_VEGETABLES).build();

        // Step 3: Run the job on BigQuery
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());
        queryJob = queryJob.waitFor();
        if (queryJob == null) {
            throw new Exception("job no longer exists");
        }
        // once the job is done, check if any error occured
        if (queryJob.getStatus().getError() != null) {
            throw new Exception(queryJob.getStatus().getError().toString());
        }

        // Step 4: Display results
        // Here, we will print the total number of rows that were inserted
        JobStatistics.QueryStatistics stats = queryJob.getStatistics();
        Long rowsInserted = stats.getDmlStats().getInsertedRowCount();
        System.out.printf("%d rows inserted\n", rowsInserted);
    }
}
