package com.mycode.amazon;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.util.Args;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DynamoDBHandler {

    private static final String CLASS_NAME = DynamoDBHandler.class.getSimpleName();
    private static final Logger LOGGER = LogManager.getLogger(CLASS_NAME);

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // INSTANCE PART
    private AmazonDynamoDB dynamoDBClient;
    private DynamoDB dynamoDB;

    private DynamoDBHandler(){

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
                                "AMAZON_DYNAMODB_ACCESS_KEY_ID",
                                "AMAZON_DYNAMODB_SECRET_ACCESS_KEY");
        dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                                    .withRegion(Regions.EU_CENTRAL_1)   // Frankfurt AWS
                                    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                                    .build();
        dynamoDB = new DynamoDB(dynamoDBClient);
    }

    // This method is used to add rows into DynamoDB. Each row may have different set of columns, which must contain PrimaryKey column.
    public void addItems(String tableName, String primaryKey, List<Map<String, String>> lstOfkeyValuePairs){

        Args.notEmpty(tableName, "table");
        Args.notEmpty(primaryKey, "primaryKey");
        Args.notNull(lstOfkeyValuePairs, "lstOfkeyValuePairs");

        /* The BATCH write limitation is 25. It's not worth checking the table if it exists!
        TableDescription tableDes = dynamoDBClient.describeTable(new DescribeTableRequest(tableName)).getTable();
        if (!TableStatus.ACTIVE.toString().equals(tableDes.getTableStatus())){
            throw new RuntimeException(String.format("Table [%s] not exists.", tableName));
        }
        */

        LOGGER.info(String.format("DynamoDB Table [%s] with PrimaryKey [%s] is going to be added [%d] items.",
                tableName, primaryKey, lstOfkeyValuePairs.size()));

        TableWriteItems forumTableWriteItems = new TableWriteItems(tableName);

        for(Map<String, String> keyValuePairs: lstOfkeyValuePairs){

            if (!keyValuePairs.containsKey(primaryKey)){
                throw new RuntimeException(String.format("PrimaryKey [%s] not found in an product.", primaryKey));
            }

            Item item = new Item();
            // Add the primary key
            item.withPrimaryKey(primaryKey, keyValuePairs.get(primaryKey));
            // Add remaining key/value pairs
            for(String key: keyValuePairs.keySet()){
                if (!key.equals(primaryKey)){
                    item.withString(key, keyValuePairs.get(key));
                }
            }

            forumTableWriteItems.addItemToPut(item);
        }

        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(forumTableWriteItems);

        do{

            Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
            if (outcome.getUnprocessedItems().size() > 0) {
                outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
            }
        }
        while (outcome.getUnprocessedItems().size() > 0);

    }

    // This method is useful for getting a list of rows based on their PrimaryKey.
    public List<Map<String, String>> getItems(String tableName, String primaryKey, List<String> lstOfPrimaryKey){

        Args.notEmpty(tableName, "tableName");
        Args.notEmpty(primaryKey, "primaryKey");
        Args.notNull(lstOfPrimaryKey, "lstOfPrimaryKey");

        TableDescription tableDes = dynamoDBClient.describeTable(new DescribeTableRequest(tableName)).getTable();
        if (!TableStatus.ACTIVE.toString().equals(tableDes.getTableStatus())){
            throw new RuntimeException(String.format("Table [%s] not exists.", tableName));
        }

        if (lstOfPrimaryKey.stream().filter(x -> StringUtils.isEmpty(x)).collect(Collectors.toList()).size() > 0){
            throw new RuntimeException(String.format("At least one of the input primary keys is empty."));
        }

        List<Map<String, String>> lst = Lists.newArrayList();

        // First method
        TableKeysAndAttributes allprodTableKeysAndAttributes = new TableKeysAndAttributes(tableName);

        // Strange thing here! If directly using "lstOfPrimaryKey", compile-time is ok, but run-time causes problem:
        // AmazonDynamoDBException: The provided key element does not match the schema
        allprodTableKeysAndAttributes.addHashOnlyPrimaryKeys(primaryKey, lstOfPrimaryKey.toArray());

        BatchGetItemOutcome outcome = dynamoDB.batchGetItem(allprodTableKeysAndAttributes);

        Map<String, KeysAndAttributes> unprocessed = null;

        do{
            for (String currTableName : outcome.getTableItems().keySet()) {

                List<Item> items = outcome.getTableItems().get(currTableName);
                for (Item item : items) {

                    Map<String, String> tempMap = Maps.newHashMap();
                    for(Map.Entry<String, Object> currAttr: item.attributes()){
                        if (currAttr.getValue() != null){   // Allprod contains all products from all categories... Seemingly, they share a fixed set of columns in Dynamo table structure.
                            tempMap.put(currAttr.getKey(), currAttr.getValue().toString());
                        }
                    }

                    lst.add(tempMap);
                }
            }

            unprocessed = outcome.getUnprocessedKeys();

            if (!unprocessed.isEmpty()) {
                outcome = dynamoDB.batchGetItemUnprocessed(unprocessed);
            }
        }
        while(!unprocessed.isEmpty());

        return lst;
    }

    // This method is used to "scan" rows (go through all rows, check conditions, if matching, return)
    public List<Map<String, String>> scanItems(String tableName, String valueOfcolumn1, String valueOfcolumn2){

        Args.notEmpty(tableName, "tableName");
        Args.notEmpty(valueOfcolumn1, "column1");
        Args.notEmpty(valueOfcolumn2, "column2");

        TableDescription tableDes = dynamoDBClient.describeTable(new DescribeTableRequest(tableName)).getTable();
        if (!TableStatus.ACTIVE.toString().equals(tableDes.getTableStatus())){
            throw new RuntimeException(String.format("Table [%s] not exists.", tableName));
        }

        List<Map<String, String>> lst = Lists.newArrayList();

        Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":val1", new AttributeValue().withS(valueOfcolumn1));
        eav.put(":val2", new AttributeValue().withS(valueOfcolumn2));

        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableName)
                .withFilterExpression("column1 = :val1 and column2 = :val2")
                .withExpressionAttributeValues(eav);

        ScanResult result = dynamoDBClient.scan(scanRequest);
        for (Map<String, AttributeValue> item : result.getItems()) {

            Map<String, String> tempMap = Maps.newHashMap();

            for(Map.Entry<String, AttributeValue> entry: item.entrySet()){
                tempMap.put(entry.getKey(), entry.getValue().getS());
            }

            lst.add(tempMap);
        }

        return lst;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // STATIC PART
    private static DynamoDBHandler dynDbHandler;

    public static DynamoDBHandler getInstance(){

        if (dynDbHandler == null){
            dynDbHandler = new DynamoDBHandler();
        }

        return dynDbHandler;
    }
}
