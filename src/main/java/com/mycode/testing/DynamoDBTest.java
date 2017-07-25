package com.mycode.testing;

import com.mycode.amazon.DynamoDBHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.util.Args;

import java.util.*;
import java.util.stream.Collectors;

public class DynamoDBTest {

    /*
    *** Context ***
    Supposed we have a DynamoDB table called "tableName". And its PrimaryKey is "id". (PrimaryKey = uniquely identify a row)
    We are interested in a series of Ids (arrIds).
    We want to get back the rows (documents) corresponding to those Ids.
    *** Func call ***
    testGetDocumentsBasedOnIds(new String[]{"12322", "32234"})
    Return a list of documents having the ids specified above.
     */
    public static List<Map<String, String>> testGetDocumentsBasedOnIds(String[] arrIds){

        Args.notNull(arrIds, "arrIds");
        Args.check(arrIds.length > 0, "Input Ids must be at least one");

        Set<String> uniqueIds = new HashSet<String>(Arrays.asList(arrIds));

        if (uniqueIds.size() != arrIds.length){
            throw new RuntimeException("Input Ids are not unique.");
        }

        if (Arrays.asList(arrIds).stream().map(x -> StringUtils.isEmpty(x)).collect(Collectors.toList()).size() > 0){
            throw new RuntimeException("There is at least one empty id.");
        }

        List<Map<String, String>> lst = DynamoDBHandler.getInstance().getItems(
                "tableName", "id", Arrays.asList(arrIds));

        if (lst.size() != arrIds.length){
            throw new RuntimeException("Not all Ids are found.");
        }

        return lst;
    }
}
