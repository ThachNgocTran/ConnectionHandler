package com.mycode.testing;

import com.mycode.amazon.AmazonS3Handler;
import org.apache.http.util.Args;

public class AmazonS3Test {

    /*
    *** Context ***
    We want to store a piece of data (can be a string, or a real file as long as they are serialized into a series of bytes).
    *** Func call ***
    storeFile("Any String you want".getBytes())
    Return a UUID that uniquely identifies the file stored in Amazon S3.
    This UUID is required to acquire the file again in the future.
     */
    public static String storeFile(byte[] someData){

        Args.notNull(someData, "someData");
        Args.check(someData.length > 0, "Data must be greater than zero in length.");

        String uuid = AmazonS3Handler.getInstance().storeFile(someData);

        return uuid;
    }
}
