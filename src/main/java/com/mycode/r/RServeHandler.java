package com.mycode.r;

import com.mycode.amazon.DynamoDBHandler;
import org.apache.http.util.Args;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

/*
Rserve doesn't provide any "client". I put it here for reference anyway.
 */

public class RServeHandler {

    private static final String CLASS_NAME = RServeHandler.class.getSimpleName();
    private static final Logger LOGGER = LogManager.getLogger(CLASS_NAME);

    private static String sendCommandToRServer(String[] commands) throws RserveException, REXPMismatchException {

        Args.notNull(commands, "commands");

        String res = null;
        RConnection conn = null;

        try{

            conn = new RConnection("R_SERVER_HOST", Integer.valueOf("R_SERVER_PORT"));

            if (!conn.isConnected())
                throw new RuntimeException(String.format("Failed to connect to R Server [%s:%s]",
                        "R_SERVER_HOST", "R_SERVER_PORT"));

            REXP resFromRServer = null;

            for(String command: commands){
                if (command != commands[commands.length-1]){
                    LOGGER.info(String.format("Execute voidEval(): [%s]", command));
                    conn.voidEval(command);
                } else {
                    LOGGER.info(String.format("Execute eval(): [%s]", command));
                    resFromRServer = conn.eval(command);
                }
            }

            res = resFromRServer.asString();
        }
        finally {
            if (conn != null){  // Important, b/c otherwise, there would be a lot of Rserve instances in Linux Server.
                conn.close();
            }
        }

        return res;
    }

}
