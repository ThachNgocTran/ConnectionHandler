package com.mycode.testing;

import org.apache.http.util.Args;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

public class RserveTest {

    /*
    *** Context ***
    Supposed we have a standard R, with package Rserve installed. (https://www.rforge.net/Rserve/)
    We can start the R server by invoking Rserve()
    Better to run the Rserve in Linux due to Linux supporting "fork" command.
    Now we connect to the R server, each connection will create the same environment as the R when it was first started. (forking)
    The commands can be 10 or 20... But only the last command has its result fetched back. The remaining commands are simply executed.
    *** Func call ***
    testSendCommandToRServer(new String[] {"myresult = 1 + 1", "toJSON(myresult)"})
    Return a JSON string... (myresult can contain even a data.frame!)
     */
    public static String testSendCommandToRServer(String[] commands) throws RserveException, REXPMismatchException {

        Args.notNull(commands, "commands");
        Args.check(commands.length > 0, "Number of commands must be at least one.");

        RConnection conn = null;
        String res = null;

        try{

            conn = new RConnection("R_SERVER_HOST", 6311);

            if (!conn.isConnected())
                throw new RuntimeException(String.format("Failed to connect to R Server [%s:%s]",
                        "R_SERVER_HOST", "6311"));

            REXP resFromRServer = null;
            for(String command: commands){
                if (command != commands[commands.length-1]){
                    conn.voidEval(command); // voidEval prevents fetching the result ==> save bandwidth.
                } else {
                    resFromRServer = conn.eval(command);    // the last command fetches the result.
                }
            }

            res = resFromRServer.asString();
        }
        finally {
            if (conn != null){  // IMPORTANT, b/c otherwise, there would be a lot of Rserve instances in Linux. (TESTED!!!)
                conn.close();
            }
        }

        return res;
    }
}
