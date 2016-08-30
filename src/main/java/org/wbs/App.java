package org.wbs;

import static spark.Spark.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        LoadDriver ld = new LoadDriver();
        LinReg linre = new LinReg();


        get("/pred", (request, response) -> {
            ld.main();
            linre.start();

            List<String> filenames = ld.getFilenames();
            for(String filename : filenames){
                linre.makePrediction(filename);
            }
            linre.stop();
            return "Done!!";
        });
    }
}
