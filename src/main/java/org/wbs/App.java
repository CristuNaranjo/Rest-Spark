package org.wbs;

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

        ld.main();
        linre.start();

        List<String> filenames = ld.getFilenames();
        for(String filename : filenames){
            linre.makePrediction(filename);
        }
        linre.stop();


//        get("/pred", (request, response) -> {
//            ld.main();
//            linre.start();
//
//            List<String> filenames = ld.getFilenames();
//            for(String filename : filenames){
//                linre.makePrediction(filename);
//            }
//            linre.stop();
//            return "Done!!";
//        });
    }
}
