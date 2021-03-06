package org.wbs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Hello Moto!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, URISyntaxException {
        LoadDriver ld = new LoadDriver();
        LinReg linre = new LinReg();

        //Leer fichero con propiedades de la base de datos
        String dbString,user,password,dbOut,dbOutTable = null;
        FileInputStream file;
        String path = App.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
        path = path.substring(0, path.lastIndexOf("/") + 1);
        path+="prop.properties";
        Properties mainProperties = new Properties();
        file = new FileInputStream(path);
        mainProperties.load(file);
        file.close();
        dbString = mainProperties.getProperty("app.db");
        user = mainProperties.getProperty("app.user");
        password = mainProperties.getProperty("app.password");
        dbOut = mainProperties.getProperty("app.dbOut");
        dbOutTable = mainProperties.getProperty("app.dbOutTable");
        List<String> dbLoadDriver = new ArrayList<String>();
        dbLoadDriver.add(dbString);
        dbLoadDriver.add(user);
        dbLoadDriver.add(password);
        List<String> dbLinReg = new ArrayList<String>();
        dbLinReg.add(dbOut);
        dbLinReg.add(dbOutTable);
        dbLinReg.add(user);
        dbLinReg.add(password);

        ld.main(dbLoadDriver);
        linre.start();

        List<String> filenames = ld.getFilenames();
        for(String filename : filenames){
            linre.makePrediction(filename,dbLinReg);
        }
        linre.stop();
    }
}
