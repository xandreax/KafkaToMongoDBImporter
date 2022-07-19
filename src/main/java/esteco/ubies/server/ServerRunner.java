package esteco.ubies.server;

import esteco.ubies.importer.MongoDBImporter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static spark.Spark.*;

/*
 * Rest api to communicate with frontend of the project:
 *
 * with the launch of the application start by default the Spark server (Jetty)
 * */

public class ServerRunner {
    private static MongoDBImporter importer;

    private static Thread t;

    public static void main(String[] args) {

        String propertiesFilePath = "/mongodbImporter.properties";
        Properties properties = new Properties();
        try {
            //http://stackoverflow.com/questions/29070109/how-to-read-properties-file-inside-jar
            InputStream in = MongoDBImporter.class.getResourceAsStream(propertiesFilePath);
            properties.load(in);
        } catch (IOException e) {
            System.out.println("Unable to load {}" + propertiesFilePath + e);
            System.exit(0);
        }

        /* *
         * client send options request before post and get
         * */
        options("/*", (request,response)->{
            String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
            if (accessControlRequestHeaders != null) {
                response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
            }
            String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
            if(accessControlRequestMethod != null){
                response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
            }
            return "OK";
        });

        before("/*", (request, response) -> response.header("Access-Control-Allow-Origin", "*"));

        /* *
         * Client send the name of the registration to import
         * -> create the collection and start the importer (if collection name is valid)
         * */
        post("/start", (request, response) -> {
            String uuid_registration = request.body().replace(" ", "");
            importer = new MongoDBImporter(properties, uuid_registration);
            t = new Thread(importer);
            t.start();
            response.status(200);
            return "Registration started";
        });

        /* *
         * Client send the message to stop importing data -> Signal to stop the importer
         * */
        post("/stop", (request, response) -> {
            importer.stopImporter();
            response.status(200);
            return "Registration stopped";
        });

        /* *
         * client check if importer thread is still running or not
         * */
        get("/status", (request, response) -> {
            if(t == null)
                return false;
            else
                return t.isAlive();
        });
    }
}
