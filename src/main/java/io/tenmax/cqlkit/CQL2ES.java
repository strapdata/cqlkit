package io.tenmax.cqlkit;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Row;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class CQL2ES extends CQL2JSON {

    String index;
    String type;
     
    public static void main(String[] args) {
        CQL2ES cqlMapper = new CQL2ES();
        cqlMapper.start(args);
    }

    @Override
    protected void prepareOptions(Options options) {
        super.prepareOptions(options);
        options.addOption( "i", "index", true, "The target elasticsearch index name." );
        options.addOption( "t", "type", true, "The target elasticsearch document type." );
    }
    
    @Override
    protected String map(Row row) {
        String doc = super.map(row);
        
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("_index", commandLine.getOptionValue("i") );
        jsonObject.addProperty("_type", commandLine.getOptionValue("t") );
        jsonObject.addProperty("_id",  buildElasticId(row));
        
        JsonObject indexObject = new JsonObject();
        indexObject.add("index",  jsonObject);
        return gson.toJson(indexObject)+System.getProperty("line.separator")+doc;
    }
    
    @Override
    protected  void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax =
                "cql2es [-c contactpoint] [-q query] [FILE]";
        String header = "File       The file to use as CQL query. If both FILE and QUERY are \n" +
                "           omitted, query will be read from STDIN.\n\n";
        formatter.printHelp(cmdLineSyntax, header, options, null);
        System.exit(0);
    }
    
    @Override
    protected void printVersion() {
        System.out.println("cql2es version " + Consts.VERSION);
        System.exit(0);
    }
    
    @Override
    protected String fileExtension() {
        return "es";
    }
    
    public String buildElasticId(Row row) {
        JsonArray jsonArray = new JsonArray();
        for(ColumnMetadata cm : this.primaryKey) {
            JsonElement jsonValue = RowUtils.toJson(cm.getType(), row.getObject(cm.getName()), false);
            jsonArray.add(jsonValue);
        }
        return gson.toJson(jsonArray);           
    }
}
