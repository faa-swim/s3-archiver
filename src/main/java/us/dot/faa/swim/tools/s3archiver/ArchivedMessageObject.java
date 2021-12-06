package us.dot.faa.swim.tools.s3archiver;

import java.util.HashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ArchivedMessageObject {
    
    private HashMap<String,Object> properties;
    private String message;    

    public ArchivedMessageObject(String rawJsonObject) throws JsonMappingException, JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode rootNode = objectMapper.readTree(rawJsonObject);
        setMessageFromHex(rootNode.path("message").asText());
        this.properties = objectMapper.convertValue(rootNode.path("properties"), new TypeReference<HashMap<String, Object>>(){});     
    }

    public HashMap<String,Object> getProperties(){
        return this.properties;
    }

    public String getPropertyValue(String propertyKey){
        return this.properties.get(propertyKey).toString();
    }

    private void setMessageFromHex(String message){        
        this.message = hexToString(message); 
    }

    public String getMessage()
    {
        return this.message;
    }

    private String hexToString(String toConvert){
        String result = new String();
        char[] charArray = toConvert.toCharArray();
        for(int i = 0; i < charArray.length; i=i+2) {
           String st = ""+charArray[i]+""+charArray[i+1];
           char ch = (char)Integer.parseInt(st, 16);
           result = result + ch;        
        }
        return result;
    }
}
