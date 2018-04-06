# Dumpy
### Dumpy is an [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) framework.  
Dumpy is composed of Extractor and Loader, the user is free to define where to handle Transform part of the ETL, either in the Extractor class or the Loader class

## Instructions

#### Extractor

define a class that implements IExtractor, override it's ```get``` method, define the loading, and if you want transform process, and return a call to ```createResponse``` method from the parent class with a List<Entity> the next cursor in the extraction loop, and if it has next:

```java

public class ExampleExtractor implements IExtractor<Map<String, Object>, Exception> {

    @Override
    public Response<Map<String, Object>> get(String cursor) throws Exception {
        //extract and transform process, in this example we are extracting to a Map<String, Object>
        List<Entity<Map<String, Object>>> entities = new LinkedList<>();	
	//add your extracted and transformed results to the entity list and return calling createResponse
        entities.add(new Entity<Map<String, Object>>() {
            @Override
            public String getId() {
                //return here the identifier of the extracted result
            }

            @Override
            public Map<String, Object> getValue() {
                return 
            }
        });
    return createResponse(entities, nextCursor, true)
    }

}

``` 


#### Loader
define a class that implements ILoader, override it's ```load``` method, which receives the extracted entities 

```java

public class ExampleLoader implements ILoader<Map<String, Object>> {
   
    @Override
    public Status load(List<? extends IExtractor.Entity<Map<String, Object>>> entities) {
     //define the loading proccess here    
    }
}
``` 


