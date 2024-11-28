package com.elevenquest.gemini;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        String projectId = System.getenv("PROJECT_ID");
        String location = System.getenv("LOCATION");
        String datasetId = System.getenv("DATASET_ID");
        
        GeminiParallelProcessor processor = new GeminiParallelProcessor(
            projectId,
            location,
            "SELECT 1 as id, 'you should respond the users request. users request is \"How old are you?\". output format should be JSON.' as input_text",
            datasetId + ".processing_results",
            datasetId + ".batch_status",
            100000,
            10000,
            10
        );
        
        processor.processBatch();
    }
}
