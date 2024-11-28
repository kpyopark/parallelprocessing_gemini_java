import com.google.cloud.bigquery.*;
import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.generativeai.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.util.stream.Collectors;

public class GeminiParallelProcessor {
    private final String projectId;
    private final String location;
    private final String inputQuery;
    private final String outputTable;
    private final String resultTable;
    private final int batchSize;
    private final int processSize;
    private final int maxWorkers;
    
    private final BigQuery bigquery;
    private final GenerativeModel model;
    private final Logger logger;
    private final ObjectMapper objectMapper;
    
    public GeminiParallelProcessor(
            String projectId,
            String location,
            String inputQuery,
            String outputTable,
            String resultTable,
            int batchSize,
            int processSize,
            int maxWorkers
    ) {
        this.projectId = projectId;
        this.location = location;
        this.inputQuery = inputQuery;
        this.outputTable = outputTable;
        this.resultTable = resultTable;
        this.batchSize = batchSize;
        this.processSize = processSize;
        this.maxWorkers = maxWorkers;
        
        // Initialize clients and configurations
        this.bigquery = BigQueryOptions.getDefaultInstance().getService();
        this.logger = Logger.getLogger(GeminiParallelProcessor.class.getName());
        this.objectMapper = new ObjectMapper();
        
        // Initialize Vertex AI and Gemini model
        VertexAI vertexAI = new VertexAI(projectId, location);
        GenerationConfig generationConfig = GenerationConfig.newBuilder()
                .setMaxOutputTokens(8192)
                .setTemperature(0.3f)
                .setTopP(0.95f)
                .build();
                
        this.model = new GenerativeModel.Builder()
                .setModelName("gemini-1.5-flash-002")
                .setGenerationConfig(generationConfig)
                .build(vertexAI);
        
        // Create result table
        createResultTable();
    }
    
    private void createResultTable() {
        try {
            Schema schema = Schema.of(
                Field.of("batch_id", StandardSQLTypeName.STRING),
                Field.of("process_start_time", StandardSQLTypeName.TIMESTAMP),
                Field.of("process_end_time", StandardSQLTypeName.TIMESTAMP),
                Field.of("processed_count", StandardSQLTypeName.INT64),
                Field.of("success_count", StandardSQLTypeName.INT64),
                Field.of("error_count", StandardSQLTypeName.INT64),
                Field.of("status", StandardSQLTypeName.STRING)
            );
            
            TableId tableId = TableId.of(projectId, resultTable);
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
            
            bigquery.create(tableInfo);
            logger.info("Result table created successfully");
        } catch (BigQueryException e) {
            logger.info("Result table already exists or error: " + e.getMessage());
        }
    }
    
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseGeminiResponse(String jsonStr) {
        try {
            String cleanJson = jsonStr.substring(
                jsonStr.indexOf("```json") + 7,
                jsonStr.lastIndexOf("```")
            ).trim();
            return objectMapper.readValue(cleanJson, Map.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse Gemini response", e);
        }
    }
    
    private Map<String, Object> processSingleRequest(Map<String, Object> row) {
        Map<String, Object> result = new HashMap<>();
        try {
            List<String> prompts = new ArrayList<>();
            prompts.add((String) row.get("input_text"));
            
            GenerateContentResponse response = model.generateContent(prompts);
            String responseText = response.getText();
            
            result.put("id", row.get("id"));
            result.put("input_text", row.get("input_text"));
            result.put("output_text", responseText);
            result.put("status", "success");
            result.put("error", null);
            result.put("processed_at", LocalDateTime.now());
            
        } catch (Exception e) {
            result.put("id", row.get("id"));
            result.put("input_text", row.get("input_text"));
            result.put("output_text", null);
            result.put("status", "error");
            result.put("error", e.getMessage());
            result.put("processed_at", LocalDateTime.now());
        }
        return result;
    }
    
    private void saveResultsToBQ(List<Map<String, Object>> results, String batchId) {
        try {
            List<InsertAllRequest.RowToInsert> rows = results.stream()
                .map(result -> InsertAllRequest.RowToInsert.of(result))
                .collect(Collectors.toList());
            
            TableId tableId = TableId.of(projectId, outputTable);
            InsertAllRequest insertRequest = InsertAllRequest.newBuilder(tableId)
                .setRows(rows)
                .build();
            
            InsertAllResponse response = bigquery.insertAll(insertRequest);
            if (response.hasErrors()) {
                logger.warning("Errors occurred while inserting rows: " + response.getInsertErrors());
            }
        } catch (Exception e) {
            logger.severe("Failed to save results to BigQuery: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
    
    private void saveBatchStatus(
            String batchId,
            LocalDateTime startTime,
            LocalDateTime endTime,
            int processedCount,
            int successCount,
            int errorCount
    ) {
        String query = String.format(
            "INSERT INTO `%s.%s` "
            + "(batch_id, process_start_time, process_end_time, processed_count, "
            + "success_count, error_count, status) "
            + "VALUES ('%s', TIMESTAMP('%s'), TIMESTAMP('%s'), %d, %d, %d, 'COMPLETED')",
            projectId, resultTable, batchId, startTime, endTime,
            processedCount, successCount, errorCount
        );
        
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        try {
            bigquery.query(queryConfig);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to save batch status", e);
        }
    }
    
    public void processBatch() {
        int offset = 0;
        while (true) {
            String query = String.format("%s LIMIT %d OFFSET %d",
                    inputQuery, batchSize, offset);
            
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
            try {
                TableResult result = bigquery.query(queryConfig);
                if (!result.hasNext()) {
                    break;
                }
                
                String batchId = String.format("batch_%s_%d",
                        LocalDateTime.now().toString().replace(":", ""), offset);
                LocalDateTime startTime = LocalDateTime.now();
                
                List<Map<String, Object>> allRows = new ArrayList<>();
                result.iterateAll().forEach(row -> {
                    Map<String, Object> rowMap = new HashMap<>();
                    row.getSchema().getFields().forEach(field -> 
                        rowMap.put(field.getName(), row.get(field.getName()).getValue())
                    );
                    allRows.add(rowMap);
                });
                
                ExecutorService executor = Executors.newFixedThreadPool(maxWorkers);
                List<Future<Map<String, Object>>> futures = new ArrayList<>();
                
                for (int i = 0; i < allRows.size(); i += processSize) {
                    List<Map<String, Object>> chunk = allRows.subList(i,
                            Math.min(i + processSize, allRows.size()));
                    
                    for (Map<String, Object> row : chunk) {
                        futures.add(executor.submit(() -> processSingleRequest(row)));
                    }
                }
                
                List<Map<String, Object>> results = new ArrayList<>();
                int successCount = 0;
                int errorCount = 0;
                
                for (Future<Map<String, Object>> future : futures) {
                    try {
                        Map<String, Object> result = future.get();
                        results.add(result);
                        if ("success".equals(result.get("status"))) {
                            successCount++;
                        } else {
                            errorCount++;
                        }
                    } catch (Exception e) {
                        logger.severe("Error processing request: " + e.getMessage());
                        errorCount++;
                    }
                }
                
                executor.shutdown();
                saveResultsToBQ(results, batchId);
                
                LocalDateTime endTime = LocalDateTime.now();
                saveBatchStatus(batchId, startTime, endTime,
                        allRows.size(), successCount, errorCount);
                
                logger.info(String.format("Processed %d items in batch %s",
                        results.size(), batchId));
                
                offset += batchSize;
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Processing interrupted", e);
            }
        }
    }
    
    public static void main(String[] args) {
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