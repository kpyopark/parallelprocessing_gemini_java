# Gemini Parallel Processor

A Java-based application for processing large-scale data through Google's Gemini AI model with parallel execution capabilities. This application integrates with BigQuery for data input/output and provides efficient batch processing with detailed status tracking.

## Features

- Parallel processing of requests to Gemini AI
- BigQuery integration for data input and output
- Batch processing with configurable sizes
- Detailed status tracking and error handling
- Concurrent execution with thread pool management
- JSON response parsing and processing

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- Google Cloud Platform account with:
  - BigQuery API enabled
  - Vertex AI API enabled
  - Appropriate permissions and credentials configured

## Environment Variables

The following environment variables must be set:

```bash
PROJECT_ID=your-gcp-project-id
LOCATION=your-gcp-location
DATASET_ID=your-bigquery-dataset-id
```

## Configuration

The main configuration parameters for the GeminiParallelProcessor:

| Parameter | Description | Default Value |
|-----------|-------------|---------------|
| projectId | GCP Project ID | - |
| location | GCP Location | - |
| inputQuery | BigQuery input query | - |
| outputTable | Output table for results | {datasetId}.processing_results |
| resultTable | Table for batch status | {datasetId}.batch_status |
| batchSize | Number of records per batch | 100000 |
| processSize | Records processed in parallel | 10000 |
| maxWorkers | Maximum concurrent threads | 10 |

## BigQuery Table Schemas

### Processing Results Table (outputTable)
```sql
CREATE TABLE IF NOT EXISTS `{datasetId}.processing_results` (
  id STRING,
  input_text STRING,
  output_text STRING,
  status STRING,
  error STRING,
  processed_at TIMESTAMP
)
```

### Batch Status Table (resultTable)
```sql
CREATE TABLE IF NOT EXISTS `{datasetId}.batch_status` (
  batch_id STRING,
  process_start_time TIMESTAMP,
  process_end_time TIMESTAMP,
  processed_count INT64,
  success_count INT64,
  error_count INT64,
  status STRING
)
```

## Build and Run

1. Clone the repository:
```bash
git clone [repository-url]
cd [repository-name]
```

2. Build the project:
```bash
mvn clean package
```

3. Run the application:
```bash
java -jar target/parallelproc-1.0-SNAPSHOT.jar
```

## Usage Example

```java
GeminiParallelProcessor processor = new GeminiParallelProcessor(
    projectId,
    location,
    "SELECT id, input_text FROM `dataset.input_table` WHERE processed = false",
    "dataset.processing_results",
    "dataset.batch_status",
    100000,  // batchSize
    10000,   // processSize
    10       // maxWorkers
);

processor.processBatch();
```

## Error Handling

The application handles errors at multiple levels:

1. **Individual Request Level**: Each Gemini API request is wrapped in try-catch blocks
2. **Batch Level**: Failed requests are tracked and logged
3. **BigQuery Integration**: Insert errors are captured and logged
4. **Overall Process**: Thread interruption and runtime exceptions are handled appropriately

## Monitoring and Logging

- Batch processing status is stored in the resultTable
- Java logging is implemented for tracking execution
- BigQuery error logging for data insertions
- Detailed processing statistics per batch

## Performance Considerations

- Adjust `batchSize` based on your BigQuery quota and data volume
- Modify `processSize` based on memory constraints
- Set `maxWorkers` according to available CPU resources
- Consider Gemini API quotas and rate limits

## Dependencies

```xml
<dependencies>
    <dependency>
        <groupId>com.google.cloud</groupId>
        <artifactId>google-cloud-bigquery</artifactId>
    </dependency>
    <dependency>
        <groupId>com.google.cloud</groupId>
        <artifactId>google-cloud-vertexai</artifactId>
    </dependency>
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
    </dependency>
</dependencies>
```

## Limitations

- Maximum batch size is constrained by BigQuery quotas
- Concurrent processing is limited by Gemini API quotas
- Memory usage scales with processSize * maxWorkers
- Response parsing assumes JSON format

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.