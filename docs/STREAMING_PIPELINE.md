# Streaming ETL Pipeline

This document describes the streaming ETL pipeline implementation that optimizes memory usage for processing large datasets.

## Overview

The streaming ETL pipeline processes data in chunks rather than loading entire datasets into memory. This approach provides several benefits:

- **Reduced Memory Usage**: Process large datasets without running out of memory
- **Better Resource Utilization**: Consistent memory usage regardless of dataset size
- **Improved Scalability**: Handle datasets that wouldn't fit in memory with traditional approach
- **Concurrent Processing**: Process multiple chunks simultaneously for better performance

## Architecture

### Components

1. **StreamingETLFlow**: Main workflow that orchestrates the streaming pipeline
2. **StreamingConfig**: Configuration class for tuning streaming parameters
3. **Memory Monitoring**: Built-in memory usage tracking and optimization
4. **Chunk Processing**: Parallel processing of data chunks with backpressure control

### Key Features

- **Configurable Chunk Sizes**: Tune chunk sizes for extract, transform, and load operations
- **Concurrent Processing**: Process multiple chunks simultaneously with configurable limits
- **Memory Monitoring**: Track memory usage throughout the pipeline
- **Error Resilience**: Continue processing even if individual chunks fail
- **Progress Tracking**: Detailed logging and progress reporting

## Configuration

### StreamingConfig Parameters

```python
@dataclass
class StreamingConfig:
    extract_chunk_size: int = 100       # Items to extract per chunk
    transform_batch_size: int = 500     # Events to transform per batch
    load_batch_size: int = 1000         # Events to load per database batch
    max_concurrent_chunks: int = 3      # Max concurrent processing chunks
    memory_threshold_mb: int = 500      # Memory threshold for backpressure
```

### Tuning Guidelines

#### Extract Chunk Size
- **Small (50-100)**: Use for complex data extraction or limited memory
- **Medium (100-500)**: Good balance for most use cases
- **Large (500+)**: Use when extraction is lightweight and memory is abundant

#### Transform Batch Size
- **Small (200-500)**: Use for complex transformations
- **Medium (500-1000)**: Good for standard transformations
- **Large (1000+)**: Use for simple transformations with good memory

#### Load Batch Size
- **Small (500-1000)**: Use for complex database schemas or constrained environments
- **Medium (1000-2000)**: Good balance for most databases
- **Large (2000+)**: Use for high-performance databases with good connection pooling

#### Max Concurrent Chunks
- **Low (1-2)**: Use when memory is very constrained
- **Medium (3-5)**: Good balance for most systems
- **High (5+)**: Use when you have abundant CPU and memory resources

## Usage Examples

### Basic Usage

```python
from models.streaming_etl_flow import StreamingETLFlow, StreamingConfig
from models.flow_input import FlowInput

# Configure streaming parameters
config = StreamingConfig(
    extract_chunk_size=100,
    transform_batch_size=500,
    load_batch_size=1000,
    max_concurrent_chunks=3
)

# Create workflow input
flow_input = FlowInput(
    query_type="LaunchpadQuery",
    args={
        "source_kind_id": "launchpad",
        "event_type": "bugs",
        "member": "username",
        "data_date_start": "2024-01-01",
        "data_date_end": "2024-01-31"
    }
)

# Run streaming workflow
result = await client.start_workflow(
    StreamingETLFlow.run,
    args=[flow_input, config],
    id="streaming-etl-example",
    task_queue=StreamingETLFlow.queue_name
)
```

### Memory-Constrained Environment

```python
# Configuration for low-memory environments
low_memory_config = StreamingConfig(
    extract_chunk_size=50,
    transform_batch_size=200,
    load_batch_size=500,
    max_concurrent_chunks=1,  # Process one chunk at a time
    memory_threshold_mb=200
)
```

### High-Performance Environment

```python
# Configuration for high-performance systems
high_perf_config = StreamingConfig(
    extract_chunk_size=200,
    transform_batch_size=1000,
    load_batch_size=2000,
    max_concurrent_chunks=5,
    memory_threshold_mb=1000
)
```

### Data Type Specific Examples

#### Bugs Streaming
```python
flow_input = FlowInput(
    query_type="LaunchpadQuery",
    args={
        "source_kind_id": "launchpad",
        "event_type": "bugs-streaming",  # Use streaming version
        "member": "username",
        "data_date_start": "2024-01-01",
        "data_date_end": "2024-01-31"
    }
)

config = StreamingConfig(
    extract_chunk_size=100,
    transform_batch_size=500,
    load_batch_size=1000,
    max_concurrent_chunks=3
)
```

#### Questions Streaming  
```python
flow_input = FlowInput(
    query_type="LaunchpadQuery", 
    args={
        "source_kind_id": "launchpad",
        "event_type": "questions-streaming",
        "member": "username",
        "data_date_start": "2024-01-01",
        "data_date_end": "2024-01-31"
    }
)

config = StreamingConfig(
    extract_chunk_size=50,      # Smaller chunks for complex nested data
    transform_batch_size=300,
    load_batch_size=800,
    max_concurrent_chunks=2     # Conservative concurrency
)
```

#### Merge Proposals Streaming
```python
flow_input = FlowInput(
    query_type="LaunchpadQuery",
    args={
        "source_kind_id": "launchpad", 
        "event_type": "merge_proposals-streaming",
        "member": "username",
        "data_date_start": "2024-01-01",
        "data_date_end": "2024-01-31"
    }
)

config = StreamingConfig(
    extract_chunk_size=30,      # Even smaller chunks for very complex data
    transform_batch_size=200,
    load_batch_size=600, 
    max_concurrent_chunks=2
)
```

## Running the Streaming Pipeline

### Start the Worker

```bash
python run_streaming_etl.py worker
```

### Run Example Workflow

```bash
python run_streaming_etl.py example
```

### Run Specific Data Type Examples

```bash
# Run bugs streaming workflow
python run_streaming_etl.py example

# Run questions streaming workflow  
python run_streaming_etl.py questions

# Run merge proposals streaming workflow
python run_streaming_etl.py merge-proposals
```

### Compare Memory Usage

```bash
# Compare single data type (bugs)
python compare_etl_memory.py

# Compare all data types comprehensively
python compare_etl_memory.py comprehensive
```

## Memory Optimization

### Monitoring

The pipeline includes built-in memory monitoring that tracks:

- Resident Set Size (RSS)
- Virtual Memory Size (VMS)
- Memory percentage usage
- Peak memory usage
- Average memory usage

### Memory Snapshots

Memory snapshots are taken at key points:

- Extraction start/complete
- Chunking progress
- Transform batch processing
- Load batch processing

### Backpressure Control

The pipeline implements backpressure by:

- Limiting concurrent chunk processing
- Monitoring memory thresholds
- Adjusting processing speed based on memory usage
- Failing gracefully when memory limits are exceeded

## Performance Comparison

Expected improvements when using streaming pipeline:

### Memory Usage
- **50-80% reduction** in peak memory usage for large datasets
- **Consistent memory usage** regardless of dataset size
- **Better memory utilization** patterns

### Scalability
- **Process datasets 5-10x larger** than with traditional approach
- **Linear scaling** with dataset size
- **Predictable resource usage**

### Processing Time
- **Similar or better** processing time due to concurrent chunk processing
- **Better resource utilization** leading to overall system improvements
- **Reduced garbage collection** pressure

## Troubleshooting

### High Memory Usage

1. Reduce `extract_chunk_size`
2. Reduce `max_concurrent_chunks`
3. Reduce `transform_batch_size` and `load_batch_size`
4. Check for memory leaks in extract methods

### Slow Processing

1. Increase `max_concurrent_chunks`
2. Increase chunk sizes if memory allows
3. Check database performance for load operations
4. Profile extract methods for bottlenecks

### Chunk Processing Failures

1. Check individual chunk error logs
2. Reduce chunk sizes to isolate problematic data
3. Add more error handling in extract methods
4. Verify database connectivity and constraints

## Future Enhancements

### True Streaming Extraction

Currently, extract methods still load all data before chunking. Future enhancements could include:

- Generator-based extract methods
- Lazy loading from APIs
- Streaming database queries
- Progressive data fetching

### Adaptive Chunk Sizing

Dynamic adjustment of chunk sizes based on:

- Memory usage patterns
- Processing performance
- Data complexity
- System resources

### Enhanced Monitoring

Additional monitoring capabilities:

- CPU usage tracking
- Database connection monitoring
- Network I/O monitoring
- Processing time analytics

## Best Practices

1. **Start with default configuration** and tune based on your specific needs
2. **Monitor memory usage** during initial runs to optimize configuration
3. **Use appropriate chunk sizes** based on your data characteristics
4. **Test with representative datasets** before production deployment
5. **Monitor and alert** on memory usage in production
6. **Keep extract methods lightweight** to maximize streaming benefits
7. **Use connection pooling** for database operations
8. **Implement proper error handling** in all pipeline stages

## Available Streaming Extract Methods

The streaming pipeline includes optimized extract methods for all major Launchpad data types:

### 1. Bugs Streaming (`launchpad-bugs-streaming`)
- **Batch Size**: 50 bugs per batch
- **Memory Optimization**: Processes bug activities and messages in batches
- **Use Case**: Large bug datasets with extensive activity history

### 2. Questions Streaming (`launchpad-questions-streaming`)
- **Batch Size**: 20 questions per batch  
- **Memory Optimization**: Processes questions and answers in batches
- **Use Case**: Question datasets with multiple answers and comments

### 3. Merge Proposals Streaming (`launchpad-merge_proposals-streaming`)
- **Batch Size**: 15 merge proposals per batch
- **Memory Optimization**: Processes merge proposals with comments and votes in batches
- **Use Case**: Merge proposal datasets with extensive comment threads

Each streaming method includes:
- **Error Resilience**: Continues processing if individual items fail
- **Progress Logging**: Detailed progress tracking for large datasets
- **Memory Monitoring**: Built-in memory usage tracking
- **Batch Processing**: Configurable batch sizes for optimal performance

## Configuration
