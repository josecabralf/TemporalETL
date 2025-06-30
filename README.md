# TemporalETL

A robust ETL (Extract, Transform, Load) framework built on [Temporal](https://temporal.io/) for processing Launchpad data. This project provides a scalable, fault-tolerant solution for extracting events from Ubuntu's Launchpad platform and transforming them into standardized event records for analytics and reporting.

**Latest Updates (June 2025):**
- ✨ **Decorator-based Architecture**: Auto-discovery and registration of query types and extraction methods This simplifies the addition of new data sources by using `@extract_method()` and `@query_type()` decorators
- 📦 **Modular Worker System**: Unified `ETLWorker` class for simplified worker management
- 🛡️ **Enhanced Database Layer**: Thread-safe singleton with automatic schema creation and improved connection pooling

## 📚 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [Class Diagram](#-class-diagram)
- [Features](#-features)
- [Prerequisites](#-prerequisites)
- [Installation](#-installation)
- [Usage](#-usage)
- [Configuration](#-configuration)
- [Database Schema](#-database-schema)
- [Extending the System](#-extending-the-system)
- [Additional Resources](#-additional-resources)

## 🏗️ Architecture Overview

TemporalETL uses Temporal workflows to orchestrate reliable ETL pipelines that can handle failures, retries, and long-running operations gracefully. The system is built around a modular, decorator-based architecture that enables automatic discovery and registration of components:

- **ETL Flow System**: Concrete `ETLFlow` implementation that orchestrates extract, transform, and load operations through Temporal activities
- **Query System**: Flexible query abstraction with `Query` base class and decorator-based registration via `@query_type()`. `QueryFactory` provides auto-discovery and dynamic instantiation of query types
- **Extract Method Factory**: `ExtractMethodFactory` with decorator-based registration via `@extract_method()` automatically discovers and loads extraction methods from flow directories
- **Input Standardization**: `FlowInput` provides consistent parameter handling across different workflow types
- **ETL Worker Management**: `ETLWorker` class manages Temporal worker lifecycle with automatic workflow and activity registration
- **Activity Functions**: Discrete units of work (extract, transform, load) that can be independently scaled and retried
- **Database Layer**: Thread-safe singleton PostgreSQL database with connection pooling, batch processing, and automatic schema creation

The system extracts data from Launchpad APIs (bugs, merge proposals, questions), transforms it into standardized event formats, and loads it into a PostgreSQL database for analytics.

## 📊 Class Diagram

```mermaid
---
config:
  theme: neo
  look: neo
  layout: dagre
---
classDiagram
direction LR

    FlowInput --> ETLFlow : receives

    ETLWorker --> ETLFlow : registers

    ETLFlow --> ExtractMethodFactory : uses
    ExtractMethodFactory ..> extract_data : creates
    extract_data <|.. LaunchpadBugs
    extract_data <|.. LaunchpadMergeProposals
    extract_data <|.. LaunchpadQuestions

    ETLFlow --> QueryFactory : uses
    ETLFlow ..> Database : inserts
    QueryFactory ..> Query : creates
    Query <|.. LaunchpadQuery

    Database ..> Event : stores

    class ETLWorker {
        +client: Client
        +worker: Worker
        +get_worker() Worker
        +run() void
    }

    class FlowInput {
        +query_type: str
        +args: Dict[str, Any]
    }

    class ETLFlow {
        +queue_name: str
        +BATCH_SIZE: int
        +run(input: FlowInput) Dict[str, Any]
        +get_activities() List[Any]
    }

    class ExtractMethodFactory {
        -_modules_imported: bool
        -_discover_flow_directories() List[str]
        -_discover_and_import_modules() void
        +create(extract_cmd_type: str) Callable
        +get_registered_types() List[str]
    }

    class QueryFactory {
        -_modules_imported: bool
        -_discover_query_directories() List[str]
        -_discover_and_import_modules() void
        +create(query_type: str, args: dict) Query
        +get_registered_types() List[str]
    }

    class Query {
        +source_kind_id: str
        +event_type: str
        +from_dict(data: dict) Query*
        +to_summary_base() dict*
    }
    <<interface>> Query

    namespace launchpad {
        class LaunchpadBugs {
            +extract_data() List[Dict[str, Any]]
        }

        class LaunchpadMergeProposals {
            +extract_data() List[Dict[str, Any]]
        }

        class LaunchpadQuestions {
            +extract_data() List[Dict[str, Any]]
        }

        class LaunchpadQuery {
            +application_name: str
            +service_root: str
            +version: str
            +member: str
            +data_date_start: str
            +data_date_end: str
            +from_dict(data: dict) LaunchpadQuery
            +to_summary_base() dict
        }
    }
    <<query_type>> LaunchpadQuery
    <<extract_method>> LaunchpadBugs
    <<extract_method>> LaunchpadMergeProposals
    <<extract_method>> LaunchpadQuestions

    class Event {
        +id: Optional[int]
        +source_kind_id: str
        +parent_item_id: str
        +event_id: str
        +event_type: str
        +relation_type: str
        +employee_id: str
        +event_time_utc: str
        +week: Optional[str]
        +timezone: Optional[str]
        +event_time: Optional[str]
        +event_properties: Optional[Dict]
        +relation_properties: Optional[Dict]
        +metrics: Optional[Dict]
    }

    class Database {
        -_instance: Database
        -_lock: Lock
        -_initialized: bool
        +insert_events_batch(events: List[Event]) int
        +get_connection() Connection
        +health_check() bool
        +get_pool_status() dict
        +_create_schema() void
    }
```


## 🚀 Features

- **Temporal-based Orchestration**: Leverages Temporal for reliable workflow execution with automatic retries and error handling
- **Multi-Source Data Extraction**: Support for Launchpad bugs, merge proposals, and questions with decorator-based pluggable extraction methods
- **ETL Pipeline Workflows**: Specialized `ETLFlow` implementation for Extract, Transform, Load operations with batch processing
- **Fault Tolerance**: Built-in resilience against network failures, API rate limits, and transient errors with exponential backoff
- **Scalable Processing**: Support for parallel workflow execution across multiple workers with configurable batch sizes
- **Event Standardization**: Transforms diverse Launchpad data into standardized event records with rich metadata
- **PostgreSQL Integration**: Production-ready database storage with connection pooling, thread-safety, batch operations, and automatic schema creation
- **Auto-Discovery Architecture**: Decorator-based registration system with automatic discovery of query types and extraction methods
- **Thread-Safe Database**: Singleton database manager with connection pooling and health monitoring
- **Comprehensive Logging**: Structured logging throughout the system for observability and debugging

## 📋 Prerequisites

- Python 3.8+
- Docker and Docker Compose
- PostgreSQL (via Docker containers)
- Launchpad API credentials (for production usage)

### Key Python Dependencies
- **temporalio**: Temporal workflow SDK for Python (v1.12.0)
- **psycopg2-binary**: PostgreSQL database adapter (v2.9.10)
- **launchpadlib**: Ubuntu Launchpad API client library (v2.1.0)
- **pytz**: Timezone handling for event processing (v2025.2)
- **python-dotenv**: Environment variable management (v1.1.0)

## 🛠️ Installation

### 1. Clone the Repository
```bash
git clone https://github.com/your-username/TemporalETL.git
cd TemporalETL
```

### 2. Set Up Python Environment
```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Start Temporal Infrastructure
```bash
# Start all services (PostgreSQL databases, Temporal Server, and Web UI)
docker-compose up -d

# Wait for services to be healthy
docker-compose ps
```

The infrastructure includes:
- **temporal-db**: PostgreSQL database for Temporal server (port 5432)
- **workflows-db**: PostgreSQL database for application data (port 7000)
- **temporal**: Temporal server with auto-setup (port 7233)
- **temporal-ui**: Temporal Web UI (port 8080)

### 4. Verify Installation
```bash
# Check that all containers are running
docker-compose ps

# Access Temporal Web UI
# Open http://localhost:8080 in your browser

# Start a worker in one terminal
python worker.py

# In another terminal, test with the bugs workflow
python run_bugs_wf.py
```

**Available Scripts:**
- `worker.py`: Generic ETL worker that handles all workflow types
- `run_bugs_wf.py`: Example workflow runner for Launchpad bugs data
- `run_launchpad_queuer.py`: Workflow queuer for batch processing

**Exploring Registered Components:**
```python
# See all registered query types
from models.query import QueryFactory
print("Query types:", QueryFactory.get_registered_types())

# See all registered extract methods  
from models.extract_cmd import ExtractMethodFactory
print("Extract methods:", ExtractMethodFactory.get_registered_types())
```

## 🚀 Usage

### Programmatic Usage

```python
import asyncio
from temporalio.client import Client
from models.flow_input import FlowInput
from models.etl_flow import ETLFlow

async def run_etl_workflow():
    # Connect to Temporal
    client = await Client.connect("localhost:7233")
    
    # Create input with query parameters
    flow_input = FlowInput(
        query_type="launchpad",  # Uses @query_type("launchpad") registered LaunchpadQuery
        args={
            "application_name": "my-launchpad-app",
            "service_root": "production", 
            "version": "devel",
            "member": "ubuntu-username",
            "data_date_start": "2024-01-01",
            "data_date_end": "2024-03-31",

            "source_kind_id": "launchpad",
            "event_type": "bugs"  # Routes to @extract_method("launchpad-bugs")
        }
    )

    # Start workflow
    handle = await client.start_workflow(
        workflow=ETLFlow.run,
        args=(flow_input,),
        id=f"etl-{flow_input.args['member']}-{flow_input.args['data_date_start']}",
        task_queue=ETLFlow.queue_name,
    )
    
    # Wait for completion and get result
    result = await handle.result()
    print(f"Processed {result.get('items_processed', 0)} events")
    print(f"Inserted {result.get('items_inserted', 0)} events into database")

# Run the workflow
asyncio.run(run_etl_workflow())
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file in the root directory or set these environment variables:

```bash
# Temporal Configuration
TEMPORAL_HOST=localhost:7233

# Launchpad API Configuration
LP_APP_ID=your-launchpad-application-id
LP_WEB_ROOT=production
LP_API_VERSION=devel
FROM_DATE=2024-01-01
TO_DATE=2024-03-31

# PostgreSQL Database Configuration
DB_HOST=localhost
DB_PORT=7000
DB_NAME=workflows-db
DB_USER=workflows-db
DB_PASSWORD=workflows-db

DB_MIN_CONN=1
DB_MAX_CONN=20

EVENTS_TABLE=events
```

### Docker Services Configuration

The `docker-compose.yml` defines the complete infrastructure:

- **temporal-db** (`localhost:5432`): PostgreSQL database for Temporal server
- **workflows-db** (`localhost:7000`): PostgreSQL database for application events
- **temporal** (`localhost:7233`): Temporal server with auto-setup
- **temporal-ui** (`localhost:8080`): Web interface for workflow monitoring

### Temporal Server Configuration

Production-ready configurations in `temporal-config/`:

- **`config.yaml`**: Core Temporal server settings with PostgreSQL persistence
- **`development-sql.yaml`**: Development-specific dynamic configuration
- **`log_config.yaml`**: Logging configuration for different environments

### Supported Event Types

The system currently supports these Launchpad data sources (automatically registered via decorators):
- **`launchpad-bugs`**: Bug reports and their activities (`@extract_method("launchpad-bugs")`)
- **`launchpad-merge_proposals`**: Code merge proposals and reviews (`@extract_method("launchpad-merge_proposals")`)
- **`launchpad-questions`**: Questions and answers from Launchpad (`@extract_method("launchpad-questions")`)

New event types can be added by creating new extraction methods and registering them with the `@extract_method("custom-name")` decorator. The system will automatically discover and register them.

## 🗄️ Database Schema

The application uses PostgreSQL with a comprehensive events table that supports rich event metadata. The database schema is automatically created during initialization:

### Events Table Structure
```sql
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    source_kind_id VARCHAR NOT NULL,
    parent_item_id VARCHAR,
    event_id VARCHAR NOT NULL UNIQUE,

    event_type VARCHAR NOT NULL,
    relation_type VARCHAR NOT NULL,

    employee_id VARCHAR NOT NULL,

    event_time_utc TIMESTAMP NOT NULL,
    week DATE NOT NULL,
    timezone VARCHAR,
    event_time TIMESTAMP,

    event_properties JSONB,
    relation_properties JSONB,
    metrics JSONB
);
```

### Field Descriptions
- **`id`**: Auto-incrementing primary key
- **`source_kind_id`**: Data source identifier (e.g., "launchpad")
- **`parent_item_id`**: Parent entity ID (bug ID, merge proposal ID, etc.)
- **`event_id`**: Unique event identifier across all sources
- **`event_type`**: Type of event (e.g., "bugs", "merge_proposals", "questions")
- **`relation_type`**: Specific action type (e.g., "created", "approved", "answered")
- **`employee_id`**: Unique identifier for the user/developer
- **`event_time_utc`**: Event timestamp in UTC
- **`week`**: Monday date of the week when event occurred
- **`timezone`**: User's timezone
- **`event_time`**: Event timestamp in user's timezone
- **`event_properties`**: JSONB field for event-specific metadata
- **`relation_properties`**: JSONB field for action-specific metadata
- **`metrics`**: JSONB field for quantitative measurements

### Connection Pool Configuration
The database layer uses PostgreSQL connection pooling for optimal performance:

- **Thread-safe singleton pattern** with double-checked locking for initialization
- **Configurable pool size** via environment variables (`DB_MIN_CONN`, `DB_MAX_CONN`)
- **Health checks** with automatic connection recovery
- **Batch processing** for efficient bulk inserts with configurable batch sizes
- **Exponential backoff** for retry logic on connection failures
- **Automatic schema creation** during initialization

## 🔄 Extending the System

### Adding New Data Sources

1. **Create extraction method with decorator**:
```python
# In source/flows/custom.py
from typing import List, Dict, Any
from source.query import SourceQuery
from models.extract_cmd import extract_method

@extract_method(name="source-custom")  # Auto-registered via decorator
async def extract_data(query: SourceQuery) -> List[Dict[str, Any]]:
    """Extract data from your custom data source"""
    # Implementation here
    events = []
    # ... extract and process data ...
    return events
```

2. **The method is automatically registered** - no manual registration needed! The `ExtractMethodFactory` will auto-discover and register it.

3. **Use the new extraction method**:
```python
# In your workflow runner
flow_input = FlowInput(
    query_type="source",  # Must match your query type
    args={
        "endpoint": "https://api.custom-source.com",
        "api_key": "your-api-key", 
        "date_range": "2024-01-01:2024-03-31",
        "source_kind_id": "source",
        "event_type": "custom"  # This will route to source-custom
    }
)
```

### Adding New Query Types

1. **Create a new query class with decorator**:
```python
# in source/query.py
from models.query import Query, query_type

@query_type("source")  # Auto-registered via decorator
class SourceQuery(Query):
    def __init__(self, endpoint: str, api_key: str, date_range: str, 
                 source_kind_id: str, event_type: str):
        self.endpoint = endpoint
        self.api_key = api_key
        self.date_range = date_range
        super().__init__(source_kind_id, event_type)
    
    @staticmethod
    def from_dict(data: dict):
        return SourceQuery(
            endpoint=data.get("endpoint", ""),
            api_key=data.get("api_key", ""),
            date_range=data.get("date_range", ""),
            source_kind_id=data.get("source_kind_id", ""),
            event_type=data.get("event_type", "")
        )
    
    def to_summary_base(self) -> dict:
        return {
            "endpoint": self.endpoint,
            "date_range": self.date_range
        }
```

2. **The query type is automatically registered** - no manual registration needed! The `QueryFactory` will auto-discover and register it when the module is imported.

### Best Practices for Extensions

- **Error Handling**: Use Temporal's retry policies and handle transient failures gracefully
- **Rate Limiting**: Implement appropriate delays for API calls to avoid rate limits  
- **Idempotency**: Ensure activities can be safely retried without side effects
- **Batch Processing**: Use the configurable `ETLFlow.BATCH_SIZE` for efficient database operations
- **Logging**: Use structured logging for better observability
- **Schema Validation**: Validate event data structure before database insertion
- **Connection Pooling**: Leverage the existing PostgreSQL connection pool for database operations
- **Decorator Registration**: Use `@query_type()` and `@extract_method()` decorators for automatic component discovery
- **Module Organization**: Place extraction methods in `flows/` subdirectories and query classes in `query.py` files for auto-discovery

## 📚 Additional Resources
- **[Temporal Documentation](https://docs.temporal.io/)** - Complete guide to Temporal workflows and activities
- **[Temporal Python SDK](https://python.temporal.io/)** - Python-specific Temporal development guide
- **[Launchpad API Documentation](https://help.launchpad.net/API)** - Launchpad REST API reference and launchpadlib usage
- **[PostgreSQL Documentation](https://www.postgresql.org/docs/)** - PostgreSQL database administration and SQL reference
- **[Docker Compose Reference](https://docs.docker.com/compose/)** - Container orchestration documentation
- **[Python AsyncIO Guide](https://docs.python.org/3/library/asyncio.html)** - Asynchronous programming patterns in Python