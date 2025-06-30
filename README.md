# TemporalETL

A robust ETL (Extract, Transform, Load) framework built on [Temporal](https://temporal.io/) for processing Launchpad data. This project provides a scalable, fault-tolerant solution for extracting events from Ubuntu's Launchpad platform and transforming them into standardized event records for analytics and reporting.

**Latest Updates (June 2025):**
- ✨ **Decorator-based Architecture**: Auto-discovery and registration of query types and extraction methods This simplifies the addition of new data sources by using `@extract_method()` and `@query_type()` decorators
- 📦 **Modular Worker System**: Unified `ETLWorker` class for simplified worker management
- 🛡️ **Enhanced Database Layer**: Thread-safe singleton with automatic schema creation and improved connection pooling
- 🏗️ **Automated Boilerplate Code Creation**: Use `scripts/new.py` to safely create a new source for data extraction.

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

### 🚀 Quick Start: Automated Source Creation

The fastest and safest way to extend the system is using the automated source creation script:

```bash
# Create a new source with default settings
python scripts/new.py

# Create a specific source with multiple flows
python scripts/new.py github issues pull_requests releases

# Validate and update existing sources
python scripts/new.py launchpad bugs merge_proposals
```

**The script automatically:**
- ✅ Creates proper directory structure
- ✅ Generates template files with correct imports and decorators
- ✅ Validates existing sources for required structure
- ✅ Creates backup files before making changes
- ✅ Ensures all required methods and decorators are present

**Why use the script?**
- Ensures proper imports and structure
- Validates existing code
- Creates backups before changes
- Follows consistent naming conventions
- Reduces human error

#### Generated Structure

For `python scripts/new.py github issues pull_requests`:

```
github/
├── __init__.py
├── query.py                    # GithubQuery class with @query_type("github")
└── flows/
    ├── __init__.py
    ├── issues.py              # @extract_method("github-issues")
    └── pull_requests.py       # @extract_method("github-pull_requests")
```

#### What Gets Validated

**Query Files (`{source}/query.py`):**
- Required imports: `dataclasses`, `typing`, `models.query`
- `@query_type("{source}")` decorator
- Class inherits from `Query`
- Has `from_dict()` static method
- Has `to_summary_base()` method

**Flow Files (`{source}/flows/{flow}.py`):**
- Required imports: `typing`, `{source}.query`, `models.extract_cmd`
- `@extract_method(name="{source}-{flow}")` decorator
- Correct function signature: `extract_data(query: {Source}Query) -> List[Dict[str, Any]]`

### Adding New Data Sources

### Manual Implementation (After Script Generation)

After using `python scripts/new.py source_name flows...`, customize the generated templates:

1. **Implement the Query Class**:
```python
# In source/query.py (generated by script, customize as needed)
from dataclasses import dataclass
from typing import Dict, Any
from models.query import Query, query_type

@dataclass
@query_type("github")  # Auto-registered via decorator
class GithubQuery(Query):
    """Query implementation for GitHub API data extraction."""
    
    # Add your specific fields
    api_token: str
    repository: str
    since_date: str
    
    def __init__(self, api_token: str, repository: str, since_date: str,
                 source_kind_id: str, event_type: str):
        self.api_token = api_token
        self.repository = repository
        self.since_date = since_date
        super().__init__(source_kind_id, event_type)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "GithubQuery":
        return GithubQuery(
            api_token=data.get("api_token", ""),
            repository=data.get("repository", ""),
            since_date=data.get("since_date", ""),
            source_kind_id=data.get("source_kind_id", "github"),
            event_type=data.get("event_type", "")
        )

    def to_summary_base(self) -> Dict[str, Any]:
        return {
            "repository": self.repository,
            "since_date": self.since_date
        }
```

2. **Implement the Extraction Method**:
```python
# In source/flows/issues.py (generated by script, customize as needed)
from typing import List, Dict, Any
from github.query import GithubQuery
from models.extract_cmd import extract_method
import logging

logger = logging.getLogger(__name__)

@extract_method(name="github-issues")  # Auto-registered via decorator
async def extract_data(query: GithubQuery) -> List[Dict[str, Any]]:
    """Extract GitHub issues data"""
    logger.info(f"Extracting GitHub issues for repository: {query.repository}")
    
    # Your implementation here
    events = []
    
    # Example: Fetch issues from GitHub API
    # github_client = setup_github_client(query.api_token)
    # issues = github_client.get_repo(query.repository).get_issues(since=query.since_date)
    # 
    # for issue in issues:
    #     events.append({
    #         "event_id": f"github-issue-{issue.id}",
    #         "parent_item_id": str(issue.id),
    #         "relation_type": "created" if issue.created_at >= since else "updated",
    #         "employee_id": issue.user.login,
    #         "event_time_utc": issue.created_at.isoformat(),
    #         "event_properties": {
    #             "title": issue.title,
    #             "state": issue.state,
    #             "labels": [label.name for label in issue.labels]
    #         }
    #     })
    
    return events
```

3. **Use the New Source in Workflows**:
```python
# The source is automatically registered - just use it!
flow_input = FlowInput(
    query_type="github",  # Routes to GithubQuery via @query_type("github")
    args={
        "api_token": "your-github-token",
        "repository": "owner/repo-name",
        "since_date": "2024-01-01",
        "source_kind_id": "github",
        "event_type": "issues"  # Routes to github-issues via @extract_method
    }
)
```

### 🔍 Validation and Maintenance

The `scripts/new.py` tool also serves as a validation and maintenance utility:

```bash
# Validate existing sources and fix any issues
python scripts/new.py launchpad bugs merge_proposals questions

# Add new flows to existing sources  
python scripts/new.py github releases deployments

# Check if a source needs updates (safe - creates backups)
python scripts/new.py my_existing_source
```

**What happens during validation:**
- ✅ **Existing valid files**: Left untouched
- ⚠️ **Missing imports/decorators**: Fixed with backup created  
- 📁 **Missing directories**: Created automatically
- 📄 **Missing files**: Generated from templates
- 🔄 **Broken structure**: Rebuilt with original backed up

### Adding New Query Types

**Recommended approach using the script:**

**Recommended approach using the script:**

1. **Generate the structure**:
```bash
python scripts/new.py custom_api data sync metrics
```

2. **The script creates everything you need** - just customize the generated templates in `custom_api/query.py` and the flow files.

### 📋 Development Workflow

1. **Plan your data source**: Identify what data you want to extract and how it maps to events
2. **Generate structure**: `python scripts/new.py your_source flow1 flow2`
3. **Implement query logic**: Customize the generated `query.py` with your API parameters
4. **Implement extraction**: Customize the generated flow files with your data extraction logic
5. **Test locally**: Use the worker and test runners to validate your implementation
6. **Monitor in Temporal UI**: View workflow execution at http://localhost:8080

### Best Practices for Extensions

#### 🏗️ **Development Process**
- **Use the automation script**: Always start with `python scripts/new.py` for consistent structure
- **Follow naming conventions**: Use snake_case for source names and flow names
- **Validate regularly**: Run the script on existing sources to catch structural issues
- **Check backups**: Review `.backup` files if the script modifies existing code

#### 🔧 **Implementation Guidelines**
- **Error Handling**: Use Temporal's retry policies and handle transient failures gracefully
- **Rate Limiting**: Implement appropriate delays for API calls to avoid rate limits  
- **Idempotency**: Ensure activities can be safely retried without side effects
- **Batch Processing**: Use the configurable `ETLFlow.BATCH_SIZE` for efficient database operations
- **Logging**: Use structured logging for better observability
- **Schema Validation**: Validate event data structure before database insertion

#### 🏛️ **Architecture Guidelines**
- **Connection Pooling**: Leverage the existing PostgreSQL connection pool for database operations
- **Decorator Registration**: Use `@query_type()` and `@extract_method()` decorators for automatic component discovery
- **Module Organization**: Place extraction methods in `flows/` subdirectories and query classes in `query.py` files for auto-discovery
- **Type Hints**: Use proper type annotations for better IDE support and code clarity

#### 🧪 **Testing and Validation**
```bash
# Validate your source structure
python scripts/new.py your_source

# Test your implementation
python worker.py  # In one terminal
python your_test_runner.py  # In another terminal

# Monitor execution
# Visit http://localhost:8080 for Temporal Web UI
```

#### 📋 **Quick Reference**

| Task | Command | Result |
|------|---------|---------|
| Create new source | `python scripts/new.py github issues pulls` | Creates `github/` with 2 flows |
| Validate existing | `python scripts/new.py launchpad bugs` | Checks/fixes launchpad structure |
| Add flows | `python scripts/new.py existing_source new_flow` | Adds new flow to existing source |
| Help | `python scripts/new.py -h` | Shows usage examples |

**Generated Files:**
- `{source}/query.py` - Query class with proper decorators and methods
- `{source}/flows/{flow}.py` - Extract method with correct signature
- `{source}/__init__.py` and `{source}/flows/__init__.py` - Package markers

**Auto-Discovery:** All sources and flows are automatically registered via decorators - no manual registration required!

## 📚 Additional Resources
- **[Temporal Documentation](https://docs.temporal.io/)** - Complete guide to Temporal workflows and activities
- **[Temporal Python SDK](https://python.temporal.io/)** - Python-specific Temporal development guide
- **[Launchpad API Documentation](https://help.launchpad.net/API)** - Launchpad REST API reference and launchpadlib usage
- **[PostgreSQL Documentation](https://www.postgresql.org/docs/)** - PostgreSQL database administration and SQL reference
- **[Docker Compose Reference](https://docs.docker.com/compose/)** - Container orchestration documentation
- **[Python AsyncIO Guide](https://docs.python.org/3/library/asyncio.html)** - Asynchronous programming patterns in Python