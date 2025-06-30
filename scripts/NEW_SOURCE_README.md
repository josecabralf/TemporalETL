# New Source Script

The `new_source.py` script automates the creation of new data sources for the TemporalETL system.

## Usage

```bash
python new_source.py [source_name] [flow1] [flow2] ...
```

### Parameters

- `source_name` (optional): Name of the new source (default: 'sample_source')
- `flows` (optional): Names of the flows to create (default: ['sample'])

### Examples

1. **Create default source with default flow:**
   ```bash
   python new_source.py
   ```
   Creates `sample_source` directory with a `sample` flow.

2. **Create custom source with default flow:**
   ```bash
   python new_source.py github
   ```
   Creates `github` directory with a `sample` flow.

3. **Create custom source with multiple flows:**
   ```bash
   python new_source.py github issues pull_requests releases
   ```
   Creates `github` directory with `issues`, `pull_requests`, and `releases` flows.

## Generated Structure

For a source named `github` with flows `issues` and `pull_requests`, the script creates:

```
github/
├── __init__.py
├── query.py
└── flows/
    ├── __init__.py
    ├── issues.py
    └── pull_requests.py
```

### Generated Files

#### `query.py`
Contains the query class with the following template:
- Inherits from `Query` base class
- Uses `@query_type` decorator with the source name
- Includes placeholder methods for `from_dict()` and `to_summary_base()`

#### Flow files (`flows/{flow_name}.py`)
Each flow file contains:
- Import statements for the source query class
- Logging configuration
- `@extract_method` decorator with name format `{source_name}-{flow_name}`
- Placeholder `extract_data()` function

## Next Steps

After running the script:

1. **Implement query logic** in `{source_name}/query.py`:
   - Add source-specific fields to the dataclass
   - Implement `from_dict()` method for deserialization
   - Implement `to_summary_base()` method for logging

2. **Implement extraction logic** in flow files:
   - Replace the `raise NotImplementedError()` with actual data extraction code
   - Use the query parameters to fetch data from your source

3. **Add dependencies** (if needed):
   - Update `requirements.txt` with any new packages required by your source

## Error Handling

The script will exit with an error if:
- The target directory already exists
- There are permission issues creating directories or files

## Examples of Real Sources

Look at the existing `launchpad/` directory for a complete example of:
- A fully implemented query class (`launchpad/query.py`)
- Multiple flow implementations (`launchpad/flows/bugs.py`, etc.)
