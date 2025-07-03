#!/usr/bin/env python3
"""
Script to automate the creation of new data sources for the TemporalETL system,
or to validate and update existing sources to ensure proper structure.

Usage:
    python new.py [source_name] [flow1] [flow2] ...

Examples:
    python new.py                                    # Creates 'sample_source' with 'sample' flow
    python new.py github                             # Creates/updates 'github' source with 'sample' flow
    python new.py github issues pull_requests        # Creates/updates 'github' source with 'issues' and 'pull_requests' flows

Features:
    - Creates new sources with proper structure
    - Validates existing sources and ensures required imports/decorators/methods
    - Creates backup files before overwriting existing files
    - Handles both new and existing flows
"""

import os
import sys
import argparse
import re
from pathlib import Path


def to_pascal_case(snake_str: str) -> str:
    """Convert snake_case to PascalCase"""
    return ''.join(word.capitalize() for word in snake_str.split('_'))


def check_and_ensure_query_file(source_dir: Path, source_name: str) -> None:
    """Check if query.py exists and ensure it has the required structure"""
    query_file = source_dir / "query.py"
    source_name_pascal = to_pascal_case(source_name)
    
    if not query_file.exists():
        create_query_file(source_dir, source_name)
        return
    
    # Read existing file
    content = query_file.read_text()
    
    # Check for minimum imports
    required_imports = [
        "from dataclasses import dataclass",
        "from typing import Dict, Any",
        "from models.etl.query import Query, query_type"
    ]
    
    missing_imports = []
    for imp in required_imports:
        if imp not in content:
            missing_imports.append(imp)
    
    # Check for query_type decorator
    query_type_pattern = rf'@query_type\("{re.escape(source_name)}"\)'
    has_query_type = bool(re.search(query_type_pattern, content))
    
    # Check for class definition inheriting from Query
    class_pattern = rf'class\s+{re.escape(source_name_pascal)}Query\s*\(\s*Query\s*\)'
    has_class = bool(re.search(class_pattern, content))
    
    # Check for required methods
    has_from_dict = bool(re.search(r'def\s+from_dict\s*\(', content))
    has_to_summary_base = bool(re.search(r'def\s+to_summary_base\s*\(', content))
    
    needs_update = missing_imports or not has_query_type or not has_class or not has_from_dict or not has_to_summary_base
    
    if needs_update:
        print(f"⚠️  {query_file} exists but needs updates")
        if missing_imports:
            print(f"   Missing imports: {', '.join(missing_imports)}")
        if not has_query_type:
            print(f"   Missing @query_type('{source_name}') decorator")
        if not has_class:
            print(f"   Missing {source_name_pascal}Query class inheriting from Query")
        if not has_from_dict:
            print("   Missing from_dict static method")
        if not has_to_summary_base:
            print("   Missing to_summary_base method")
        
        # Create a backup and recreate the file
        backup_file = query_file.with_suffix('.py.backup')
        backup_file.write_text(content)
        print(f"   Created backup: {backup_file}")
        
        create_query_file(source_dir, source_name)
        print(f"   Recreated: {query_file}")
    else:
        print(f"✅ {query_file} exists and has required structure")


def check_and_ensure_flow_file(flows_dir: Path, source_name: str, flow_name: str) -> None:
    """Check if flow file exists and ensure it has the required structure"""
    flow_file = flows_dir / f"{flow_name}.py"
    source_name_pascal = to_pascal_case(source_name)
    
    if not flow_file.exists():
        create_flow_file(flows_dir, source_name, flow_name)
        return
    
    # Read existing file
    content = flow_file.read_text()
    
    # Check for minimum imports
    required_imports = [
        "from typing import List, Dict, Any",
        f"from {source_name}.query import {source_name_pascal}Query",
        "from models.etl.extract_cmd import extract_method"
    ]
    
    missing_imports = []
    for imp in required_imports:
        if imp not in content:
            missing_imports.append(imp)
    
    # Check for extract_method decorator
    extract_method_pattern = rf'@extract_method\(name="{re.escape(source_name)}-{re.escape(flow_name)}"\)'
    has_extract_method = bool(re.search(extract_method_pattern, content))
    
    # Check for extract_data function signature
    function_pattern = rf'def\s+extract_data\s*\(\s*query:\s*{re.escape(source_name_pascal)}Query\s*\)\s*->\s*List\[Dict\[str,\s*Any\]\]'
    has_function = bool(re.search(function_pattern, content))
    
    needs_update = missing_imports or not has_extract_method or not has_function
    
    if needs_update:
        print(f"⚠️  {flow_file} exists but needs updates")
        if missing_imports:
            print(f"   Missing imports: {', '.join(missing_imports)}")
        if not has_extract_method:
            print(f"   Missing @extract_method(name='{source_name}-{flow_name}') decorator")
        if not has_function:
            print(f"   Missing extract_data function with correct signature")
        
        # Create a backup and recreate the file
        backup_file = flow_file.with_suffix('.py.backup')
        backup_file.write_text(content)
        print(f"   Created backup: {backup_file}")
        
        create_flow_file(flows_dir, source_name, flow_name)
        print(f"   Recreated: {flow_file}")
    else:
        print(f"✅ {flow_file} exists and has required structure")


def create_query_file(source_dir: Path, source_name: str) -> None:
    """Create the query.py file for the new source"""
    source_name_pascal = to_pascal_case(source_name)
    
    query_content = f'''from dataclasses import dataclass
from typing import Dict, Any

from models.etl.query import Query, query_type


@dataclass
@query_type("{source_name}")
class {source_name_pascal}Query(Query):
    """
    Query implementation for {source_name} data extraction.
    """
    def __init__(self, source_kind_id: str, event_type: str):
        super().__init__(source_kind_id, event_type)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "{source_name_pascal}Query":
        raise NotImplementedError("Subclasses must implement from_dict() method.")

    def to_summary_base(self) -> Dict[str, Any]:
        raise NotImplementedError("Subclasses must implement to_summary_base() method.")
'''
    
    query_file = source_dir / "query.py"
    query_file.write_text(query_content)
    print(f"📄 Created: {query_file}")


def create_flow_file(flows_dir: Path, source_name: str, flow_name: str) -> None:
    """Create a flow file for the new source"""
    source_name_pascal = to_pascal_case(source_name)
    
    flow_content = f'''from typing import List, Dict, Any

from {source_name}.query import {source_name_pascal}Query
from models.etl.extract_cmd import extract_method

import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@extract_method(name="{source_name}-{flow_name}")
async def extract_data(query: {source_name_pascal}Query) -> List[Dict[str, Any]]:
    raise NotImplementedError("Must implement this method.")
'''
    
    flow_file = flows_dir / f"{flow_name}.py"
    flow_file.write_text(flow_content)
    print(f"📄 Created: {flow_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Create a new data source for the TemporalETL system or update an existing one",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python new.py                                    # Creates 'sample_source' with 'sample' flow
  python new.py github                             # Creates/updates 'github' source with 'sample' flow  
  python new.py github issues pull_requests        # Creates/updates 'github' source with 'issues' and 'pull_requests' flows
        """
    )
    
    parser.add_argument(
        'source_name', 
        nargs='?', 
        default='sample_source',
        help='Name of the new source (default: sample_source)'
    )
    
    parser.add_argument(
        'flows', 
        nargs='*', 
        default=['sample'],
        help='Names of the flows to create (default: sample)'
    )
    
    args = parser.parse_args()
    
    source_name = args.source_name
    flows = args.flows
    
    print(f"Processing source: {source_name}")
    print(f"With flows: {', '.join(flows)}")
    
    # Get the current working directory (should be the project root)
    project_root = Path.cwd()
    
    # Handle source directory
    source_dir = project_root / "sources" / source_name
    source_exists = source_dir.exists()
    
    if source_exists:
        print(f"📁 Source directory '{source_dir}' already exists - checking structure")
    else:
        source_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Created source directory: {source_dir}")
    
    # Handle flows directory
    flows_dir = source_dir / "flows"
    flows_exists = flows_dir.exists()
    
    if flows_exists:
        print(f"📁 Flows directory '{flows_dir}' already exists")
    else:
        flows_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Created flows directory: {flows_dir}")
    
    # Check and ensure query.py file
    check_and_ensure_query_file(source_dir, source_name)
    
    # Check and ensure flow files
    for flow in flows:
        check_and_ensure_flow_file(flows_dir, source_name, flow)
    
    print(f"\n✅ Successfully processed source '{source_name}' with {len(flows)} flow(s)")
    print(f"📁 Source directory: {source_dir}")
    
    if not source_exists:
        print(f"🆕 New source created with files:")
        print(f"   - {source_dir}/query.py")
        for flow in flows:
            print(f"   - {source_dir}/flows/{flow}.py")
        
        print(f"\n🚀 Next steps:")
        print(f"   1. Implement the query logic in {source_dir}/query.py")
        print(f"   2. Implement the extraction logic in the flow files")
        print(f"   3. Add any required dependencies to requirements.txt")
    else:
        print(f"🔄 Existing source validated and updated as needed")
        print(f"\n💡 Tip: Check backup files (.backup) if any important code was overwritten")


if __name__ == "__main__":
    main()
