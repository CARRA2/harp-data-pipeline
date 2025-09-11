#!/usr/bin/env python3
"""
Script to check for missing variables in MARS archive by comparing 
expected parameters from templates with actual available data.
Can check for specific month/year and validate level counts for ml/pl/hl types.
"""
import sys
import os
import subprocess
import re
from collections import defaultdict
import argparse
import tempfile
from datetime import datetime, timedelta
import json

def parse_template_file(template_path):
    """Parse a MARS template file and extract parameters."""
    params = []
    levtype = None
    stream = None
    type_val = None
    levelist = None
    
    with open(template_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('param='):
                param_line = line.replace('param=', '').rstrip(',')
                params = param_line.split('/')
            elif line.startswith('levtype='):
                levtype = line.replace('levtype=', '').rstrip(',')
            elif line.startswith('stream='):
                stream = line.replace('stream=', '').rstrip(',')
            elif line.startswith('type='):
                type_val = line.replace('type=', '').rstrip(',')
            elif line.startswith('levelist='):
                levelist_line = line.replace('levelist=', '').rstrip(',')
                levelist = levelist_line.split('/')
    
    return {
        'params': params,
        'levtype': levtype,
        'stream': stream,
        'type': type_val,
        'levelist': levelist
    }

def create_custom_template(template_path, year, month, database=None):
    """Create a temporary template file with custom date range and optionally custom database."""
    template_info = parse_template_file(template_path)
    
    # Generate date range for the specified month (if provided)
    date_str = None
    if year is not None and month is not None:
        if template_info['stream'] == 'moda':
            # Monthly data - single date
            date_str = f"{year}{month:02d}01"
        else:
            # Daily data - full month range
            start_date = datetime(year, month, 1)
            if month == 12:
                end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(year, month + 1, 1) - timedelta(days=1)
            
            date_str = f"{start_date.strftime('%Y-%m-%d')}/to/{end_date.strftime('%Y-%m-%d')}"
    
    # Create temporary file
    temp_fd, temp_path = tempfile.mkstemp(suffix='.mars', text=True)
    print(temp_path)
    
    try:
        with os.fdopen(temp_fd, 'w') as f:
            with open(template_path, 'r') as orig:
                for line in orig:
                    if line.strip().startswith('date=') and date_str:
                        f.write(f"date={date_str},\n")
                    elif line.strip().startswith('database=') and database:
                        f.write(f"database={database},\n")
                    else:
                        f.write(line)
        return temp_path
    except Exception as e:
        os.unlink(temp_path)
        raise e

def run_mars_listing(template_path, year=None, month=None, database=None):
    """Run mars with a template file and return the output."""
    actual_template = template_path
    temp_template = None
    
    try:
        # Create custom template if year/month or database specified
        if (year is not None and month is not None) or database is not None:
            temp_template = create_custom_template(template_path, year, month, database)
            #with open(temp_template, 'r') as file:
            #    content = file.read()
            #    print(content)
            actual_template = temp_template
        
        result = subprocess.run(['mars', actual_template], 
                              capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            return result.stdout
        else:
            print(f"Error running mars with {actual_template}:")
            print(result.stderr)
            return None
    except subprocess.TimeoutExpired:
        print(f"Mars command timed out for {actual_template}")
        return None
    except Exception as e:
        print(f"Error running mars: {e}")
        return None
    finally:
        # Clean up temporary file
        if temp_template and os.path.exists(temp_template):
            os.unlink(temp_template)

def parse_mars_output(mars_output, levtype=None):
    """Parse MARS output and extract available parameters by date and level information."""
    available_params = defaultdict(set)
    level_counts = defaultdict(lambda: defaultdict(set))  # param -> date -> set of levels
    current_date = None
    
    lines = mars_output.split('\n')
    in_data_section = False
    
    for line in lines:
        line = line.strip()
        
        # Check if we're in the data section (after the header)
        if re.match(r'^(date|file)\s+(file\s+)?length.*param', line):
            in_data_section = True
            continue
        
        if not in_data_section:
            continue
        
        # Handle different formats based on level type and stream type
        if levtype in ['ml', 'pl', 'hl']:
            # Format with levelist column: date file length levelist missing offset param
            date_match = re.match(r'^(\d{4}-\d{2}-\d{2}|\d{8})\s+\d+\s+\d+\s+(\d+)\s+\.\s+\d+\s+(\d+)', line)
            if date_match:
                date_str = date_match.group(1)
                # Normalize date format
                if len(date_str) == 8:  # YYYYMMDD
                    current_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                else:
                    current_date = date_str
                level = date_match.group(2)
                param = date_match.group(3)
                available_params[current_date].add(param)
                level_counts[param][current_date].add(level)
        else:
            # Try daily format first: date file length missing offset param
            date_match = re.match(r'^(\d{4}-\d{2}-\d{2}|\d{8})\s+\d+\s+\d+\s+\.\s+\d+\s+(\d+)', line)
            if date_match:
                date_str = date_match.group(1)
                # Normalize date format
                if len(date_str) == 8:  # YYYYMMDD
                    current_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                else:
                    current_date = date_str
                param = date_match.group(2)
                available_params[current_date].add(param)
            else:
                # Try monthly format: file length missing month offset param
                monthly_match = re.match(r'^\d+\s+\d+\s+\.\s+(\d{4}-\d{2})\s+\d+\s+(\d+)', line)
                if monthly_match:
                    month_str = monthly_match.group(1)
                    # Convert YYYY-MM to YYYY-MM-01 for consistency
                    current_date = f"{month_str}-01"
                    param = monthly_match.group(2)
                    available_params[current_date].add(param)
    
    return available_params, level_counts

def check_missing_variables(template_path, year=None, month=None, database=None, verbose=True):
    """Check for missing variables in a specific template."""
    if verbose:
        print(f"\nChecking template: {os.path.basename(template_path)}")
        if year is not None and month is not None:
            print(f"For: {year}-{month:02d}")
        if database:
            print(f"Database: {database}")
        print("=" * 60)
    
    # Parse template to get expected parameters
    template_info = parse_template_file(template_path)
    expected_params = set(template_info['params'])
    expected_levels = template_info['levelist']
    levtype = template_info['levtype']
    
    # Extract template details from filename and content
    template_name = os.path.basename(template_path)
    template_parts = template_name.replace('mars_request_', '').split('_')
    
    # Parse template name: mars_request_[levtype]_levels_[type]_[stream]
    if len(template_parts) >= 4:
        type_val = template_parts[-2]  # an or fc
        stream = template_parts[-1]    # dame or moda
    else:
        type_val = template_info['type']
        stream = template_info['stream']
    
    if verbose:
        print(f"Level type: {levtype}")
        if expected_levels:
            print(f"Expected levels ({len(expected_levels)}): {expected_levels}")
        print(f"Expected parameters ({len(expected_params)}): {sorted(expected_params)}")
    
    # Run MARS listing
    mars_output = run_mars_listing(template_path, year, month, database)
    if not mars_output:
        if verbose:
            print("Failed to get MARS output")
        return None
    
    # Parse MARS output
    available_by_date, level_counts = parse_mars_output(mars_output, levtype)
    
    if not available_by_date:
        if verbose:
            print("No data found in MARS output")
        return None
    
    # Check for missing parameters
    all_dates = sorted(available_by_date.keys())
    if verbose:
        print(f"\nDate range in archive: {all_dates[0]} to {all_dates[-1]}")
        print(f"Total dates checked: {len(all_dates)}")
    
    # Find parameters that are missing across all dates
    all_available_params = set()
    for date_params in available_by_date.values():
        all_available_params.update(date_params)
    
    missing_params = expected_params - all_available_params
    extra_params = all_available_params - expected_params
    
    if verbose:
        print(f"\nActually available parameters ({len(all_available_params)}): {sorted(all_available_params)}")
        
        if missing_params:
            print(f"\nMISSING PARAMETERS ({len(missing_params)}):")
            for param in sorted(missing_params):
                print(f"  - {param}")
        else:
            print("\n✓ All expected parameters are available")
        
        if extra_params:
            print(f"\nEXTRA PARAMETERS ({len(extra_params)}) (available but not in template):")
            for param in sorted(extra_params):
                print(f"  + {param}")
    
    # Check for parameters missing on specific dates
    if verbose:
        print(f"\nChecking parameter availability by date...")
    params_missing_dates = defaultdict(list)
    
    for date, date_params in available_by_date.items():
        missing_on_date = expected_params - date_params
        for param in missing_on_date:
            params_missing_dates[param].append(date)
    
    if verbose and params_missing_dates:
        print(f"\nPARAMETERS WITH MISSING DATES:")
        for param, missing_dates in sorted(params_missing_dates.items()):
            print(f"  Parameter {param}: missing on {len(missing_dates)} dates")
            if len(missing_dates) <= 10:  # Show dates if not too many
                print(f"    Dates: {', '.join(missing_dates)}")
            else:
                print(f"    First 5 dates: {', '.join(missing_dates[:5])}")
                print(f"    Last 5 dates: {', '.join(missing_dates[-5:])}")
    
    # Check level counts for ml/pl/hl types
    level_issues = defaultdict(list)  # param -> list of (date, actual_count, expected_count)
    
    if levtype in ['ml', 'pl', 'hl'] and expected_levels and level_counts:
        if verbose:
            print(f"\nChecking level counts for {levtype} type...")
        expected_level_count = len(expected_levels)
        
        for param in sorted(all_available_params):
            if param in level_counts:
                param_level_issues = []
                for date, levels in level_counts[param].items():
                    actual_count = len(levels)
                    if actual_count != expected_level_count:
                        param_level_issues.append((date, actual_count, expected_level_count))
                        level_issues[param].append({
                            'date': date,
                            'actual_count': actual_count,
                            'expected_count': expected_level_count,
                            'missing_levels': list(set(expected_levels) - levels)
                        })
                
                if verbose:
                    if param_level_issues:
                        print(f"  Parameter {param}: level count issues on {len(param_level_issues)} dates")
                        if len(param_level_issues) <= 5:
                            for date, actual, expected in param_level_issues:
                                print(f"    {date}: {actual}/{expected}")
                        else:
                            for date, actual, expected in param_level_issues[:3]:
                                print(f"    {date}: {actual}/{expected}")
                            print(f"    ... and {len(param_level_issues) - 3} more dates")
                    else:
                        print(f"  ✓ Parameter {param}: correct level count on all dates")
    elif levtype in ['ml', 'pl', 'hl'] and not expected_levels:
        if verbose:
            print(f"\nWarning: {levtype} level type but no levelist found in template")
    
    # Return structured data for summary
    return {
        'template_name': template_name,
        'levtype': levtype,
        'type': type_val,
        'stream': stream,
        'expected_levels': expected_levels,
        'date_range': f"{all_dates[0]} to {all_dates[-1]}" if all_dates else "No data",
        'total_dates': len(all_dates),
        'missing_params': sorted(missing_params),
        'extra_params': sorted(extra_params),
        'params_missing_dates': dict(params_missing_dates),
        'level_issues': dict(level_issues),
        'database': database or 'default'
    }

def generate_summary(results):
    """Generate a comprehensive summary of all missing variables."""
    if not results:
        print("\nNo results to summarize.")
        return
    
    print("\n" + "=" * 80)
    print("COMPREHENSIVE MISSING VARIABLES SUMMARY")
    print("=" * 80)
    
    # Filter out None results
    valid_results = [r for r in results if r is not None]
    
    if not valid_results:
        print("No valid results found.")
        return
    
    # Summary by parameter
    print("\n1. MISSING PARAMETERS SUMMARY BY PARAM")
    print("-" * 50)
    
    all_missing_params = set()
    param_templates = defaultdict(list)
    
    for result in valid_results:
        for param in result['missing_params']:
            all_missing_params.add(param)
            param_templates[param].append({
                'template': result['template_name'],
                'levtype': result['levtype'],
                'type': result['type'],
                'stream': result['stream'],
                'database': result['database']
            })
    
    if all_missing_params:
        for param in sorted(all_missing_params):
            print(f"\nParameter {param}:")
            templates = param_templates[param]
            for tmpl in templates:
                print(f"  - {tmpl['template']} (levtype={tmpl['levtype']}, type={tmpl['type']}, stream={tmpl['stream']}, db={tmpl['database']})")
    else:
        print("✓ No completely missing parameters found")
    
    # Summary by template characteristics
    print("\n\n2. MISSING PARAMETERS BY TYPE/STREAM/LEVTYPE")
    print("-" * 50)
    
    type_stream_missing = defaultdict(lambda: defaultdict(set))
    
    for result in valid_results:
        key = f"{result['type']}/{result['stream']}/{result['levtype']}"
        for param in result['missing_params']:
            type_stream_missing[key]['params'].add(param)
        
        if not hasattr(type_stream_missing[key], 'count'):
            type_stream_missing[key]['count'] = 0
            type_stream_missing[key]['params'] = set()
        type_stream_missing[key]['count'] += 1
    
    for key, data in sorted(type_stream_missing.items()):
        if data['params']:
            print(f"\n{key}:")
            print(f"  Missing parameters: {sorted(data['params'])}")
    
    # Summary of parameters missing on specific dates (not completely missing)
    print("\n\n3. PARAMETERS WITH PARTIAL DATE COVERAGE")
    print("-" * 50)
    
    partial_missing = defaultdict(list)
    
    for result in valid_results:
        for param, dates in result['params_missing_dates'].items():
            if param not in result['missing_params']:  # Not completely missing
                partial_missing[param].append({
                    'template': result['template_name'],
                    'missing_dates': len(dates),
                    'total_dates': result['total_dates'],
                    'date_range': result['date_range'],
                    'levtype': result['levtype'],
                    'type': result['type'],
                    'stream': result['stream']
                })
    
    if partial_missing:
        for param in sorted(partial_missing.keys()):
            print(f"\nParameter {param}:")
            for info in partial_missing[param]:
                coverage = ((info['total_dates'] - info['missing_dates']) / info['total_dates']) * 100
                print(f"  - {info['template']}: {coverage:.1f}% coverage ({info['missing_dates']}/{info['total_dates']} missing) [{info['date_range']}]")
    else:
        print("✓ No parameters with partial date coverage issues found")
    
    # Summary of level issues
    print("\n\n4. LEVEL COUNT ISSUES (ml/pl/hl types)")
    print("-" * 50)
    
    level_issues_found = False
    for result in valid_results:
        if result['level_issues']:
            level_issues_found = True
            print(f"\nTemplate: {result['template_name']} (levtype={result['levtype']})")
            for param, issues in result['level_issues'].items():
                print(f"  Parameter {param}: {len(issues)} dates with level issues")
                # Show a few examples
                for i, issue in enumerate(issues[:3]):
                    missing_levels = ', '.join(issue['missing_levels']) if issue['missing_levels'] else 'none'
                    print(f"    {issue['date']}: {issue['actual_count']}/{issue['expected_count']} levels (missing: {missing_levels})")
                if len(issues) > 3:
                    print(f"    ... and {len(issues) - 3} more dates")
    
    if not level_issues_found:
        print("✓ No level count issues found")
    
    # Overall statistics
    print("\n\n5. OVERALL STATISTICS")
    print("-" * 50)
    
    templates_with_issues = sum(1 for r in valid_results if r['missing_params'] or r['params_missing_dates'] or r['level_issues'])
    total_missing_params = len(all_missing_params)
    templates_by_type = defaultdict(int)
    
    for result in valid_results:
        templates_by_type[f"{result['type']}/{result['stream']}/{result['levtype']}"] += 1
    
    print(f"Total templates checked: {len(valid_results)}")
    print(f"Templates with issues: {templates_with_issues}")
    print(f"Total unique missing parameters: {total_missing_params}")
    print(f"\nTemplates by type:")
    for type_combo, count in sorted(templates_by_type.items()):
        print(f"  {type_combo}: {count}")

def main():
    parser = argparse.ArgumentParser(
        description='Check for missing variables in MARS templates',
        epilog='''
Examples:
  # Check all templates for default date range
  python3 check_missing_variables.py
  
  # Check specific template for October 1985
  python3 check_missing_variables.py -t mars_request_sfc_levels_an_dame -y 1985 -m 10
  
  # Check pressure level template (includes level count validation)
  python3 check_missing_variables.py -t mars_request_pl_levels_an_dame -y 1985 -m 9
  
  # Check monthly template
  python3 check_missing_variables.py -t mars_request_sfc_levels_an_moda -y 1985 -m 9
  
  # Check using marser database instead of marssc
  python3 check_missing_variables.py -t mars_request_sfc_levels_an_dame -y 1985 -m 10 -db marser
  
  # Check all templates with marser database
  python3 check_missing_variables.py -db marser
  
  # Generate comprehensive summary of missing variables across all templates
  python3 check_missing_variables.py --summary
  
  # Generate summary for specific month/year
  python3 check_missing_variables.py -y 1985 -m 10 --summary
  
  # Export results to JSON file
  python3 check_missing_variables.py -y 1985 -m 10 -j missing_vars.json
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--template', '-t', 
                       help='Specific template file to check (default: check all)')
    parser.add_argument('--template-dir', '-d', default='mars_templates',
                       help='Directory containing MARS templates (default: mars_templates)')
    parser.add_argument('--year', '-y', type=int,
                       help='Year to check (e.g., 1985)')
    parser.add_argument('--month', '-m', type=int, choices=range(1, 13),
                       help='Month to check (1-12)')
    parser.add_argument('--database', '-db', choices=['marssc', 'marser'],
                       help='MARS database to use (default: use template default, usually marssc)')
    parser.add_argument('--summary', '-s', action='store_true',
                       help='Generate comprehensive summary of all missing variables')
    parser.add_argument('--json-output', '-j', metavar='FILE',
                       help='Export results to JSON file')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.template_dir):
        print(f"Template directory not found: {args.template_dir}")
        return
    
    # Check if both year and month are provided or neither
    if (args.year is None) != (args.month is None):
        print("Error: Both --year and --month must be provided together, or neither")
        return
    
    results = []
    
    if args.template:
        # Check specific template
        template_path = os.path.join(args.template_dir, args.template)
        if not os.path.exists(template_path):
            template_path = args.template  # Try as full path
        
        if os.path.exists(template_path):
            result = check_missing_variables(template_path, args.year, args.month, args.database, not args.summary)
            if result:
                results.append(result)
        else:
            print(f"Template file not found: {args.template}")
    else:
        # Check all templates
        template_files = [f for f in os.listdir(args.template_dir) 
                         if f.startswith('mars_request_')]
        
        if not template_files:
            print(f"No MARS template files found in {args.template_dir}")
            return
        
        if not args.summary:
            print(f"Found {len(template_files)} template files")
            if args.year and args.month:
                print(f"Checking for {args.year}-{args.month:02d}")
            if args.database:
                print(f"Using database: {args.database}")
        
        for template_file in sorted(template_files):
            template_path = os.path.join(args.template_dir, template_file)
            try:
                result = check_missing_variables(template_path, args.year, args.month, args.database, not args.summary)
                if result:
                    results.append(result)
            except Exception as e:
                if not args.summary:
                    print(f"Error checking {template_file}: {e}")
                continue
            
            if not args.summary:
                print("\n" + "="*80 + "\n")
    
    # Generate summary if requested
    if args.summary:
        generate_summary(results)
    
    # Export to JSON if requested
    if args.json_output:
        with open(args.json_output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nResults exported to {args.json_output}")

if __name__ == "__main__":
    main()
