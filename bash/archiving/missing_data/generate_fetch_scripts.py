#!/usr/bin/env python3
"""
Script to generate MARS fetching scripts from missing data JSON files.
Unlike the templates that use 'list', this generates 'retrieve' commands 
with stream=oper and target= specifications.
"""

import json
import argparse
import os
from collections import defaultdict
from datetime import datetime


def parse_template_for_base_info(template_path,database='marser'):
    """Parse a template file to extract base parameters for fetching."""
    base_params = {
        'class': 'rr',
        'database': database,
        'expver': 'prod',
        'origin': 'no-ar-pa'
    }
    
    if os.path.exists(template_path):
        with open(template_path, 'r') as f:
            for line in f:
                line = line.strip().rstrip(',')
                if '=' in line:
                    key, value = line.split('=', 1)
                    # Don't override database if it was explicitly passed
                    if key in ['class', 'expver', 'origin']:
                        base_params[key] = value
                    elif key == 'database' and database == 'marser':
                        # Keep the passed database parameter, don't override from template
                        continue
                    elif key == 'database':
                        # Only use template database if no specific database was requested
                        base_params[key] = value
    
    return base_params


def generate_target_filename(template_name, date_str, param=None, level=None):
    """Generate a target filename for the retrieved data."""
    # Remove mars_request_ prefix and extract components
    base_name = template_name.replace('mars_request_', '')
    
    # Format date for filename (remove hyphens)
    clean_date = date_str.replace('-', '')
    
    # Build filename components
    filename_parts = [base_name, clean_date]
    
    if param:
        filename_parts.append(f"param{param}")
    
    if level:
        filename_parts.append(f"lev{level}")
    
    return '_'.join(filename_parts) + '.grib'


def generate_fetch_script_for_missing_params(result, template_dir="mars_templates", output_dir="fetch_scripts"):
    """Generate fetch scripts for completely missing parameters."""
    scripts = []
    
    if not result['missing_params']:
        return scripts
    
    # Get base parameters from template
    template_path = os.path.join(template_dir, result['template_name'])
    base_params = parse_template_for_base_info(template_path, 'marser')
    
    # Parse date range
    date_range = result['date_range']
    if ' to ' in date_range:
        start_date, end_date = date_range.split(' to ')
        date_spec = f"{start_date}/to/{end_date}"
    else:
        date_spec = date_range
    
    # Generate script for missing parameters
    for param in result['missing_params']:
        script_name = f"fetch_missing_{result['template_name']}_{param}.mars"
        target_file = generate_target_filename(result['template_name'], date_range.split(' to ')[0], param)
        
        script_content = ["retrieve,"]
        script_content.append(f"class={base_params['class']},")
        script_content.append(f"database={base_params['database']},")
        script_content.append(f"date={date_spec},")
        script_content.append(f"expver={base_params['expver']},")
        
        if result['levtype'] != 'sfc' and result['expected_levels']:
            levelist = '/'.join(result['expected_levels'])
            script_content.append(f"levelist={levelist},")
        
        script_content.append(f"levtype={result['levtype']},")
        script_content.append(f"origin={base_params['origin']},")
        script_content.append(f"param={param},")
        script_content.append("stream=oper,")
        
        # Add time and step for analysis data
        if result['type'] == 'an':
            script_content.append("time=0000/0300/0600/0900/1200/1500/1800/2100,")
            script_content.append("step=3,")
        elif result['type'] == 'fc':
            # Placeholder for forecast - depends on variable type
            script_content.append("# TODO: Add appropriate time/step for forecast data,")
        
        script_content.append(f"type={result['type']},")
        script_content.append(f'target="{target_file}"')
        
        scripts.append({
            'filename': script_name,
            'content': '\n'.join(script_content) + '\n',
            'target_file': target_file
        })
    
    return scripts


def generate_fetch_script_for_partial_missing(result, template_dir="mars_templates", output_dir="fetch_scripts"):
    """Generate fetch scripts for parameters missing on specific dates."""
    scripts = []
    
    if not result['params_missing_dates']:
        return scripts
    
    # Get base parameters from template  
    template_path = os.path.join(template_dir, result['template_name'])
    base_params = parse_template_for_base_info(template_path)
    
    # Group missing dates by parameter
    for param, missing_dates in result['params_missing_dates'].items():
        if param in result['missing_params']:
            # Skip completely missing params (handled by other function)
            continue
            
        # Create date specification from missing dates
        if len(missing_dates) == 1:
            date_spec = missing_dates[0]
        else:
            # For multiple dates, create range or list
            sorted_dates = sorted(missing_dates)
            date_spec = '/'.join(sorted_dates)
        
        script_name = f"fetch_partial_{result['template_name']}_{param}.mars"
        target_file = generate_target_filename(result['template_name'], sorted_dates[0], param)
        
        script_content = ["retrieve,"]
        script_content.append(f"class={base_params['class']},")
        script_content.append(f"database={base_params['database']},")
        script_content.append(f"date={date_spec},")
        script_content.append(f"expver={base_params['expver']},")
        
        if result['levtype'] != 'sfc' and result['expected_levels']:
            levelist = '/'.join(result['expected_levels'])
            script_content.append(f"levelist={levelist},")
        
        script_content.append(f"levtype={result['levtype']},")
        script_content.append(f"origin={base_params['origin']},")
        script_content.append(f"param={param},")
        script_content.append("stream=oper,")
        
        # Add time and step for analysis data
        if result['type'] == 'an':
            script_content.append("time=0000/0300/0600/0900/1200/1500/1800/2100,")
            script_content.append("step=3,")
        elif result['type'] == 'fc':
            # Placeholder for forecast - depends on variable type
            script_content.append("# TODO: Add appropriate time/step for forecast data,")
        
        script_content.append(f"type={result['type']},")
        script_content.append(f'target="{target_file}"')
        
        scripts.append({
            'filename': script_name,
            'content': '\n'.join(script_content) + '\n',
            'target_file': target_file
        })
    
    return scripts


def generate_fetch_script_for_level_issues(result, template_dir="mars_templates", output_dir="fetch_scripts"):
    """Generate fetch scripts for parameters with missing levels."""
    scripts = []
    
    if not result['level_issues']:
        return scripts
    
    # Get base parameters from template
    template_path = os.path.join(template_dir, result['template_name'])
    base_params = parse_template_for_base_info(template_path, 'marser')
    
    # Group missing levels by parameter and date
    for param, issues in result['level_issues'].items():
        # Collect all unique missing levels and dates
        missing_levels_by_date = defaultdict(set)
        all_dates = set()
        
        for issue in issues:
            date = issue['date']
            missing_levels = issue['missing_levels']
            all_dates.add(date)
            missing_levels_by_date[date].update(missing_levels)
        
        # Find levels that are missing across all dates
        common_missing_levels = None
        for date, levels in missing_levels_by_date.items():
            if common_missing_levels is None:
                common_missing_levels = set(levels)
            else:
                common_missing_levels &= set(levels)
        
        if not common_missing_levels:
            continue
        
        # Create date specification
        sorted_dates = sorted(all_dates)
        if len(sorted_dates) == 1:
            date_spec = sorted_dates[0]
        elif len(sorted_dates) > 1:
            date_spec = f"{sorted_dates[0]}/to/{sorted_dates[-1]}"
        
        # Create script for missing levels
        script_name = f"fetch_levels_{result['template_name']}_{param}.mars"
        target_file = generate_target_filename(result['template_name'], sorted_dates[0], param, "missing_levels")
        
        script_content = ["retrieve,"]
        script_content.append(f"class={base_params['class']},")
        script_content.append(f"database={base_params['database']},")
        script_content.append(f"date={date_spec},")
        script_content.append(f"expver={base_params['expver']},")
        
        # Only include the missing levels
        levelist = '/'.join(sorted(common_missing_levels))
        script_content.append(f"levelist={levelist},")
        
        script_content.append(f"levtype={result['levtype']},")
        script_content.append(f"origin={base_params['origin']},")
        script_content.append(f"param={param},")
        script_content.append("stream=oper,")
        
        # Add time and step for analysis data
        if result['type'] == 'an':
            script_content.append("time=0000/0300/0600/0900/1200/1500/1800/2100,")
            script_content.append("step=3,")
        elif result['type'] == 'fc':
            # Placeholder for forecast - depends on variable type
            script_content.append("# TODO: Add appropriate time/step for forecast data,")
        
        script_content.append(f"type={result['type']},")
        script_content.append(f'target="{target_file}"')
        
        scripts.append({
            'filename': script_name,
            'content': '\n'.join(script_content) + '\n',
            'target_file': target_file
        })
    
    return scripts


def main():
    parser = argparse.ArgumentParser(
        description='Generate MARS fetch scripts from missing data JSON files',
        epilog='''
Examples:
  # Generate fetch scripts from JSON file
  python3 generate_fetch_scripts.py -j missing_1986_01.json
  
  # Generate scripts with custom output directory
  python3 generate_fetch_scripts.py -j missing_1986_01.json -o my_fetch_scripts
  
  # Generate only scripts for missing parameters (not partial or level issues)
  python3 generate_fetch_scripts.py -j missing_1986_01.json --missing-only
  
  # Generate scripts with custom template directory
  python3 generate_fetch_scripts.py -j missing_1986_01.json -t /path/to/templates
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--json-file', '-j', required=True,
                       help='JSON file containing missing data analysis')
    parser.add_argument('--output-dir', '-o', default='fetch_scripts',
                       help='Output directory for generated scripts (default: fetch_scripts)')
    parser.add_argument('--template-dir', '-t', default='mars_templates',
                       help='Directory containing MARS templates (default: mars_templates)')
    parser.add_argument('--missing-only', action='store_true',
                       help='Generate scripts only for completely missing parameters')
    parser.add_argument('--partial-only', action='store_true',
                       help='Generate scripts only for parameters missing on specific dates')
    parser.add_argument('--levels-only', action='store_true',
                       help='Generate scripts only for level issues')
    parser.add_argument('--summary', '-s', action='store_true',
                       help='Show summary of what would be generated without creating files')
    
    args = parser.parse_args()
    
    # Load JSON data
    if not os.path.exists(args.json_file):
        print(f"Error: JSON file not found: {args.json_file}")
        return
    
    with open(args.json_file, 'r') as f:
        results = json.load(f)
    
    if not isinstance(results, list):
        print("Error: JSON file should contain a list of results")
        return
    
    # Create output directory
    if not args.summary and not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    all_scripts = []
    summary_stats = {
        'missing_params': 0,
        'partial_missing': 0,
        'level_issues': 0,
        'total_scripts': 0
    }
    
    print(f"Processing {len(results)} template results...")
    
    for result in results:
        if result is None:
            continue
            
        template_scripts = []
        
        # Generate scripts for different types of missing data
        if not args.partial_only and not args.levels_only:
            missing_scripts = generate_fetch_script_for_missing_params(
                result, args.template_dir, args.output_dir)
            template_scripts.extend(missing_scripts)
            summary_stats['missing_params'] += len(missing_scripts)
        
        if not args.missing_only and not args.levels_only:
            partial_scripts = generate_fetch_script_for_partial_missing(
                result, args.template_dir, args.output_dir)
            template_scripts.extend(partial_scripts)
            summary_stats['partial_missing'] += len(partial_scripts)
        
        if not args.missing_only and not args.partial_only:
            level_scripts = generate_fetch_script_for_level_issues(
                result, args.template_dir, args.output_dir)
            template_scripts.extend(level_scripts)
            summary_stats['level_issues'] += len(level_scripts)
        
        all_scripts.extend(template_scripts)
        
        if template_scripts:
            print(f"\nTemplate: {result['template_name']} ({result['levtype']}/{result['type']}/{result['stream']})")
            print(f"  Generated {len(template_scripts)} fetch script(s)")
            
            for script in template_scripts:
                print(f"    - {script['filename']} -> {script['target_file']}")
    
    summary_stats['total_scripts'] = len(all_scripts)
    
    # Show summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Scripts for missing parameters:     {summary_stats['missing_params']}")
    print(f"Scripts for partial missing data:   {summary_stats['partial_missing']}")
    print(f"Scripts for level issues:           {summary_stats['level_issues']}")
    print(f"Total fetch scripts:                {summary_stats['total_scripts']}")
    
    if args.summary:
        print(f"\n(Summary mode - no files created)")
        return
    
    if not all_scripts:
        print("\nNo fetch scripts needed - no missing data found!")
        return
    
    # Write all scripts to files
    print(f"\nWriting {len(all_scripts)} scripts to {args.output_dir}/...")
    
    for script in all_scripts:
        script_path = os.path.join(args.output_dir, script['filename'])
        with open(script_path, 'w') as f:
            f.write(script['content'])
    
    print(f"\nAll fetch scripts generated in: {args.output_dir}/")
    print(f"Run the scripts with: mars <script_name>")


if __name__ == "__main__":
    main()
