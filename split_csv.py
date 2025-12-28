#!/usr/bin/env python3
"""
CSV Splitter for CrossCloudDDT - Simple version

Splits mega-CSV into separate files based on object markers (01_Account, 02_Contact, etc.)
Each marker becomes a separate file with that exact name.
"""

import csv
import os
import sys
import re

def split_mega_csv(input_file, output_dir):
    if not os.path.exists(input_file):
        print(f"❌ Error: Input file not found: {input_file}")
        return
    
    os.makedirs(output_dir, exist_ok=True)
    print(f"📂 Reading: {input_file}")
    
    with open(input_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        header_row = next(reader)
        
        # Find object markers (format: XX_ObjectName)
        object_pattern = re.compile(r'^(\d+)_([A-Za-z0-9_]+)$')  # Allow digits in object names
        
        sections = []
        current_section = None
        
        for col_idx, cell in enumerate(header_row):
            match = object_pattern.match(cell)
            if match:
                # Save previous section
                if current_section:
                    current_section['end_col'] = col_idx
                    sections.append(current_section)
                
                # Start new section
                filename = f"{cell}.csv"  # Use marker as-is
                current_section = {
                    'filename': filename,
                    'start_col': col_idx + 1,
                    'headers': []
                }
            elif current_section:
                current_section['headers'].append(cell)
        
        # Save last section
        if current_section:
            current_section['end_col'] = len(header_row)
            sections.append(current_section)
        
        if not sections:
            print("⚠️ No object markers found (expected format: 01_Account, 02_Contact, etc.)")
            return
        
        # Auto-renumber to avoid collisions
        seen_filenames = {}
        renumbered = []
        counter = 1
        
        for section in sections:
            original_filename = section['filename']
            object_name = section['filename'].replace('.csv', '').split('_', 1)[-1]  # Extract "Account" from "01_Account.csv"
            
            # Create unique filename with sequential number
            new_filename = f"{counter:02d}_{object_name}.csv"
            renumbered.append({
                'original': original_filename,
                'new': new_filename,
                'section': section
            })
            section['filename'] = new_filename
            counter += 1
        
        print(f"✅ Found {len(sections)} sections, renumbered sequentially:")
        for item in renumbered:
            if item['original'] != item['new']:
                print(f"   - {item['original']} → {item['new']}")
            else:
                print(f"   - {item['new']}")
        
        # Open output files
        output_files = {}
        writers = {}
        
        for section in sections:
            filepath = os.path.join(output_dir, section['filename'])
            output_files[section['filename']] = open(filepath, 'w', newline='', encoding='utf-8')
            writers[section['filename']] = csv.writer(output_files[section['filename']])
            writers[section['filename']].writerow(section['headers'])
        
        # Process data rows
        row_count = 0
        for row in reader:
            row_count += 1
            
            for section in sections:
                start = section['start_col']
                end = section['end_col']
                data = row[start:end] if end <= len(row) else row[start:]
                
                # Pad if needed
                while len(data) < len(section['headers']):
                    data.append('')
                
                # Write if not empty
                if any(cell.strip() for cell in data):
                    writers[section['filename']].writerow(data)
        
        # Close files
        for f in output_files.values():
            f.close()
        
        print(f"✅ Processed {row_count} rows")
        print(f"📁 Output: {output_dir}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python split_csv.py input.csv output_folder/")
        sys.exit(1)
    
    split_mega_csv(sys.argv[1], sys.argv[2])
