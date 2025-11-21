import time

start_time = time.perf_counter()

import json
import re

def clean_price(price_value):
    """
    Extract numeric value from any text. Keeps only digits and decimal point.
    """
    if price_value is None:
        return None
    
    # If already a number, return it
    if isinstance(price_value, (int, float)):
        return float(price_value)
    
    # Convert to string and keep only digits and decimal point
    price_str = str(price_value)
    numeric_only = re.sub(r'[^\d.]', '', price_str)
    
    # Handle multiple decimal points (keep only first)
    parts = numeric_only.split('.')
    if len(parts) > 2:
        numeric_only = parts[0] + '.' + ''.join(parts[1:])
    
    # Convert to float
    if numeric_only and numeric_only != '.':
        try:
            return float(numeric_only)
        except ValueError:
            return None
    
    return None

def clean_jsonl_prices(input_file, output_file, price_field='price'):
    """
    Read JSONL file, clean price field, and write to new file.
    """
    cleaned_count = 0
    error_count = 0
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        
        for line_num, line in enumerate(infile, 1):
            try:
                data = json.loads(line)
                
                # Clean the price field if it exists
                if price_field in data:
                    original = data[price_field]
                    cleaned = clean_price(original)
                    
                    if cleaned is not None:
                        data[price_field] = cleaned
                        if original != cleaned:
                            cleaned_count += 1
                    else:
                        # Keep original if cleaning failed
                        error_count += 1
                        print(f"Line {line_num}: Could not clean '{original}'")
                
                # Write cleaned data
                outfile.write(json.dumps(data) + '\n')
                
            except json.JSONDecodeError as e:
                print(f"Line {line_num}: JSON decode error - {e}")
                error_count += 1
    
    print(f"\nCleaning complete!")
    print(f"Cleaned: {cleaned_count} prices")
    print(f"Errors: {error_count}")
    return cleaned_count, error_count

clean_jsonl_prices(r'c:\Users\Darby\Downloads\Tools_and_Home_Improvement_3.jsonl', r'c:\Users\Darby\Downloads\Tools_and_Home_Improvement_2.jsonl')

end_time = time.perf_counter()
print(f"\nExecution time: {end_time - start_time:.2f} seconds")