import os
import shutil
import glob

def save_single_csv(dataframe, output_path, filename):
    """Save Spark DataFrame as single CSV file"""
    temp_dir = f"{output_path}_temp"
    
    # Write to temp directory with single partition
    dataframe.coalesce(1).write \
        .mode('overwrite') \
        .option('header', 'true') \
        .csv(temp_dir)
    
    # Find the CSV file
    csv_file = glob.glob(f'{temp_dir}/*.csv')[0]
    
    # Copy to final location
    final_path = f'{output_path}/{filename}.csv'
    os.makedirs(output_path, exist_ok=True)
    shutil.copy(csv_file, final_path)
    
    # Clean up temp
    shutil.rmtree(temp_dir)
