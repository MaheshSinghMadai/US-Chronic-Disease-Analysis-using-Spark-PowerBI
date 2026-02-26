import os
import shutil
import glob

def save_single_parquet(dataframe, output_path, filename, compression = "snappy"):
    temp_dir = f"{output_path}_temp"
    
    # Write to temp directory with single partition
    dataframe.coalesce(1).write \
        .mode('overwrite') \
        .option("compression", compression) \
        .csv(temp_dir)
    
    # Find the CSV file
    part_files = glob.glob(os.path.join(temp_dir, "part-*.parquet"))
    if not part_files:
        raise FileNotFoundError(f"No Parquet part file found in {temp_dir}")

    parquet_file = part_files[0]

    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)

    # Copy to final location as a single parquet file
    final_path = os.path.join(output_path, f"{filename}.parquet")
    shutil.copy(parquet_file, final_path)

    # Clean up temp directory
    shutil.rmtree(temp_dir)
