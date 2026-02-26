import os
import shutil
import glob
import uuid
import time

def save_single_parquet(dataframe, output_path, filename, compression="snappy"):
    """
    Saves DataFrame as:
        <output_path>/<filename>.parquet

    Creates a unique temp folder inside output_path.
    Safely cleans up temp even on Windows.
    """

    # Ensure gold directory exists
    os.makedirs(output_path, exist_ok=True)

    # Create unique temp directory inside gold folder
    temp_dir = os.path.join(output_path, f"_temp_{filename}_{uuid.uuid4().hex}")

    # Write parquet
    (dataframe.coalesce(1)
        .write
        .mode("overwrite")
        .option("compression", compression)
        .parquet(temp_dir)
    )

    # Small wait (important on Windows — file system lag)
    time.sleep(1)

    # Find part file
    part_files = glob.glob(os.path.join(temp_dir, "part-*.parquet"))

    if not part_files:
        raise FileNotFoundError(f"No parquet part file found in {temp_dir}")

    part_file = part_files[0]
    final_path = os.path.join(output_path, f"{filename}.parquet")

    # Remove existing file safely
    if os.path.exists(final_path):
        try:
            os.remove(final_path)
        except PermissionError:
            time.sleep(1)
            os.remove(final_path)

    # Move parquet file
    shutil.move(part_file, final_path)

    # Clean temp directory safely
    try:
        shutil.rmtree(temp_dir)
    except PermissionError:
        time.sleep(1)
        shutil.rmtree(temp_dir, ignore_errors=True)