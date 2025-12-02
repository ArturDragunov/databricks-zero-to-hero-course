import boto3
import json
import time
import pyarrow.parquet as pq
from pathlib import Path

# Initialize Kinesis client
kinesis = boto3.client('kinesis', region_name='us-east-1')  # match your region
stream_name = 'telematics-stream-tmh'

# Read parquet files
data_dir = Path(r'C:\Users\dragu\Documents\GIT\databricks-zero-to-hero-course\data\telematics')
parquet_files = list(data_dir.glob('*.parquet'))

for i in range(len(parquet_files)):
  table = pq.read_table(parquet_files[i])
  df = table.to_pandas()
  if i > 0 :
    break
  # Send each row as a separate record
  for idx, row in df.iterrows():
    record = row.to_dict()
    
    kinesis.put_record(
      StreamName=stream_name,
      Data=json.dumps(record),
      PartitionKey=str(idx)
    )
    
    # Optional: add small delay to avoid throttling
    time.sleep(0.01)
    
    if idx % 100 == 0:
      print(f"Sent {idx} records from {parquet_files[i].name}")

print("All records sent!")