import pandas as pd
import boto3
import io

# Function to read CSV data from S3
def read_csv_from_s3(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df

# Function to perform data validation
def validate_data(df):
    # Sample Validation 1: Check if required columns are present
    required_columns = ['user_id', 'email','phone']
    if not all(col in df.columns for col in required_columns):
        raise ValueError("Missing required columns")   
      
    # Sample Validation 2: Check for range constraints
    df['age'] = (pd.to_datetime('today').year - pd.to_datetime(df['date_of_birth']).dt.year)
    if (df['age'] < 0).any() or (df['age'] > 120).any():
        raise ValueError("Invalid age values")

    # Sample Validation 3: Phone number validation (assuming a standard format like xxx-xxx-xxxx)
    phone_regex = r'^\d{3}-\d{3}-\d{4}$'
    if not df['phone'].str.match(phone_regex).all():
        raise ValueError("Invalid phone number format")

    # Sample Validation 4: Check for duplicates
    if df.duplicated().any():
        raise ValueError("Duplicate rows detected")

# Function to perform data transformation
def transform_data(df):
    # Sample Transformation 1: Name concatenation
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
  
    # Sample Transformation 2: Standardize job titles (convert to lowercase)
    df['job_title'] = df['job_title'].str.lower()
  
    # Sample Transformation 3: Years of experience
    df['years_since_joining'] = (pd.Timestamp.now() - df['joining_date']).dt.days // 365
  
    # Sample Transformation 4: Filtering out rows with invalid values
    df = df[df['salary'] > 0]
    return df

# Function to write processed CSV data back to S3
def write_csv_to_s3(df, bucket, key):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3 = boto3.client('s3')
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=key)

# Main Lambda handler function
def lambda_handler(event, context):
    # Extracting bucket and key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Reading CSV data from S3
    df = read_csv_from_s3(bucket, key)

    # Validate data
    validate_data(df)
    
    # Transform data
    df_transform = transform_data(df)
    
    # Write transformed data back to S3
    transformed_key = 'transformed_' + key
    write_csv_to_s3(df_transform, bucket, transformed_key)
