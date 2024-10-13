import boto3
import pandas as pd


s3_client = boto3.client('s3')

BUCKET_NAME = 'data-engineering-bucket-2024'
FILE_KEY = 'sales-mock-data.csv'



def get_sales_dataframe() -> pd.DataFrame:
    s3_client.download_file(BUCKET_NAME, FILE_KEY, 'local_sales_data.csv')
    df = pd.read_csv('local_sales_data.csv')

    print(f"Local sales data retrieved: \n\n {df.head()} \n\n")
    return df

def main():
    get_sales_dataframe()

if __name__ == '__main__':
    main()