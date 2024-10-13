import pandas as pd

from extract_data import get_sales_dataframe

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df['discount'] = df['discount'].fillna(0)
    df['quantity']= df['quantity'].fillna(0)

    df.dropna(subset=['customer_id'], inplace=True)
    df.drop_duplicates(subset=['order_id'], inplace=True)

    print(f"Dataframe after cleaning: \n\n {df.head()} \n\n")
    return df

def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df['total_sales'] = df['quantity'] * df['price'] * (1 - df['discount'])
    df['order_date'] = pd.to_datetime(df['order_date'], format='%Y-%m-%d')

    print(f"Dataframe after transformation: \n\n {df.head()} \n\n")
    return df

def main():
    df = get_sales_dataframe()
    df = clean_dataframe(df)
    transform_dataframe(df)

    print("Data processing complete.")

if __name__ == '__main__':
    main()
