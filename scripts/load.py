import pandas as pd
from supabase import create_client
import os
from dotenv import load_dotenv 

def get_supabase_client():
    load_dotenv()
    
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')
    
    if not url or not key:
      raise ValueError('Missing supabase key or url in the .env')
  
    return create_client(url, key)

def load_to_supabase(staged_path: str, table_name: str = "customer_churn"):
    if not os.path.abspath(staged_path):
        staged_path = os.path.abspath(os.path.dirname(__file__ ), staged_path)
        
    
    if not os.path.exists(staged_path):
        print(f"❌ Error: File not found at {staged_path}")
        print("ℹ️  Please run transform.py first to generate the transformed data")
        return
    
    try:
        supabase = get_supabase_client()
        
        batch_size = 200
        df = pd.read_csv(staged_path)
        total_rows = len(df)
        
        df.rename(columns={
            "MonthlyCharges": "monthlycharges",
            "TotalCharges": "totalcharges",
            "Churn": "churn",
            "InternetService": "internetservice",
            "Contract": "contract",
            "PaymentMethod": "paymentmethod",
        }, inplace=True)
        
        for i in range(0, total_rows, batch_size):
            batch = df[i:i+batch_size].copy()
            # Convert Nan to None
            batch = batch.where(pd.notnull(batch), None)
            records = df[
                [
                    'tenure',
                    'monthlycharges',
                    'totalcharges',
                    'churn',
                    'internetservice',
                    'contract',
                    'paymentmethod',
                    'tenure_group',
                    'monthly_charge_segment',
                    'has_internet_service',
                    'is_multi_line_user',
                    'contract_type_code',
                ]
            ].to_dict('records')

            
            try:
                response = supabase.table(table_name).insert(records).execute()
                if hasattr(response, 'error') and response.error:
                    print(f"⚠️  Error in batch {i//batch_size + 1}: {response.error}")
                else:
                    end = min(i + batch_size, total_rows)
                    print(f"✅ Inserted rows {i+1}-{end} of {total_rows}")
            except Exception as e:
                print(f"⚠️  Error in batch {i//batch_size + 1}: {str(e)}")
                continue
        print(f'Finshed loading data into "{table_name}')
        
    except Exception as e:
        print(f"Error loading data '{e}'")
        
        
if __name__ == "__main__":
    # Path relative to the script location
    staged_csv_path = os.path.join("..", "data", "staged", "churn_transformed.csv")
    load_to_supabase(staged_csv_path)