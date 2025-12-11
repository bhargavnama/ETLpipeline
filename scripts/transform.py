import pandas as pd 
import numpy as np
import os

def transform_data(raw_path):
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)
    
    df = pd.read_csv(raw_path)
    
    # Convert total charges column to numeric
    df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
    
    # Filling missing numeric values
    df['tenure'] = df['tenure'].fillna(df['tenure'].median())
    df['MonthlyCharges'] = df['MonthlyCharges'].fillna(df['MonthlyCharges'].median())
    df['TotalCharges'] = df['TotalCharges'].fillna(df['TotalCharges'].median())
    
    # Tenure Group
    conditions = [
        (df['tenure'] >= 0 ) & (df['tenure'] <= 12),
        (df['tenure'] >= 12) & (df['tenure'] <= 36),
        (df['tenure'] >= 37) & (df['tenure'] <= 60),
    ]

    choices = ['New', 'Regular', 'Loyal']

    df['tenure_group'] = np.select(conditions, choices, default='Champion')
    
    # Monthly Charge Segment
    conditions = [
    df['MonthlyCharges'] < 30,
        (df['MonthlyCharges'] >= 30) & (df['MonthlyCharges'] <= 70)
    ]

    choices = ['Low', 'Medium']

    df['monthly_charge_segment'] = np.select(conditions, choices, default='High')

    df[['MonthlyCharges', 'monthly_charge_segment']]
    
    # has internet service
    df['has_internet_service'] = np.where(
        df['InternetService'] == 'No', 0, 1
    )
    df[['InternetService', 'has_internet_service']]
    
    # is multiline user
    df['is_multi_line_user'] = np.where(
        df['MultipleLines'] == 'Yes', 1, 0
    )
    df[['MultipleLines', 'is_multi_line_user']]
    
    # contract type code
    df['contract_type_code'] = df['Contract'].map({
        'Month-to-month': 0,
        'One year': 1,
        'Two year': 2
    })
    
    # Dropping unnecessary fields
    df= df.drop(columns=['customerID', 'gender'], axis=1)
    
    # Saving the transformed data
    staged_path = os.path.join(staged_dir, "churn_transformed.csv")
    df.to_csv(staged_path, index=False) 
    
    print(f"Churn data is successfully transformed and saved to the path '{staged_path}'")
    
    return staged_path

if __name__ == "__main__":
    from extract import extract_data
    raw_path = extract_data()
    transform_data(raw_path)