from supabase import create_client, Client
import os
import pandas as pd

def analysis_report(supabase: Client, table_name: str = 'customer_churn'):
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    processed_dir = os.path.join(base_dir, 'data', 'processed')
    os.makedirs(processed_dir, exist_ok=True)
    
    data = supabase.table(table_name).select('*').execute()
    
    df = pd.DataFrame(data.data)

    # ----- 1. Churn Percentage -----
    churn_percentage = (df['churn'].value_counts().get('Yes', 0) / df.shape[0]) * 100
    churn_percentage = round(churn_percentage, 2)

    # ----- 2. Average Monthly Charges per Contract -----
    avg_monthly_by_contract = df.groupby('contract')['monthlycharges'].mean().round(2)

    # ----- 3. Customer Segment Counts -----
    customer_segments = df['tenure_group'].value_counts()

    # ----- 4. Internet Service Distribution -----
    internet_service_dist = df['internetservice'].value_counts()

    # ----- 5. Pivot Table: Churn vs Tenure Group -----
    pivot_churn_tenure = pd.pivot_table(
        df,
        values='id',
        index='tenure_group',
        columns='churn',
        aggfunc='count',
        fill_value=0
    )

    # -------------------------------------------------
    # Build final summary table in a flattened format
    # -------------------------------------------------

    summary_rows = []

    # Churn percentage
    summary_rows.append(["churn_percentage", churn_percentage])

    # Avg monthly charges per contract
    for contract_type, value in avg_monthly_by_contract.items():
        summary_rows.append([f"avg_monthly_{contract_type.replace(' ', '_').lower()}", value])

    # Customer segments count
    for seg, count in customer_segments.items():
        summary_rows.append([f"segment_count_{seg.lower()}", count])

    # Internet service distribution
    for service, count in internet_service_dist.items():
        summary_rows.append([f"internet_service_{service.replace(' ', '_').lower()}", count])

    # Pivot table values
    for tenure_group in pivot_churn_tenure.index:
        for churn_value in pivot_churn_tenure.columns:
            summary_rows.append([
                f"pivot_{tenure_group.lower()}_{churn_value.lower()}",
                pivot_churn_tenure.loc[tenure_group, churn_value]
            ])

    # Convert to DataFrame
    summary_df = pd.DataFrame(summary_rows, columns=["metric", "value"])
    
    processed_path = os.path.join(processed_dir, 'analysis_summary.csv')
    summary_df.to_csv(processed_path, index=False)
    
    print(f"Analysis is done and saved the report at '{processed_path}'")
    

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    
    client = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
    analysis_report(client)