# budget_analyzer.py
import os
from database import get_db_connection, get_user_profile
from datetime import datetime, timedelta
import json

# Reuse the client from the main AI categorizer
try:
    from ai_categorizer import openai_client, MODEL
except ImportError:
    openai_client = None
    MODEL = 'gpt-4o' # Fallback model

def get_historical_spending_summary(months: int = 6) -> dict:
    """
    Analyzes the last N full months of transactions to find the average
    monthly spend per category.
    """
    conn = get_db_connection()
    today = datetime.today()
    
    # Calculate the first day of the period N months ago
    first_day_of_current_month = today.replace(day=1)
    end_date = first_day_of_current_month - timedelta(days=1)
    
    start_date = end_date.replace(day=1)
    # Loop to go back N-1 more months
    for _ in range(months - 1):
        start_date = (start_date - timedelta(days=1)).replace(day=1)

    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    print(f"Analyzing spending from {start_date_str} to {end_date_str} ({months} months)...")

    excluded_categories = ('Income', 'Transfer', 'Card Payment', 'Financial Transactions', 'Savings')
    
    query = f"""
        SELECT
            category,
            SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as total_spent
        FROM transactions
        WHERE
            transaction_date BETWEEN ? AND ?
            AND category NOT IN ({','.join('?' for _ in excluded_categories)})
        GROUP BY category
    """
    params = [start_date_str, end_date_str] + list(excluded_categories)
    
    rows = conn.execute(query, params).fetchall()
    conn.close()

    summary = {row['category']: row['total_spent'] / months for row in rows if row['total_spent'] > 0}
    return summary

# In budget_analyzer.py, add this new function

def get_multi_period_spending_summary() -> dict:
    """
    Calculates average monthly spending over multiple periods (18, 6, 3, and 1 month)
    and consolidates them into a single structure.
    """
    periods = {'18m': 18, '6m': 6, '3m': 3, '1m': 1}
    all_categories = set()
    period_data = {}

    # Calculate summary for each period and collect all unique categories
    for key, months in periods.items():
        summary = get_historical_spending_summary(months=months)
        period_data[key] = summary
        all_categories.update(summary.keys())
    
    # Consolidate the data into a nested dictionary for the frontend
    consolidated_summary = {}
    for category in sorted(list(all_categories)):
        consolidated_summary[category] = {
            f'avg_18m': period_data['18m'].get(category, 0),
            f'avg_6m': period_data['6m'].get(category, 0),
            f'avg_3m': period_data['3m'].get(category, 0),
            f'avg_1m': period_data['1m'].get(category, 0),
        }
    return consolidated_summary

def propose_budget() -> dict:
    """
    Generates a budget proposal by sending detailed, multi-period historical
    spending data to an AI for trend analysis.
    """
    if not openai_client:
        return {"error": "OpenAI client is not initialized."}

    # --- UPGRADE ---
    # 1. Call our new function to get all four historical average periods.
    historical_summary = get_multi_period_spending_summary()
    profile = get_user_profile()
    monthly_income = (profile.get('annual_after_tax_income') or 0) / 12

    if not historical_summary:
        return {"error": "Not enough historical data to generate a budget."}
    
    if monthly_income <= 0:
        return {"error": "Please set your annual after-tax income in the User Profile."}

    # 2. Use a smarter prompt that tells the AI to look for trends.
    prompt = f"""
    You are a top-tier financial analyst creating a budget proposal for a client.
    Your client has a monthly after-tax income of ${monthly_income:,.2f}.

    **Your Task:**
    Analyze their historical spending trends and propose a realistic monthly budget.

    **Instructions:**
    1.  **Analyze Trends:** Review the client's spending averages over different periods (18m, 6m, 3m, 1m). Look for categories where spending is increasing or decreasing. For example, is the 3-month average for 'Dining' higher than the 18-month average?
    2.  **Propose a Budget:** Based on these trends and their income, create a sensible monthly budget. The output MUST be a JSON object where keys are the category names and values are the proposed budget limits.
    3.  **Output JSON Only:** Return ONLY the JSON object for the budget proposal. Do not include any explanations or other text.

    **Client's Historical Spending (Average Per Month):**
    {json.dumps(historical_summary, indent=2)}
    """
    
    try:
        response = openai_client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
        )
        content = response.choices[0].message.content
        proposed_budget = json.loads(content)
        return {"proposed_budget": proposed_budget}
    except Exception as e:
        print(f"AI budget proposal failed: {e}")
        return {"error": f"AI budget proposal failed: An error occurred."}

    # --- DYNAMIC CATEGORY LIST ---
    # Create a dynamic list of categories directly from the user's spending history
    expense_categories = sorted(list(historical_summary.keys()))

    # --- FINAL, HYPER-PERSONALIZED PROMPT ---
    prompt = f"""
    You are a wealth manager for a high-net-worth client with a substantial monthly after-tax income of ${monthly_income:,.2f}.

    **Your Task:**
    Conduct a deep dive into their finances and propose a sophisticated, realistic monthly budget.

    **Instructions:**
    1.  **Analyze the Client's Habits:** Review their 6-month average spending data to understand their lifestyle and priorities.
        - Client's Historical Spending: {json.dumps(historical_summary, indent=2)}

    2.  **Benchmark Against Reality:** Access your extensive knowledge of real-world financial data. Compare the client's spending against the habits of financially healthy, high-earning individuals who live a fulfilling life. **Do NOT use simplistic rules like 50/30/20.** Your analysis must be nuanced and based on realistic data for this income bracket.

    3.  **Create a Comprehensive Budget:** Propose a monthly budget limit for **every single one of the client's actual expense categories** listed below. 
        - Expense Categories: {expense_categories}

    4.  **Prioritize Savings (Crucial):** You must also propose a substantial and achievable monthly **"Savings"** goal. This should be a key part of the budget.

    **Output Instructions:**
    Return ONLY a single, valid JSON object mapping every single expense category AND a "Savings" category to a recommended monthly limit, rounded to the nearest dollar.
    """
    
    try:
        response = openai_client.chat.completions.create(
            model=MODEL,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": prompt}],
        )
        content = response.choices[0].message.content
        proposed_budget = json.loads(content or '{}')
        return {"status": "success", "proposed_budget": proposed_budget}
    except Exception as e:
        print(f"Error calling OpenAI for budget proposal: {e}")
        return {"error": f"AI service failed: {e}"}