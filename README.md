# THE Accountant - Local Deployment

This project is a personal finance application that acts as an AI-powered accountant. It allows you to import raw CSV transaction files from various banks, automatically parses them, and uses an AI service to intelligently categorize every transaction.

---

## Setup and Installation Instructions

Follow these steps to get the application running on your computer.

### Step 1: Set Up Project
1.  Create the folder structure and all the files listed in the project plan.
2.  Copy and paste the code from each of the provided blocks into the corresponding empty file.

### Step 2: Set Up Python Environment
1.  Open the **Terminal** application.
2.  Navigate to your project folder: `cd path/to/your/AI_Accountant`
3.  Create a Python virtual environment: `python3 -m venv venv`
4.  Activate the virtual environment: `source venv/bin/activate`

### Step 3: Install Dependencies
1.  With your virtual environment active, install all required Python libraries:
    ```bash
    pip install -r requirements.txt
    ```

### Step 4: Configure the AI Categorizer
1.  Open the `ai_categorizer.py` file.
2.  Locate the line `API_KEY = os.environ.get("OPENAI_API_KEY", "YOUR_AI_API_KEY_HERE")`.
3.  Replace `"YOUR_AI_API_KEY_HERE"` with your actual API key from OpenAI or another provider. For better security, consider setting it as an environment variable named `OPENAI_API_KEY`.

---

## How to Run the Application

1.  Make sure your virtual environment is active (you should see `(venv)` in your terminal prompt).
2.  In your terminal, inside the `AI_Accountant` directory, run the main `app.py` script:
    ```bash
    python app.py
    ```
3.  You will see output indicating the Flask app is running, likely on `http://127.0.0.1:5001`.
4.  Open your web browser (e.g., Chrome, Safari) and go to that address: **http://127.0.0.1:5001**

You should now see the AI Accountant application interface.

## How to Use the App
1.  **Add an Account:** Create an account (e.g., "Amex Gold") using the form.
2.  **Import CSV:** Select the account, choose the corresponding CSV file from your computer, and click "Upload CSV".
3.  **View Transactions:** The table will populate with the imported data, marked as "Uncategorized".
4.  **Categorize:** Click the "Categorize with AI" button. The app will send the transactions to the AI and update the categories in the table.
5.  **Correct & Teach:** If the AI makes a mistake, change the category using the dropdown on any transaction. Your correction is automatically saved as a rule for the future, making the app smarter over time.
