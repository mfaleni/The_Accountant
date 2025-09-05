i am going to provide you with a whole bunch of files from an app that we have built partially together. I want you to analyze the whole list of files and the codes within it. Do not start until i give you the "Go Ahead". The idea is to run an audit on all these files and debug them completely. The application is working, but it is not stable, it is pulling data from several sources using different techniques and processes to get to the same result. I believe that after so much debugging we are duplicating processes that are contradictory to each other and are bugging the results.



* The first thing is to understand the intention, and understand what is the function of each file that i provide you with.

- Build a world renowned accountant that takes raw data from any bank and normalizes it, understanding that each bank or credit card uses different signage for different types of transactions, use different headers, all use different descriptions, etc...

      - We need to have different files for different functions, * a merchant extractor that cleans the name and extracts the correct name. * a categorizer that looks at the merchant and using open ai, searches the name and assigns a category and a subcategory. * an anti duplication file that prevents transactions from duplicated, looking at the same date with the exact same amount, from the exact same merchant.

      - Those processes build a database with a unique numerical sequential ID per each transaction. The data base needs to have; *the original transaction description, *the cleaned merchant name out of the description, * the normalized amount, * the category, *the subcategory., *Blank columns with the space for me to provide human input after the machine has done its job to correct categories, subcategories and cleaned names.

      - THE OUTMOST IMPORTANT part of this app is categorizing, once the categories are assigned, and not corrected by me, they become rules a single source of information is created. The WHOLY GRAIL CSV becomes the builder of the whole structure and the app needs to look into it for reference to all new transactions, it needs to be efficient enough to understand the merchants, and use a single streamlined process to extract the perfect merchant name each time. This is the machine learning module which needs to be fed constantly by the input of new data, the key is to have single sourcing of the data to ALWAYS have consistency accross all the processes. For this we need to be able to export all the transactions in bulk into a csv with the columns described before. If i dont input anu

      - Once the data is sorted, cleaned, categorized and corrected. We move on to the budget analysis. *We need to filter by totals for each category for the past 3-6-9-12-18 months, *With that data create an avg expensed in each category and subcategory for those periods as well. OPEN AI will then look at each of these, and compare with the worlds average for a family with our avg income over the last 18 months, without using any specific rules, only real data, and propose a budget for each category and subcategory, *based on the avg monthly spent over the said periods shown in a pop up table, and the proposed budget, i will then set a final budget.

      - We will then launch the app into render for online access and to coexist and interact with Plaid. Plaid will provide daily data access to all of my transactions from all of the accounts and update the app with those transactions.

      - Now with the app getting daily feeds from plaid, with a budget already built and agreed on, normalized transactions, and clean data, we will track the expenses via a tracking dashboard that will analyze my daily expenditures for the month, and project if i am going to be over or under budget. Provide suggestions where to cut down, and how to reduce or improve financial health.

      - As an extra function, the app will run a by quarterly analysis to study my financial health and provide real world suggestions from openai as to where do i need to improve.



* Explain the specific detailed function of each file

* Explain the specific detailed relationship of each file

* Provide that explanation in details

* Provide the issues, process duplications, bugs and improvements, detail what file to change, what file is extra, what do we need to change and why

* After we analyze i need you to rebuild the app, with the correct files and codes.




Show thinking
Of course. Here is a version of your request structured as a clear and actionable prompt for an AI assistant like Gemini or ChatGPT.

Project Directive: Audit and Rebuild of Personal Finance Application
Objective:
You are to act as a senior software developer tasked with auditing, debugging, and completely refactoring a personal finance application. I will provide you with all the current application files. The application is partially functional but unstable due to duplicated processes and contradictory logic developed during extensive debugging.

Your mission is to analyze the existing code, report your findings, and then rebuild the application into a stable, efficient, and streamlined system based on the detailed requirements below.

Workflow:
The project will proceed in two distinct phases. Do not start Phase 1 until I have uploaded all the files and given the command "Go Ahead."

Phase 1: Full Code Audit and Refactoring Plan
Once you receive the "Go Ahead," perform a comprehensive audit of all provided code. Your output for this phase must be a single, detailed report containing the following sections:

File Function Analysis:

For each file provided, explain its specific and detailed function within the application.

System Architecture and Interrelationships:

Explain in detail how all the files relate to and interact with one another. Describe the flow of data through the system, from initial import to the final output.

Issues and Recommendations Report:

Identify all bugs, process duplications (e.g., multiple functions attempting to clean the same data), and logical contradictions in the code.

Provide a detailed, actionable plan for improvement. For each issue, specify:

Which file(s) need to be changed.

Which file(s) are redundant and should be removed.

A clear explanation of why the changes are necessary to achieve stability and efficiency.

Phase 2: Application Rebuild
After I have reviewed and approved your audit from Phase 1, you will proceed with rebuilding the application according to your recommendations and the following core functional requirements.

Core Functional Requirements:

1. Data Processing Pipeline:

Bank Data Normalization: The app must ingest raw transaction data (e.g., from a CSV) from any bank or credit card and normalize it, successfully handling variations in headers, date formats, and transaction signage (e.g., debits/credits).

Merchant Extraction Module: Create a single, efficient module to clean raw transaction descriptions and extract a standardized merchant name.

AI Categorization Module: After a merchant is identified, use an OpenAI API call to assign a primary Category and a Subcategory.

Anti-Duplication Module: Implement a robust system to prevent duplicate transactions. A duplicate is defined as a transaction with the exact same Date, Amount, and Cleaned Merchant Name.

2. Database and Machine Learning Core:

Database Structure: All processed transactions will be stored in a primary database (the "WHOLY GRAIL CSV"). Each transaction entry must have a unique numerical ID and the following columns:

Original Transaction Description

Cleaned Merchant Name

Normalized Amount

Category (AI-assigned)

Subcategory (AI-assigned)

Corrected Merchant (Blank, for user input)

Corrected Category (Blank, for user input)

Corrected Subcategory (Blank, for user input)

The "Single Source of Truth" Logic: This is the most critical component. When a new transaction is processed, the system must first check the "WHOLY GRAIL CSV" for user-corrected entries associated with that Cleaned Merchant Name.

If a rule exists from a previous user correction, apply it directly without calling the OpenAI API.

If no rule exists, call the OpenAI API, and store the transaction for potential user correction.

This ensures that user input continuously trains the system, creating a consistent and self-improving categorization engine.

Bulk Data Export: The application must have a function to export the entire transaction database to a CSV file.

3. Budgeting and Financial Analysis:

Spending Analysis: Create a module that can filter all transactions and calculate the total and average spending for each category and subcategory over selectable time periods (e.g., past 3, 6, 9, 12, 18 months).

AI Budget Proposal:

The app will use OpenAI to analyze the user's historical spending averages.

The AI will compare this data to global averages for a household of a similar income (which I will provide) and propose a monthly budget for each category and subcategory.

User Budget Setting: Based on the historical data and the AI proposal displayed in a clear table, I will input and set a final budget.

4. Live Operations and Dashboard:

Deployment & Plaid Integration: The final application must be architected for deployment on a platform like Render and designed to integrate with Plaid for automated, daily fetching of new transactions.

Budget Tracking Dashboard: A front-end interface that tracks daily expenditures against the set budget, projecting whether I will be over or under budget for the current month.

AI Financial Advisor: The dashboard should include an AI-powered function that provides actionable suggestions on how to cut down on expenses and improve financial health. It should also run a more in-depth analysis on a quarterly basis to provide strategic financial advice.

Please confirm you have understood these instructions and await my file uploads.







The data processing pipeline is not necessarily limited to those modules, those are only examples. I would like to build the application with as many separate modules as possible, so that debugging is not as complex. I need the App, Script and Index files to fetch data from all the rest of the modules, without necessarily executing processes within them as i believe this was the source of the current issue. please take this into consideration and re-write the complete prompt




Show thinking
Of course. This is a crucial architectural decision that will significantly improve the stability and maintainability of your application. By treating the main files as orchestrators and enforcing a strict separation of concerns, we can prevent the kind of logic conflicts you're experiencing.

Here is the completely rewritten prompt, incorporating this modular architectural mandate.

Project Directive: Audit and Rebuild of a Modular Personal Finance Application
Objective:
You are to act as a senior software architect and Developer. Your mission is to audit, debug, and completely rebuild a personal finance application based on a strict, modular architecture. I will provide you with all the current application files. The existing application is unstable because core files (App, Script, Index) are performing data processing directly, leading to duplicated and contradictory logic.

Your primary goal is to refactor this into a system where dedicated, single-purpose modules perform all the processing, and the main files act only as orchestrators.

Workflow:
The project will proceed in two distinct phases. Do not start Phase 1 until I have uploaded all the files and given the command "Go Ahead."

Core Architectural Mandate: Strict Modularity
This is the most important rule for the entire project. The final application must adhere to the following principles:

Orchestrator Files (App, Script, Index, etc.): These core files are responsible only for controlling the flow of data. Their job is to:

Call a processing module.

Receive the processed data (the result) from that module.

Pass that result to the next appropriate module in the sequence.

They must not contain any logic for data manipulation, cleaning, or analysis themselves.

Worker Modules (e.g., merchant_extractor.js, categorizer.js): Each module must have a single, clearly defined responsibility. It receives data, performs its specific task, and returns the result. You are encouraged to create as many distinct modules as necessary to ensure each one is simple and easy to debug.

Phase 1: Full Code Audit and Refactoring Plan
Once you receive the "Go Ahead," perform a comprehensive audit of all provided code through the lens of our Core Architectural Mandate. Your output must be a single, detailed report containing:

File Function Analysis:

For each file, explain its intended function.

System Architecture and Interrelationships:

Map out the current data flow and explain how the files currently interact.

Issues and Recommendations Report:

Architectural Violations: Specifically identify every instance where orchestrator files (App, Script, Index, etc.) are performing processing logic instead of delegating to a module.

Bugs & Redundancies: Pinpoint all other bugs, process duplications, and logical contradictions.

Refactoring Blueprint: Provide a detailed, actionable plan. For each issue, specify:

What logic needs to be moved out of an orchestrator file and into a new or existing module.

Which files are redundant and can be removed.

A clear explanation of why each change is necessary to achieve a stable, modular architecture.

Phase 2: Modular Application Rebuild
After I have reviewed and approved your audit, you will rebuild the application according to your refactoring blueprint and the following functional requirements, strictly adhering to the modular architecture.

Required Modules & Functionality:

1. Data Processing Pipeline (Implemented as a sequence of independent modules):

Module: Data Ingestor: Handles loading raw transaction data (e.g., from CSVs) and passing it to the orchestrator.

Module: Data Normalizer: Takes raw data and standardizes it. Handles variations in headers, date formats, and transaction signage. Returns normalized data.

Module: Merchant Extractor: Takes a normalized transaction. Cleans the description field and extracts a standardized merchant name. Returns the transaction object with the cleaned merchant name added.

Module: Duplication Checker: Takes a transaction and checks it against the main database to see if it's a duplicate (same date, amount, merchant). Returns a flag indicating if it's a duplicate.

Module: Categorization Engine:

This module takes a transaction with a cleaned merchant name.

It first queries the "WHOLY GRAIL" database module (see below) for a user-defined rule for that merchant.

If a rule exists, it applies the category/subcategory and returns the result.

If no rule exists, it makes an API call to OpenAI to get a suggested category/subcategory, adds them to the transaction, and returns it.

2. Database & Machine Learning Core (Implemented as a data management module):

Module: Database Manager ("WHOLY GRAIL CSV"): This module will manage all interactions with the primary database. It will expose functions for the orchestrator to call, such as:

getTransaction(id)

addTransaction(data)

findRuleForMerchant(merchantName)

updateUserCorrections(id, corrections)

getAllTransactions()

The database schema must include fields for: ID, Original Description, Cleaned Merchant, Normalized Amount, AI Category, AI Subcategory, and blank fields for Corrected Merchant, Corrected Category, Corrected Subcategory.

Module: Data Exporter: A simple module that, when called, retrieves all data from the Database Manager and formats it into a CSV file for download.

3. Budgeting and Financial Analysis (Implemented as analytical modules):

Module: Spending Analyzer: Takes a time period (3, 6, 12 months) and a category/subcategory as input. It queries the Database Manager for relevant transactions, calculates totals and averages, and returns a structured data object with the results.

Module: AI Budget Advisor: Takes the output from the Spending Analyzer and a user's income. It calls OpenAI to compare spending against global averages and returns a structured budget proposal.

Module: Budget Manager: Allows the user to set, update, and retrieve their final budget goals.

4. Live Operations and Dashboard (Front-end orchestrator and UI components):

Plaid Integration Module: A dedicated module to handle all API interactions with Plaid for fetching daily transactions.

Budget Tracking & Projection Module: Takes current month's transactions and the user's set budget. It returns data for the dashboard, including current spending progress and end-of-month projections.

Financial Health Advisor Module: A module that runs quarterly analysis on historical spending and provides strategic, AI-generated financial advice.

Please confirm you have understood these revised instructions, with the critical emphasis on a strict orchestrator/module architecture. Then, await my file uploads.