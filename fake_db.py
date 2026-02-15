"""
Faker Database Generator and Integration for DPDSL Testing
Creates realistic 100k employee database and runs comprehensive privacy tests
"""

import sqlite3
import sys
import os
from faker import Faker
import numpy as np
import pandas as pd

# === CONFIGURATION ===
# Use absolute path to database file (works in any working directory)
DB_NAME = os.path.join(os.path.dirname(__file__), 'employee_faker.db')
LOCALE = 'en_US'
RECORD_COUNT = 100_000  # 100k employees for realistic testing

# Metadata bounds for DP
METADATA_BOUNDS = {
    'Salary': 150000,  # Cap salary at $150k
    'Zip': 99999,
    'Hire_date': None,  # Dates handled separately
}


def generate_faker_database(force_regenerate=False):
    """
    Generate a realistic employee database using Faker.
    
    Args:
        force_regenerate: If True, drops existing table and recreates
    
    Returns:
        Connection to the database
    """
    
    print("\n" + "="*80)
    print("🎲 FAKER DATABASE GENERATOR")
    print("="*80)
    
    # Check if database already exists
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM employee")
        existing_count = cursor.fetchone()[0]
        
        if existing_count >= RECORD_COUNT and not force_regenerate:
            print(f"✅ Database already exists with {existing_count:,} records")
            print(f"   Using existing database: {DB_NAME}")
            return conn
        elif force_regenerate:
            print(f"♻️  Dropping existing table and regenerating...")
            cursor.execute("DROP TABLE IF EXISTS employee")
            conn.commit()
    except:
        pass
    
    # Create table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS employee (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        First_name TEXT(100) NOT NULL,
        Last_name TEXT(100) NOT NULL,
        Address TEXT,
        City TEXT,
        State TEXT,
        Zip INTEGER,
        Email TEXT NOT NULL UNIQUE,
        Hire_date TEXT,
        Salary REAL,
        Bank_account_number TEXT,
        Company_name TEXT,
        Job_title TEXT
    );
    """
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        print(f"✅ Connected to {DB_NAME}")
        print(f"   Table 'employee' ready")
    except sqlite3.Error as e:
        print(f"❌ FATAL ERROR: Could not create table: {e}")
        sys.exit(1)
    
    # Generate fake data
    sql_insert_query = """
    INSERT OR IGNORE INTO employee 
    (First_name, Last_name, Address, City, State, Zip, Email, Hire_date, 
     Salary, Bank_account_number, Company_name, Job_title) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    
    try:
        faker = Faker(LOCALE)
        used_emails = set()
        records_to_insert = []
        
        print(f"\n🎲 Generating {RECORD_COUNT:,} fake employee records...")
        print(f"   This will take 1-2 minutes...")
        
        # Add progress indicator
        batch_size = 10000
        
        while len(records_to_insert) < RECORD_COUNT:
            fname = faker.first_name()
            lname = faker.last_name()
            address = faker.street_address()
            city = faker.city()
            state = faker.state_abbr()
            zipcode = faker.zipcode_in_state(state)
            email = faker.email()
            
            # Prevent duplicate emails
            if email in used_emails:
                continue
            used_emails.add(email)
            
            hire_date = faker.date_between(start_date='-15y', end_date='today')
            
            # Realistic salary distribution (with some outliers)
            if len(records_to_insert) % 1000 == 0 and len(records_to_insert) < 100:
                # 0.1% are executives with very high salaries
                salary = float(faker.pydecimal(left_digits=7, right_digits=2, 
                                              positive=True, min_value=200000, max_value=2000000))
            else:
                # 99.9% have normal salaries
                salary = float(faker.pydecimal(left_digits=6, right_digits=2, 
                                              positive=True, min_value=30000, max_value=180000))

            bank_account = faker.iban()
            company = faker.company()
            job = faker.job()
            
            records_to_insert.append((
                fname, lname, address, city, state, zipcode,
                email, hire_date, salary, bank_account, company, job
            ))
            
            # Progress indicator
            if len(records_to_insert) % batch_size == 0:
                print(f"   Progress: {len(records_to_insert):,}/{RECORD_COUNT:,} records generated...")
        
        print(f"✅ Data generation complete: {len(records_to_insert):,} records")
        
    except Exception as e:
        print(f"❌ FATAL ERROR during data generation: {e}")
        conn.close()
        sys.exit(1)
    
    # Insert data
    try:
        print(f"\n💾 Inserting records into database...")
        print(f"   This may take 2-3 minutes for {RECORD_COUNT:,} records...")
        
        cursor.executemany(sql_insert_query, records_to_insert)
        conn.commit()
        
        print(f"✅ Successfully inserted {cursor.rowcount:,} records")
        
    except sqlite3.Error as e:
        print(f"❌ ERROR during insertion: {e}")
        conn.rollback()
        return None
    
    # Verify and show statistics
    print("\n" + "="*80)
    print("📊 DATABASE STATISTICS")
    print("="*80)
    
    cursor.execute("SELECT COUNT(*) FROM employee")
    total = cursor.fetchone()[0]
    
    cursor.execute("SELECT AVG(Salary), MIN(Salary), MAX(Salary) FROM employee")
    avg_sal, min_sal, max_sal = cursor.fetchone()
    
    cursor.execute("SELECT COUNT(DISTINCT State) FROM employee")
    num_states = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT Company_name) FROM employee")
    num_companies = cursor.fetchone()[0]
    
    print(f"Total records: {total:,}")
    print(f"Average salary: ${avg_sal:,.2f}")
    print(f"Salary range: ${min_sal:,.2f} - ${max_sal:,.2f}")
    print(f"States represented: {num_states}")
    print(f"Unique companies: {num_companies:,}")
    print("="*80)
    
    return conn


def test_dpdsl_with_faker_db():
    """
    Run DPDSL tests on the realistic Faker database.
    This shows how the system performs with 100k real-looking records.
    """
    
    print("\n\n" + "🔒"*40)
    print("DPDSL TESTS WITH REALISTIC 100K EMPLOYEE DATABASE")
    print("🔒"*40)
    
    # Import DPDSL components (assumes dp_rewriter.py is available)
    try:
        from dp_rewriter import rewrite_and_execute, BudgetManager
    except ImportError:
        print("⚠️  Could not import dp_rewriter. Make sure dp_rewriter.py is in the same directory.")
        return
    
    conn = generate_faker_database()
    
    if conn is None:
        print("❌ Could not connect to database")
        return
    
    # Test scenarios
    tests = [
        {
            'name': 'Average Salary (Protected)',
            'sql': 'SELECT AVG(PRIVATE Salary OF [1.0]) FROM employee',
            'description': 'Should clip outliers at $150k and add noise'
        },
        {
            'name': 'Max Salary (Outlier Protection)',
            'sql': 'SELECT MAX(PRIVATE Salary OF [0.5]) FROM employee',
            'description': 'Should prevent revealing billionaire CEO salaries'
        },
        {
            'name': 'Count Employees (Public)',
            'sql': 'SELECT COUNT(*) FROM employee',
            'description': 'Count is safe to reveal'
        },
        {
            'name': 'Salary by State (GROUP BY)',
            'sql': 'SELECT PUBLIC State, AVG(PRIVATE Salary OF [1.0]) FROM employee GROUP BY PUBLIC State',
            'description': 'Average salary per state with DP protection'
        },
        {
            'name': 'High Earners Count',
            'sql': 'SELECT COUNT(*) FROM employee WHERE PRIVATE Salary > 100000',
            'description': 'Count of high earners (if WHERE is supported)'
        },
        {
            'name': 'Direct Email Access (Should Block)',
            'sql': 'SELECT PRIVATE Email FROM employee',
            'description': 'Should block direct PII access'
        },
    ]
    
    budget = BudgetManager(max_budget=10.0)
    
    for i, test in enumerate(tests, 1):
        print(f"\n{'─'*80}")
        print(f"Test {i}: {test['name']}")
        print(f"{'─'*80}")
        print(f"Description: {test['description']}")
        print(f"Query: {test['sql']}")
        print()
        
        try:
            result, errors = rewrite_and_execute(
                test['sql'], 
                conn, 
                verbose=True,
                budget_manager=budget
            )
            
            if errors:
                print(f"🔒 BLOCKED/ERROR:")
                for err in errors:
                    print(f"   {err}")
            else:
                print(f"✅ SUCCESS")
                if result:
                    # Limit output for large results
                    if len(result) > 10:
                        print(f"📊 Result: {len(result)} rows (showing first 10)")
                        for row in result[:10]:
                            print(f"   {row}")
                        print(f"   ... ({len(result) - 10} more rows)")
                    else:
                        print(f"📊 Result: {result}")
        except Exception as e:
            print(f"❌ ERROR: {e}")
    
    # Budget report
    print("\n\n" + "="*80)
    print(budget.get_report())
    
    conn.close()


def analyze_privacy_risk():
    """
    Analyze the privacy risk in the Faker database.
    Shows what data could be leaked without DP protection.
    """
    
    print("\n\n" + "⚠️"*40)
    print("PRIVACY RISK ANALYSIS (Without DP Protection)")
    print("⚠️"*40)
    
    conn = generate_faker_database()
    
    print("\n1️⃣ Uniqueness Analysis:")
    print("="*80)
    
    # Check unique combinations that could identify individuals
    queries = {
        'Unique email addresses': 'SELECT COUNT(DISTINCT Email) FROM employee',
        'Unique (First_name, Last_name)': 'SELECT COUNT(*) FROM (SELECT First_name, Last_name FROM employee GROUP BY First_name, Last_name HAVING COUNT(*) = 1)',
        'Unique (City, Job_title)': 'SELECT COUNT(*) FROM (SELECT City, Job_title FROM employee GROUP BY City, Job_title HAVING COUNT(*) = 1)',
        'Unique (Company, Salary)': 'SELECT COUNT(*) FROM (SELECT Company_name, Salary FROM employee GROUP BY Company_name, Salary HAVING COUNT(*) = 1)',
    }
    
    cursor = conn.cursor()
    for desc, query in queries.items():
        cursor.execute(query)
        count = cursor.fetchone()[0]
        total = 100000
        print(f"   {desc}: {count:,} ({count/total*100:.1f}% of records)")
    
    print("\n2️⃣ Sensitive Data Exposure:")
    print("="*80)
    
    # Show what sensitive data exists
    cursor.execute("SELECT First_name, Last_name, Email, Salary, Bank_account_number FROM employee LIMIT 5")
    print("   Sample records (showing what PRIVATE data could leak):")
    print(f"   {'Name':<25} {'Email':<30} {'Salary':<15} {'Bank Account':<30}")
    print("   " + "-"*100)
    for row in cursor.fetchall():
        fname, lname, email, salary, bank = row
        print(f"   {fname} {lname:<20} {email:<30} ${salary:>12,.2f} {bank:<30}")
    
    print("\n3️⃣ Outlier Detection:")
    print("="*80)
    
    # Find extreme outliers
    cursor.execute("SELECT First_name, Last_name, Salary FROM employee ORDER BY Salary DESC LIMIT 10")
    print("   Top 10 highest earners (these need protection!):")
    print(f"   {'Name':<30} {'Salary':<15}")
    print("   " + "-"*45)
    for row in cursor.fetchall():
        fname, lname, salary = row
        print(f"   {fname} {lname:<25} ${salary:>12,.2f}")
    
    print("\n⚠️  Without DPDSL protection, all this data is exposed!")
    print("✅ With DPDSL: PII blocked, outliers clipped, noise added")
    print("="*80)
    
    conn.close()


def performance_benchmark():
    """
    Benchmark DPDSL performance on 100k records.
    """
    import time
    
    print("\n\n" + "⚡"*40)
    print("PERFORMANCE BENCHMARK (100k Records)")
    print("⚡"*40)
    
    from dp_rewriter import rewrite_and_execute
    
    conn = generate_faker_database()
    
    queries = [
        ('Simple aggregation', 'SELECT AVG(PRIVATE Salary OF [1.0]) FROM employee'),
        ('COUNT(*)', 'SELECT COUNT(*) FROM employee'),
        ('GROUP BY (50 states)', 'SELECT PUBLIC State, AVG(PRIVATE Salary OF [1.0]) FROM employee GROUP BY PUBLIC State'),
    ]
    
    print("\nQuery Performance:")
    print("="*80)
    print(f"{'Query Type':<30} {'Time (ms)':<15} {'Rows Processed':<20}")
    print("-"*80)
    
    for desc, query in queries:
        start = time.time()
        result, errors = rewrite_and_execute(query, conn, verbose=False)
        elapsed = (time.time() - start) * 1000  # Convert to ms
        
        if not errors:
            rows = len(result) if result else 0
            print(f"{desc:<30} {elapsed:>10.2f} ms   {100000:>15,} records")
        else:
            print(f"{desc:<30} {'ERROR':<15}")
    
    print("="*80)
    
    conn.close()


if __name__ == "__main__":
    # Generate the database
    conn = generate_faker_database(force_regenerate=False)
    
    if conn:
        conn.close()
        
        # Run comprehensive tests
        analyze_privacy_risk()
        test_dpdsl_with_faker_db()
        performance_benchmark()
        
        print("\n\n" + "="*80)
        print("✅ ALL TESTS COMPLETE")
        print(f"   Database: {DB_NAME} ({RECORD_COUNT:,} records)")
        print(f"   Ready for production testing!")
        print("="*80)