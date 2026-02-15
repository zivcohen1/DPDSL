import sqlite3
from faker import Faker
import random
import sys
import os

# === CONFIGURATION ===
DB_NAME = 'employee_faker.db'
PROJECT_COUNT = 50000  # 50k projects
LOCALE = 'en_US'

def generate_projects_table():
   
    print("GENERATING LINKED 'PROJECTS' TABLE")


    if not os.path.exists(DB_NAME):
        print(f"❌ Error: Database '{DB_NAME}' not found.")
        print("   Please run your employee generator script first!")
        return

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    fake = Faker(LOCALE)

    # 1. Get existing Employee IDs to link against
    # We grab IDs so we can assign projects to REAL employees
    try:
        print("⏳ Loading existing employee IDs...")
        cursor.execute("SELECT id, First_name, Last_name FROM employee")
        employees = cursor.fetchall()
        
        if not employees:
            print("❌ Error: No employees found in the table.")
            return
            
        # Separate IDs for random choices
        all_ids = [emp[0] for emp in employees]
        print(f"✅ Loaded {len(all_ids):,} potential employees for assignment.")
        
    except sqlite3.Error as e:
        print(f"❌ Database Error: {e}")
        return

    # 2. Create the Projects Table
    create_table_sql = """
    DROP TABLE IF EXISTS projects;
    CREATE TABLE projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER,
        project_name TEXT,
        budget REAL,
        deadline DATE,
        FOREIGN KEY(employee_id) REFERENCES employee(id)
    );
    """
    cursor.executescript(create_table_sql)
    print("✅ Table 'projects' created.")

    # 3. Generate Project Records
    projects_data = []
    
    # --- SCENARIO 1: THE "WORKAHOLIC" (High Fan-out) ---
    # We take the first employee and give them MANY projects.
    # This tests if your Elastic Sensitivity correctly clips them.
    
    target_emp = employees[0] # First person in DB
    target_id = target_emp[0]
    target_name = f"{target_emp[1]} {target_emp[2]}"
    
    FAN_OUT_SIZE = 10 
    print(f"\n🎯 CREATING TEST SCENARIO: High Fan-out")
    print(f"   User: {target_name} (ID: {target_id})")
    print(f"   Assigning {FAN_OUT_SIZE} projects to them.")
    print("   (This allows you to demonstrate Elastic Sensitivity clipping!)")

    for i in range(FAN_OUT_SIZE):
        projects_data.append((
            target_id,
            f"Super Critical Project {i+1}",
            random.uniform(50000, 500000), # Budget
            fake.date_between(start_date='today', end_date='+2y')
        ))

    # --- SCENARIO 2: NORMAL DISTRIBUTION ---
    # Randomly assign the rest of the projects
    print(f"🎲 Generating remaining {PROJECT_COUNT - FAN_OUT_SIZE:,} random projects...")
    
    for _ in range(PROJECT_COUNT - FAN_OUT_SIZE):
        # Pick a random employee
        emp_id = random.choice(all_ids)
        
        # Project budgets (Gaussian distribution around $50k)
        budget = max(5000, random.gauss(50000, 20000))
        
        projects_data.append((
            emp_id,
            fake.bs().title(),  # "Synergized Bandwidth"
            budget,
            fake.date_between(start_date='today', end_date='+3y')
        ))

    # 4. Insert Data
    print(f"💾 Inserting {len(projects_data):,} records into database...")
    insert_sql = "INSERT INTO projects (employee_id, project_name, budget, deadline) VALUES (?, ?, ?, ?)"
    
    try:
        cursor.executemany(insert_sql, projects_data)
        conn.commit()
        print(f"✅ Successfully inserted all projects.")
    except sqlite3.Error as e:
        print(f"❌ Insertion Error: {e}")

    # 5. Create Indices for Performance
    print("⚡ optimizing database (Creating Index on employee_id)...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_project_emp ON projects(employee_id);")
    conn.commit()

    # 6. Verify and Report
    print("\n" + "="*60)
    print("🔍 VERIFICATION: Top Contributors (Fan-out Risks)")
    print("="*60)
    
    test_query = """
    SELECT e.First_name, e.Last_name, COUNT(p.id) as Project_Count
    FROM employee e
    JOIN projects p ON e.id = p.employee_id
    GROUP BY e.id
    ORDER BY Project_Count DESC
    LIMIT 5;
    """
    
    cursor.execute(test_query)
    results = cursor.fetchall()
    
    print(f"{'Name':<25} {'Projects Assigned':<20}")
    print("-" * 50)
    for row in results:
        print(f"{row[0] + ' ' + row[1]:<25} {row[2]:<20}")
        
    print("\n✅ Database is ready for JOIN testing!")
    conn.close()

if __name__ == "__main__":
    generate_projects_table()