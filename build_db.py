"""
Build script for Render deployment.
Generates the Faker database and projects table before the app starts.
"""
from fake_db import generate_faker_database
from projects_db import generate_projects_table

if __name__ == "__main__":
    print("=== Building database for deployment ===")
    conn = generate_faker_database(force_regenerate=False)
    if conn:
        conn.close()
    generate_projects_table()
    print("=== Database build complete ===")
