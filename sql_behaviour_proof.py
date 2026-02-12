import psycopg2

def run_experiment():
    # 1. CONNECT to the database
    # We use the credentials we created in step 2
    conn = psycopg2.connect(
        dbname="de_handbook",
        user="cliffe",
        password="BLOOMberg411",
        host="localhost"
    )
    conn.autocommit = True # Automatically save changes (good for learning, bad for banking apps)
    cur = conn.cursor()

    print("--- 1. SETUP: Resetting the Environment ---")
    # Clean slate: Drop tables if they exist so we can run this script multiple times
    cur.execute("DROP TABLE IF EXISTS orders;")
    cur.execute("DROP TABLE IF EXISTS customers;")

    # Create Tables
    cur.execute("""
        CREATE TABLE customers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            region VARCHAR(50)
        );
    """)
    cur.execute("""
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            customer_id INT REFERENCES customers(id),
            amount INT
        );
    """)

    # Seed Data (The Edge Case)
    # Alice (North) -> Has an order
    # Bob (South) -> No order
    cur.execute("INSERT INTO customers (name, region) VALUES ('Alice', 'North'), ('Bob', 'South');")
    
    # We need Alice's ID to give her an order. 
    # Let's assume Alice is ID 1 and Bob is ID 2 (standard for fresh tables)
    cur.execute("INSERT INTO orders (customer_id, amount) VALUES (1, 100);")
    print("Database seeded: Alice (North, 1 order), Bob (South, 0 orders)\n")


    # --- THE EXPERIMENTS ---

    # Query A: INNER JOIN
    print("--- Query A: INNER JOIN (The Trap) ---")
    cur.execute("""
        SELECT c.name, COUNT(o.id) 
        FROM customers c
        INNER JOIN orders o ON c.id = o.customer_id
        GROUP BY c.name;
    """)
    rows = cur.fetchall()
    print(f"Result: {rows}")
    print("Notice: Bob is missing entirely!\n")

    # Query B: LEFT JOIN
    print("--- Query B: LEFT JOIN (The Fix) ---")
    cur.execute("""
        SELECT c.name, COUNT(o.id) 
        FROM customers c
        LEFT JOIN orders o ON c.id = o.customer_id
        GROUP BY c.name;
    """)
    rows = cur.fetchall()
    print(f"Result: {rows}")
    print("Notice: Bob is here, with a count of 0.\n")

    # Query C: LEFT JOIN with WHERE (The Order of Operations Mistake)
    print("--- Query C: LEFT JOIN + WHERE region != 'North' ---")
    cur.execute("""
        SELECT c.name, COUNT(o.id) 
        FROM customers c
        LEFT JOIN orders o ON c.id = o.customer_id
        WHERE c.region != 'North'
        GROUP BY c.name;
    """)
    rows = cur.fetchall()
    print(f"Result: {rows}")
    print("Notice: We filtered AFTER the join. Alice (North) was removed.\n")

    # Query D: LEFT JOIN with AND (The Architect's Solution)
    print("--- Query D: LEFT JOIN + AND region != 'North' ---")
    cur.execute("""
        SELECT c.name, COUNT(o.id) 
        FROM customers c
        LEFT JOIN orders o ON c.id = o.customer_id AND c.region != 'North'
        GROUP BY c.name;
    """)
    rows = cur.fetchall()
    print(f"Result: {rows}")
    print("Notice: Alice is still here! But her order count is 0 because the match condition failed, not the row retrieval.\n")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_experiment()
