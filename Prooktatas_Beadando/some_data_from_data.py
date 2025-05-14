import csv
from sqlalchemy import create_engine, text

ENGINE = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')

def select_all():
    with ENGINE.connect() as conn:
        query = text("SELECT * FROM beadando.denormaliz_lt_hr_adatok")
        result = conn.execute(query)

        for row in result:
            print(row)

# 1. feladat
def most_saturated_department():
    question = "Első feladat: Melyik részlegben dolgozik a legtöbb munkavállaló?"
    query = text("""
                SELECT department, COUNT(*) AS employee_count
                FROM beadando.denormaliz_lt_hr_adatok
                GROUP BY department
                ORDER BY employee_count DESC
                LIMIT 1;
                """)
    with ENGINE.connect() as conn:
        result = conn.execute(query)
        my_list = []
        for row in result:
            my_list.append(f"Részleg: {row[0]}, Dolgozók száma: {row[1]}")

        return my_list

# 2. feladat
def day_off_count():
    question2 = "Második feladat: Kik azok a dolgozók, akik 2023-ban legalább 10 nap szabadságot vettek ki? "
    query = text("""
                select * from beadando.denormaliz_lt_hr_adatok
                where date_part('year', hire_date) = 2023 and absence_days >= 10;
                """)
    
    with ENGINE.connect() as conn:
        result = conn.execute(query)
        print(f"{question2}")
        my_list = []
        for row in result.mappings():
            my_list.append(f"{row['last_name']} {row['first_name']} aki {row['absence_days']} napot volt távol.")
        
        return my_list

# 3. feladat
def average_salary_by_department():
    question3 = "Harmadik feladat: Részlegek szerinti átlagfizetések kiszámítása:"
    query = text("""
                SELECT department, AVG(monthly_salary)
                FROM beadando.denormaliz_lt_hr_adatok
                group by department;
                """)
    
    with ENGINE.connect() as conn:
        result = conn.execute(query)
        print(f"{question3}")
        my_list = []
        for row in result:
            my_list.append(f"{row[0]} {row[1]}")

        return my_list

# 4. feladat 
def different_job_in_city():
    question4 = "Melyik városban dolgozik a legtöbb különböző munkakörű dolgozó? "
    query = text("""
                select location, count(distinct job) as job_count
                from beadando.denormaliz_lt_hr_adatok
                group by location
                order by location;
                """)
    
    with ENGINE.connect() as conn:
        result = conn.execute(query)
        print(f"{question4}")
        my_list = []
        for row in result:
            my_list.append(f"{row[0]} {row[1]}")
        
        return my_list

# 5. feladat Dolgozók listája, akik 2023-ban csatlakoztak a céghez, beosztásukkal és részlegükkel együtt 
def employee_join_twenty_twentytwo():
    question5 = "Dolgozók listája, akik 2023-ban csatlakoztak a céghez, beosztásukkal és részlegükkel együtt:"
    query = text("""
                select first_name, last_name, department, job
                from beadando.denormaliz_lt_hr_adatok dlha 
                where date_part('year', hire_date) = 2023 ;
                """)
    
    with ENGINE.connect() as conn:
        result = conn.execute(query)
        print(f"{question5}")
        my_list = []
        for row in result:
            my_list.append(f"Név: {row[1]} {row[0]} | Részleg: {row[2]} | Beosztás: {row[3]}")
            
        return my_list
        
            

def write_answer_to_file(file_path, content):
    try:
        with open(file_path, mode='w', encoding='utf-8') as file:
            for item in content:
                file.write(f"{item}\n")
        print(f"Sikeres: {file_path}")
    except Exception as e:
        print(f"Nem sikeres: {e}")

if __name__ == '__main__':
    content = most_saturated_department()
    file_name = "most_saturated_department"
    file_path = rf"E:\Suli\Prooktatas\results\{file_name}.txt"
    
    # select_all()
    # most_saturated_department()
    # day_off_count()
    # average_salary()
    # different_job_in_city()
    # employee_join_twenty_twentytwo()
    write_answer_to_file(file_path, content)