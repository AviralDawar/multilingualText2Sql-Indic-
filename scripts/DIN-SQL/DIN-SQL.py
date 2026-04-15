from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from db_utils import get_connection, get_default_config_path, load_config  # noqa: E402


# -------------------- Minimal DIN-SQL prompts --------------------
schema_linking_prompt = '''Table advisor, columns = [*,s_ID,i_ID]
Table classroom, columns = [*,building,room_number,capacity]
Table course, columns = [*,course_id,title,dept_name,credits]
Table department, columns = [*,dept_name,building,budget]
Table instructor, columns = [*,ID,name,dept_name,salary]
Table prereq, columns = [*,course_id,prereq_id]
Table section, columns = [*,course_id,sec_id,semester,year,building,room_number,time_slot_id]
Table student, columns = [*,ID,name,dept_name,tot_cred]
Table takes, columns = [*,ID,course_id,sec_id,semester,year,grade]
Table teaches, columns = [*,ID,course_id,sec_id,semester,year]
Table time_slot, columns = [*,time_slot_id,day,start_hr,start_min,end_hr,end_min]
Foreign_keys = [course.dept_name = department.dept_name,instructor.dept_name = department.dept_name,section.building = classroom.building,section.room_number = classroom.room_number,section.course_id = course.course_id,teaches.ID = instructor.ID,teaches.course_id = section.course_id,teaches.sec_id = section.sec_id,teaches.semester = section.semester,teaches.year = section.year,student.dept_name = department.dept_name,takes.ID = student.ID,takes.course_id = section.course_id,takes.sec_id = section.sec_id,takes.semester = section.semester,takes.year = section.year,advisor.s_ID = student.ID,advisor.i_ID = instructor.ID,prereq.prereq_id = course.course_id,prereq.course_id = course.course_id]
Q: "Find the buildings which have rooms with capacity more than 50."
A: Let’s think step by step. In the question "Find the buildings which have rooms with capacity more than 50.", we are asked:
"the buildings which have rooms" so we need column = [classroom.capacity]
"rooms with capacity" so we need column = [classroom.building]
Based on the columns and tables, we need these Foreign_keys = [].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = [50]. So the Schema_links are:
Schema_links: [classroom.building,classroom.capacity,50]

Table department, columns = [*,Department_ID,Name,Creation,Ranking,Budget_in_Billions,Num_Employees]
Table head, columns = [*,head_ID,name,born_state,age]
Table management, columns = [*,department_ID,head_ID,temporary_acting]
Foreign_keys = [management.head_ID = head.head_ID,management.department_ID = department.Department_ID]
Q: "How many heads of the departments are older than 56 ?"
A: Let’s think step by step. In the question "How many heads of the departments are older than 56 ?", we are asked:
"How many heads of the departments" so we need column = [head.*]
"older" so we need column = [head.age]
Based on the columns and tables, we need these Foreign_keys = [].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = [56]. So the Schema_links are:
Schema_links: [head.*,head.age,56]

Table department, columns = [*,Department_ID,Name,Creation,Ranking,Budget_in_Billions,Num_Employees]
Table head, columns = [*,head_ID,name,born_state,age]
Table management, columns = [*,department_ID,head_ID,temporary_acting]
Foreign_keys = [management.head_ID = head.head_ID,management.department_ID = department.Department_ID]
Q: "what are the distinct creation years of the departments managed by a secretary born in state 'Alabama'?"
A: Let’s think step by step. In the question "what are the distinct creation years of the departments managed by a secretary born in state 'Alabama'?", we are asked:
"distinct creation years of the departments" so we need column = [department.Creation]
"departments managed by" so we need column = [management.department_ID]
"born in" so we need column = [head.born_state]
Based on the columns and tables, we need these Foreign_keys = [department.Department_ID = management.department_ID,management.head_ID = head.head_ID].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = ['Alabama']. So the Schema_links are:
Schema_links: [department.Creation,department.Department_ID = management.department_ID,head.head_ID = management.head_ID,head.born_state,'Alabama']

Table Addresses, columns = [*,address_id,line_1,line_2,city,zip_postcode,state_province_county,country]
Table Candidate_Assessments, columns = [*,candidate_id,qualification,assessment_date,asessment_outcome_code]
Table Candidates, columns = [*,candidate_id,candidate_details]
Table Courses, columns = [*,course_id,course_name,course_description,other_details]
Table People, columns = [*,person_id,first_name,middle_name,last_name,cell_mobile_number,email_address,login_name,password]
Table People_Addresses, columns = [*,person_address_id,person_id,address_id,date_from,date_to]
Table Student_Course_Attendance, columns = [*,student_id,course_id,date_of_attendance]
Table Student_Course_Registrations, columns = [*,student_id,course_id,registration_date]
Table Students, columns = [*,student_id,student_details]
Foreign_keys = [Students.student_id = People.person_id,People_Addresses.address_id = Addresses.address_id,People_Addresses.person_id = People.person_id,Student_Course_Registrations.course_id = Courses.course_id,Student_Course_Registrations.student_id = Students.student_id,Student_Course_Attendance.student_id = Student_Course_Registrations.student_id,Student_Course_Attendance.course_id = Student_Course_Registrations.course_id,Candidates.candidate_id = People.person_id,Candidate_Assessments.candidate_id = Candidates.candidate_id]
Q: "List the id of students who never attends courses?"
A: Let’s think step by step. In the question "List the id of students who never attends courses?", we are asked:
"id of students" so we need column = [Students.student_id]
"never attends courses" so we need column = [Student_Course_Attendance.student_id]
Based on the columns and tables, we need these Foreign_keys = [Students.student_id = Student_Course_Attendance.student_id].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = []. So the Schema_links are:
Schema_links: [Students.student_id = Student_Course_Attendance.student_id]

Table Country, columns = [*,id,name]
Table League, columns = [*,id,country_id,name]
Table Player, columns = [*,id,player_api_id,player_name,player_fifa_api_id,birthday,height,weight]
Table Player_Attributes, columns = [*,id,player_fifa_api_id,player_api_id,date,overall_rating,potential,preferred_foot,attacking_work_rate,defensive_work_rate,crossing,finishing,heading_accuracy,short_passing,volleys,dribbling,curve,free_kick_accuracy,long_passing,ball_control,acceleration,sprint_speed,agility,reactions,balance,shot_power,jumping,stamina,strength,long_shots,aggression,interceptions,positioning,vision,penalties,marking,standing_tackle,sliding_tackle,gk_diving,gk_handling,gk_kicking,gk_positioning,gk_reflexes]
Table Team, columns = [*,id,team_api_id,team_fifa_api_id,team_long_name,team_short_name]
Table Team_Attributes, columns = [*,id,team_fifa_api_id,team_api_id,date,buildUpPlaySpeed,buildUpPlaySpeedClass,buildUpPlayDribbling,buildUpPlayDribblingClass,buildUpPlayPassing,buildUpPlayPassingClass,buildUpPlayPositioningClass,chanceCreationPassing,chanceCreationPassingClass,chanceCreationCrossing,chanceCreationCrossingClass,chanceCreationShooting,chanceCreationShootingClass,chanceCreationPositioningClass,defencePressure,defencePressureClass,defenceAggression,defenceAggressionClass,defenceTeamWidth,defenceTeamWidthClass,defenceDefenderLineClass]
Table sqlite_sequence, columns = [*,name,seq]
Foreign_keys = [Player_Attributes.player_api_id = Player.player_api_id,Player_Attributes.player_fifa_api_id = Player.player_fifa_api_id,League.country_id = Country.id,Team_Attributes.team_api_id = Team.team_api_id,Team_Attributes.team_fifa_api_id = Team.team_fifa_api_id]
Q: "List the names of all left-footed players who have overall rating between 85 and 90."
A: Let’s think step by step. In the question "List the names of all left-footed players who have overall rating between 85 and 90.", we are asked:
"names of all left-footed players" so we need column = [Player.player_name,Player_Attributes.preferred_foot]
"players who have overall rating" so we need column = [Player_Attributes.overall_rating]
Based on the columns and tables, we need these Foreign_keys = [Player_Attributes.player_api_id = Player.player_api_id].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = [left,85,90]. So the Schema_links are:
Schema_links: [Player.player_name,Player_Attributes.preferred_foot,Player_Attributes.overall_rating,Player_Attributes.player_api_id = Player.player_api_id,left,85,90]

Table advisor, columns = [*,s_ID,i_ID]
Table classroom, columns = [*,building,room_number,capacity]
Table course, columns = [*,course_id,title,dept_name,credits]
Table department, columns = [*,dept_name,building,budget]
Table instructor, columns = [*,ID,name,dept_name,salary]
Table prereq, columns = [*,course_id,prereq_id]
Table section, columns = [*,course_id,sec_id,semester,year,building,room_number,time_slot_id]
Table student, columns = [*,ID,name,dept_name,tot_cred]
Table takes, columns = [*,ID,course_id,sec_id,semester,year,grade]
Table teaches, columns = [*,ID,course_id,sec_id,semester,year]
Table time_slot, columns = [*,time_slot_id,day,start_hr,start_min,end_hr,end_min]
Foreign_keys = [course.dept_name = department.dept_name,instructor.dept_name = department.dept_name,section.building = classroom.building,section.room_number = classroom.room_number,section.course_id = course.course_id,teaches.ID = instructor.ID,teaches.course_id = section.course_id,teaches.sec_id = section.sec_id,teaches.semester = section.semester,teaches.year = section.year,student.dept_name = department.dept_name,takes.ID = student.ID,takes.course_id = section.course_id,takes.sec_id = section.sec_id,takes.semester = section.semester,takes.year = section.year,advisor.s_ID = student.ID,advisor.i_ID = instructor.ID,prereq.prereq_id = course.course_id,prereq.course_id = course.course_id]
Q: "Give the title of the course offered in Chandler during the Fall of 2010."
A: Let’s think step by step. In the question "Give the title of the course offered in Chandler during the Fall of 2010.", we are asked:
"title of the course" so we need column = [course.title]
"course offered in Chandler" so we need column = [SECTION.building]
"during the Fall" so we need column = [SECTION.semester]
"of 2010" so we need column = [SECTION.year]
Based on the columns and tables, we need these Foreign_keys = [course.course_id = SECTION.course_id].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = [Chandler,Fall,2010]. So the Schema_links are:
Schema_links: [course.title,course.course_id = SECTION.course_id,SECTION.building,SECTION.year,SECTION.semester,Chandler,Fall,2010]

Table city, columns = [*,City_ID,Official_Name,Status,Area_km_2,Population,Census_Ranking]
Table competition_record, columns = [*,Competition_ID,Farm_ID,Rank]
Table farm, columns = [*,Farm_ID,Year,Total_Horses,Working_Horses,Total_Cattle,Oxen,Bulls,Cows,Pigs,Sheep_and_Goats]
Table farm_competition, columns = [*,Competition_ID,Year,Theme,Host_city_ID,Hosts]
Foreign_keys = [farm_competition.Host_city_ID = city.City_ID,competition_record.Farm_ID = farm.Farm_ID,competition_record.Competition_ID = farm_competition.Competition_ID]
Q: "Show the status of the city that has hosted the greatest number of competitions."
A: Let’s think step by step. In the question "Show the status of the city that has hosted the greatest number of competitions.", we are asked:
"the status of the city" so we need column = [city.Status]
"greatest number of competitions" so we need column = [farm_competition.*]
Based on the columns and tables, we need these Foreign_keys = [farm_competition.Host_city_ID = city.City_ID].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = []. So the Schema_links are:
Schema_links: [city.Status,farm_competition.Host_city_ID = city.City_ID,farm_competition.*]

Table advisor, columns = [*,s_ID,i_ID]
Table classroom, columns = [*,building,room_number,capacity]
Table course, columns = [*,course_id,title,dept_name,credits]
Table department, columns = [*,dept_name,building,budget]
Table instructor, columns = [*,ID,name,dept_name,salary]
Table prereq, columns = [*,course_id,prereq_id]
Table section, columns = [*,course_id,sec_id,semester,year,building,room_number,time_slot_id]
Table student, columns = [*,ID,name,dept_name,tot_cred]
Table takes, columns = [*,ID,course_id,sec_id,semester,year,grade]
Table teaches, columns = [*,ID,course_id,sec_id,semester,year]
Table time_slot, columns = [*,time_slot_id,day,start_hr,start_min,end_hr,end_min]
Foreign_keys = [course.dept_name = department.dept_name,instructor.dept_name = department.dept_name,section.building = classroom.building,section.room_number = classroom.room_number,section.course_id = course.course_id,teaches.ID = instructor.ID,teaches.course_id = section.course_id,teaches.sec_id = section.sec_id,teaches.semester = section.semester,teaches.year = section.year,student.dept_name = department.dept_name,takes.ID = student.ID,takes.course_id = section.course_id,takes.sec_id = section.sec_id,takes.semester = section.semester,takes.year = section.year,advisor.s_ID = student.ID,advisor.i_ID = instructor.ID,prereq.prereq_id = course.course_id,prereq.course_id = course.course_id]
Q: "Find the id of instructors who taught a class in Fall 2009 but not in Spring 2010."
A: Let’s think step by step. In the question "Find the id of instructors who taught a class in Fall 2009 but not in Spring 2010.", we are asked:
"id of instructors who taught " so we need column = [teaches.id]
"taught a class in" so we need column = [teaches.semester,teaches.year]
Based on the columns and tables, we need these Foreign_keys = [].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = [Fall,2009,Spring,2010]. So the Schema_links are:
schema_links: [teaches.id,teaches.semester,teaches.year,Fall,2009,Spring,2010]

Table Accounts, columns = [*,account_id,customer_id,date_account_opened,account_name,other_account_details]
Table Customers, columns = [*,customer_id,customer_first_name,customer_middle_initial,customer_last_name,gender,email_address,login_name,login_password,phone_number,town_city,state_county_province,country]
Table Financial_Transactions, columns = [*,transaction_id,account_id,invoice_number,transaction_type,transaction_date,transaction_amount,transaction_comment,other_transaction_details]
Table Invoice_Line_Items, columns = [*,order_item_id,invoice_number,product_id,product_title,product_quantity,product_price,derived_product_cost,derived_vat_payable,derived_total_cost]
Table Invoices, columns = [*,invoice_number,order_id,invoice_date]
Table Order_Items, columns = [*,order_item_id,order_id,product_id,product_quantity,other_order_item_details]
Table Orders, columns = [*,order_id,customer_id,date_order_placed,order_details]
Table Product_Categories, columns = [*,production_type_code,product_type_description,vat_rating]
Table Products, columns = [*,product_id,parent_product_id,production_type_code,unit_price,product_name,product_color,product_size]
Foreign_keys = [Orders.customer_id = Customers.customer_id,Invoices.order_id = Orders.order_id,Accounts.customer_id = Customers.customer_id,Products.production_type_code = Product_Categories.production_type_code,Financial_Transactions.account_id = Accounts.account_id,Financial_Transactions.invoice_number = Invoices.invoice_number,Order_Items.order_id = Orders.order_id,Order_Items.product_id = Products.product_id,Invoice_Line_Items.product_id = Products.product_id,Invoice_Line_Items.invoice_number = Invoices.invoice_number,Invoice_Line_Items.order_item_id = Order_Items.order_item_id]
Q: "Show the id, the date of account opened, the account name, and other account detail for all accounts."
A: Let’s think step by step. In the question "Show the id, the date of account opened, the account name, and other account detail for all accounts.", we are asked:
"the id, the date of account opened, the account name, and other account detail for all accounts." so we need column = [Accounts.account_id,Accounts.account_name,Accounts.other_account_details,Accounts.date_account_opened]
Based on the columns and tables, we need these Foreign_keys = [].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = []. So the Schema_links are:
Schema_links: [Accounts.account_id,Accounts.account_name,Accounts.other_account_details,Accounts.date_account_opened]

Table city, columns = [*,City_ID,Official_Name,Status,Area_km_2,Population,Census_Ranking]
Table competition_record, columns = [*,Competition_ID,Farm_ID,Rank]
Table farm, columns = [*,Farm_ID,Year,Total_Horses,Working_Horses,Total_Cattle,Oxen,Bulls,Cows,Pigs,Sheep_and_Goats]
Table farm_competition, columns = [*,Competition_ID,Year,Theme,Host_city_ID,Hosts]
Foreign_keys = [farm_competition.Host_city_ID = city.City_ID,competition_record.Farm_ID = farm.Farm_ID,competition_record.Competition_ID = farm_competition.Competition_ID]
Q: "Show the status shared by cities with population bigger than 1500 and smaller than 500."
A: Let’s think step by step. In the question "Show the status shared by cities with population bigger than 1500 and smaller than 500.", we are asked:
"the status shared by cities" so we need column = [city.Status]
"cities with population" so we need column = [city.Population]
Based on the columns and tables, we need these Foreign_keys = [].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = [1500,500]. So the Schema_links are:
Schema_links: [city.Status,city.Population,1500,500]

'''


def _strip_step_by_step_language(prompt_text: str) -> str:
    return (
        prompt_text.replace("A: Let’s think step by step. ", "A: ")
        .replace("A: Let's think step by step. ", "A: ")
    )


schema_linking_prompt = _strip_step_by_step_language(schema_linking_prompt)

classification_prompt = '''Q: "Find the buildings which have rooms with capacity more than 50."
schema_links: [classroom.building,classroom.capacity,50]
A: Let’s think step by step. The SQL query for the question "Find the buildings which have rooms with capacity more than 50." needs these tables = [classroom], so we don't need JOIN.
Plus, it doesn't require nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the questions = [""].
So, we don't need JOIN and don't need nested queries, then the the SQL query can be classified as "EASY".
Label: "EASY"

Q: "What are the names of all instructors who advise students in the math depart sorted by total credits of the student."
schema_links: [advisor.i_id = instructor.id,advisor.s_id = student.id,instructor.name,student.dept_name,student.tot_cred,math]
A: Let’s think step by step. The SQL query for the question "What are the names of all instructors who advise students in the math depart sorted by total credits of the student." needs these tables = [advisor,instructor,student], so we need JOIN.
Plus, it doesn't need nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the questions = [""].
So, we need JOIN and don't need nested queries, then the the SQL query can be classified as "NON-NESTED".
Label: "NON-NESTED"

Q: "Find the room number of the rooms which can sit 50 to 100 students and their buildings."
schema_links: [classroom.building,classroom.room_number,classroom.capacity,50,100]
A: Let’s think step by step. The SQL query for the question "Find the room number of the rooms which can sit 50 to 100 students and their buildings." needs these tables = [classroom], so we don't need JOIN.
Plus, it doesn't require nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the questions = [""].
So, we don't need JOIN and don't need nested queries, then the the SQL query can be classified as "EASY".
Label: "EASY"

Q: "How many courses that do not have prerequisite?"
schema_links: [course.*,course.course_id = prereq.course_id]
A: Let’s think step by step. The SQL query for the question "How many courses that do not have prerequisite?" needs these tables = [course,prereq], so we need JOIN.
Plus, it requires nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the questions = ["Which courses have prerequisite?"].
So, we need JOIN and need nested queries, then the the SQL query can be classified as "NESTED".
Label: "NESTED"

Q: "Find the title of course that is provided by both Statistics and Psychology departments."
schema_links: [course.title,course.dept_name,Statistics,Psychology]
A: Let’s think step by step. The SQL query for the question "Find the title of course that is provided by both Statistics and Psychology departments." needs these tables = [course], so we don't need JOIN.
Plus, it requires nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the questions = ["Find the titles of courses that is provided by Psychology departments"].
So, we don't need JOIN and need nested queries, then the the SQL query can be classified as "NESTED".
Label: "NESTED"

Q: "Find the id of instructors who taught a class in Fall 2009 but not in Spring 2010."
schema_links: [teaches.id,teaches.semester,teaches.year,Fall,2009,Spring,2010]
A: Let’s think step by step. The SQL query for the question "Find the id of instructors who taught a class in Fall 2009 but not in Spring 2010." needs these tables = [teaches], so we don't need JOIN.
Plus, it requires nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the questions = ["Find the id of instructors who taught a class in Spring 2010"].
So, we don't need JOIN and need nested queries, then the the SQL query can be classified as "NESTED".
Label: "NESTED"

Q: "Find the name of the department that offers the highest total credits?"
schema_links: [course.dept_name,course.credits]
A: Let’s think step by step. The SQL query for the question "Find the name of the department that offers the highest total credits?." needs these tables = [course], so we don't need JOIN.
Plus, it doesn't require nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the questions = [""].
So, we don't need JOIN and don't need nested queries, then the the SQL query can be classified as "EASY".
Label: "EASY"

Q: "What is the name of the instructor who advises the student with the greatest number of total credits?"
schema_links: [advisor.i_id = instructor.id,advisor.s_id = student.id,instructor.name,student.tot_cred ]
A: Let’s think step by step. The SQL query for the question "What is the name of the instructor who advises the student with the greatest number of total credits?" needs these tables = [advisor,instructor,student], so we need JOIN.
Plus, it doesn't need nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the questions = [""].
So, we need JOIN and don't need nested queries, then the the SQL query can be classified as "NON-NESTED".
Label: "NON-NESTED"

Q: "Find the total number of students and total number of instructors for each department."
schema_links = [department.dept_name = instructor.dept_name,student.id,student.dept_name = department.dept_name,instructor.id]
A: Let’s think step by step. The SQL query for the question "Find the total number of students and total number of instructors for each department." needs these tables = [department,instructor,student], so we need JOIN.
Plus, it doesn't need nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the questions = [""].
So, we need JOIN and don't need nested queries, then the the SQL query can be classified as "NON-NESTED".
Label: "NON-NESTED"

Q: "Give the name and building of the departments with greater than average budget."
schema_links: [department.budget,department.dept_name,department.building]
A: Let’s think step by step. The SQL query for the question "Give the name and building of the departments with greater than average budget." needs these tables = [department], so we don't need JOIN.
Plus, it requires nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the questions = ["What is the average budget of the departments"].
So, we don't need JOIN and need nested queries, then the the SQL query can be classified as "NESTED".
Label: "NESTED"

'''

classification_prompt = _strip_step_by_step_language(classification_prompt)

easy_prompt = '''Q: "Find the buildings which have rooms with capacity more than 50."
Schema_links: [classroom.building,classroom.capacity,50]
SQL: SELECT DISTINCT building FROM classroom WHERE capacity  >  50

Q: "Find the room number of the rooms which can sit 50 to 100 students and their buildings."
Schema_links: [classroom.building,classroom.room_number,classroom.capacity,50,100]
SQL: SELECT building ,  room_number FROM classroom WHERE capacity BETWEEN 50 AND 100

Q: "Give the name of the student in the History department with the most credits."
Schema_links: [student.name,student.dept_name,student.tot_cred,History]
SQL: SELECT name FROM student WHERE dept_name  =  'History' ORDER BY tot_cred DESC LIMIT 1

Q: "Find the total budgets of the Marketing or Finance department."
Schema_links: [department.budget,department.dept_name,Marketing,Finance]
SQL: SELECT sum(budget) FROM department WHERE dept_name  =  'Marketing' OR dept_name  =  'Finance'

Q: "Find the department name of the instructor whose name contains 'Soisalon'."
Schema_links: [instructor.dept_name,instructor.name,Soisalon]
SQL: SELECT dept_name FROM instructor WHERE name LIKE '%Soisalon%'

Q: "What is the name of the department with the most credits?"
Schema_links: [course.dept_name,course.credits]
SQL: SELECT dept_name FROM course GROUP BY dept_name ORDER BY sum(credits) DESC LIMIT 1

Q: "How many instructors teach a course in the Spring of 2010?"
Schema_links: [teaches.ID,teaches.semester,teaches.YEAR,Spring,2010]
SQL: SELECT COUNT (DISTINCT ID) FROM teaches WHERE semester  =  'Spring' AND YEAR  =  2010

Q: "Find the name of the students and their department names sorted by their total credits in ascending order."
Schema_links: [student.name,student.dept_name,student.tot_cred]
SQL: SELECT name ,  dept_name FROM student ORDER BY tot_cred

Q: "Find the year which offers the largest number of courses."
Schema_links: [SECTION.YEAR,SECTION.*]
SQL: SELECT YEAR FROM SECTION GROUP BY YEAR ORDER BY count(*) DESC LIMIT 1

Q: "What are the names and average salaries for departments with average salary higher than 42000?"
Schema_links: [instructor.dept_name,instructor.salary,42000]
SQL: SELECT dept_name ,  AVG (salary) FROM instructor GROUP BY dept_name HAVING AVG (salary)  >  42000

Q: "How many rooms in each building have a capacity of over 50?"
Schema_links: [classroom.*,classroom.building,classroom.capacity,50]
SQL: SELECT count(*) ,  building FROM classroom WHERE capacity  >  50 GROUP BY building

Q: "Find the names of the top 3 departments that provide the largest amount of courses?"
Schema_links: [course.dept_name,course.*]
SQL: SELECT dept_name FROM course GROUP BY dept_name ORDER BY count(*) DESC LIMIT 3

Q: "Find the maximum and average capacity among rooms in each building."
Schema_links: [classroom.building,classroom.capacity]
SQL: SELECT max(capacity) ,  avg(capacity) ,  building FROM classroom GROUP BY building

Q: "Find the title of the course that is offered by more than one department."
Schema_links: [course.title]
SQL: SELECT title FROM course GROUP BY title HAVING count(*)  >  1

'''

medium_prompt = '''Q: "Find the total budgets of the Marketing or Finance department."
Schema_links: [department.budget,department.dept_name,Marketing,Finance]
A: Let’s think step by step. For creating the SQL for the given question, we need to join these tables = []. First, create an intermediate representation, then use it to construct the SQL query.
Intermediate_representation: select sum(department.budget) from department  where  department.dept_name = \"Marketing\"  or  department.dept_name = \"Finance\"
SQL: SELECT sum(budget) FROM department WHERE dept_name  =  'Marketing' OR dept_name  =  'Finance'

Q: "Find the name and building of the department with the highest budget."
Schema_links: [department.budget,department.dept_name,department.building]
A: Let’s think step by step. For creating the SQL for the given question, we need to join these tables = []. First, create an intermediate representation, then use it to construct the SQL query.
Intermediate_representation: select department.dept_name , department.building from department  order by department.budget desc limit 1
SQL: SELECT dept_name ,  building FROM department ORDER BY budget DESC LIMIT 1

Q: "What is the name and building of the departments whose budget is more than the average budget?"
Schema_links: [department.budget,department.dept_name,department.building]
A: Let’s think step by step. For creating the SQL for the given question, we need to join these tables = []. First, create an intermediate representation, then use it to construct the SQL query.
Intermediate_representation:  select department.dept_name , department.building from department  where  @.@ > avg ( department.budget ) 
SQL: SELECT dept_name ,  building FROM department WHERE budget  >  (SELECT avg(budget) FROM department)

Q: "Find the total number of students and total number of instructors for each department."
Schema_links: [department.dept_name = student.dept_name,student.id,department.dept_name = instructor.dept_name,instructor.id]
A: Let’s think step by step. For creating the SQL for the given question, we need to join these tables = [department,student,instructor]. First, create an intermediate representation, then use it to construct the SQL query.
Intermediate_representation: "select count( distinct student.ID) , count( distinct instructor.ID) , department.dept_name from department  group by instructor.dept_name
SQL: SELECT count(DISTINCT T2.id) ,  count(DISTINCT T3.id) ,  T3.dept_name FROM department AS T1 JOIN student AS T2 ON T1.dept_name  =  T2.dept_name JOIN instructor AS T3 ON T1.dept_name  =  T3.dept_name GROUP BY T3.dept_name

Q: "Find the title of courses that have two prerequisites?"
Schema_links: [course.title,course.course_id = prereq.course_id]
A: Let’s think step by step. For creating the SQL for the given question, we need to join these tables = [course,prereq]. First, create an intermediate representation, then use it to construct the SQL query.
Intermediate_representation: select course.title from course  where  count ( prereq.* )  = 2  group by prereq.course_id
SQL: SELECT T1.title FROM course AS T1 JOIN prereq AS T2 ON T1.course_id  =  T2.course_id GROUP BY T2.course_id HAVING count(*)  =  2

Q: "Find the name of students who took any class in the years of 2009 and 2010."
Schema_links: [student.name,student.id = takes.id,takes.YEAR,2009,2010]
A: Let’s think step by step. For creating the SQL for the given question, we need to join these tables = [student,takes]. First, create an intermediate representation, then use it to construct the SQL query.
Intermediate_representation: select  distinct student.name from student  where  takes.year = 2009  or  takes.year = 2010
SQL: SELECT DISTINCT T1.name FROM student AS T1 JOIN takes AS T2 ON T1.id  =  T2.id WHERE T2.YEAR  =  2009 OR T2.YEAR  =  2010

Q: "list in alphabetic order all course names and their instructors' names in year 2008."
Schema_links: [course.title,course.course_id = teaches.course_id,teaches.id = instructor.id,instructor.name,teaches.year,2008]
A: Let’s think step by step. For creating the SQL for the given question, we need to join these tables = [course,teaches,instructor]. First, create an intermediate representation, then use it to construct the SQL query.
Intermediate_representation: select course.title , instructor.name from course  where  teaches.year = 2008  order by course.title asc
SQL: SELECT T1.title ,  T3.name FROM course AS T1 JOIN teaches AS T2 ON T1.course_id  =  T2.course_id JOIN instructor AS T3 ON T2.id  =  T3.id WHERE T2.YEAR  =  2008 ORDER BY T1.title

'''

hard_prompt = '''Q: "Find the title of courses that have two prerequisites?"
Schema_links: [course.title,course.course_id = prereq.course_id]
A: Let's think step by step. "Find the title of courses that have two prerequisites?" can be solved by knowing the answer to the following sub-question "What are the titles for courses with two prerequisites?".
The SQL query for the sub-question "What are the titles for courses with two prerequisites?" is SELECT T1.title FROM course AS T1 JOIN prereq AS T2 ON T1.course_id  =  T2.course_id GROUP BY T2.course_id HAVING count(*)  =  2
So, the answer to the question "Find the title of courses that have two prerequisites?" is =
Intermediate_representation: select course.title from course  where  count ( prereq.* )  = 2  group by prereq.course_id
SQL: SELECT T1.title FROM course AS T1 JOIN prereq AS T2 ON T1.course_id  =  T2.course_id GROUP BY T2.course_id HAVING count(*)  =  2

Q: "Find the name and building of the department with the highest budget."
Schema_links: [department.dept_name,department.building,department.budget]
A: Let's think step by step. "Find the name and building of the department with the highest budget." can be solved by knowing the answer to the following sub-question "What is the department name and corresponding building for the department with the greatest budget?".
The SQL query for the sub-question "What is the department name and corresponding building for the department with the greatest budget?" is SELECT dept_name ,  building FROM department ORDER BY budget DESC LIMIT 1
So, the answer to the question "Find the name and building of the department with the highest budget." is =
Intermediate_representation: select department.dept_name , department.building from department  order by department.budget desc limit 1
SQL: SELECT dept_name ,  building FROM department ORDER BY budget DESC LIMIT 1

Q: "Find the title, credit, and department name of courses that have more than one prerequisites?"
Schema_links: [course.title,course.credits,course.dept_name,course.course_id = prereq.course_id]
A: Let's think step by step. "Find the title, credit, and department name of courses that have more than one prerequisites?" can be solved by knowing the answer to the following sub-question "What is the title, credit value, and department name for courses with more than one prerequisite?".
The SQL query for the sub-question "What is the title, credit value, and department name for courses with more than one prerequisite?" is SELECT T1.title ,  T1.credits , T1.dept_name FROM course AS T1 JOIN prereq AS T2 ON T1.course_id  =  T2.course_id GROUP BY T2.course_id HAVING count(*)  >  1
So, the answer to the question "Find the name and building of the department with the highest budget." is =
Intermediate_representation: select course.title , course.credits , course.dept_name from course  where  count ( prereq.* )  > 1  group by prereq.course_id 
SQL: SELECT T1.title ,  T1.credits , T1.dept_name FROM course AS T1 JOIN prereq AS T2 ON T1.course_id  =  T2.course_id GROUP BY T2.course_id HAVING count(*)  >  1

Q: "Give the name and building of the departments with greater than average budget."
Schema_links: [department.dept_name,department.building,department.budget]
A: Let's think step by step. "Give the name and building of the departments with greater than average budget." can be solved by knowing the answer to the following sub-question "What is the average budget of departments?".
The SQL query for the sub-question "What is the average budget of departments?" is SELECT avg(budget) FROM department
So, the answer to the question "Give the name and building of the departments with greater than average budget." is =
Intermediate_representation: select department.dept_name , department.building from department  where  @.@ > avg ( department.budget )
SQL: SELECT dept_name ,  building FROM department WHERE budget  >  (SELECT avg(budget) FROM department)

Q: "Find the id of instructors who taught a class in Fall 2009 but not in Spring 2010."
Schema_links: [teaches.id,teaches.semester,teaches.YEAR,Fall,2009,Spring,2010]
A: Let's think step by step. "Find the id of instructors who taught a class in Fall 2009 but not in Spring 2010." can be solved by knowing the answer to the following sub-question "Find the id of instructors who taught a class in Spring 2010".
The SQL query for the sub-question "Find the id of instructors who taught a class in Spring 2010" is SELECT id FROM teaches WHERE semester  =  'Spring' AND YEAR  =  2010
So, the answer to the question "Find the id of instructors who taught a class in Fall 2009 but not in Spring 2010." is = 
Intermediate_representation: select teaches.ID from teaches  where  teaches.semester = \"Fall\"  and  teaches.year = 2009  and  teaches.semester != \"Spring\"  and  teaches.year = 2010 
SQL: SELECT id FROM teaches WHERE semester  =  'Fall' AND YEAR  =  2009 EXCEPT SELECT id FROM teaches WHERE semester  =  'Spring' AND YEAR  =  2010

Q: "Find the name of the courses that do not have any prerequisite?"
Schema_links: [course.title,course.course_id]
A: Let's think step by step. "Find the name of the courses that do not have any prerequisite?" can be solved by knowing the answer to the following sub-question "What are the courses that have any prerequisite?".
The SQL query for the sub-question "What are the courses that have any prerequisite?" is SELECT course_id FROM prereq
So, the answer to the question "Find the name of the courses that do not have any prerequisite?" is =
Intermediate_representation: select course.title from course  where  @.@ not in prereq.course_id 
SQL: SELECT title FROM course WHERE course_id NOT IN (SELECT course_id FROM prereq)

Q: "Find the salaries of all distinct instructors that are less than the largest salary."
Schema_links: [instructor.salary]
A: Let's think step by step. "Find the salaries of all distinct instructors that are less than the largest salary." can be solved by knowing the answer to the following sub-question "What is the largest salary of instructors".
The SQL query for the sub-question "What is the largest salary of instructors" is SELECT max(salary) FROM instructor
So, the answer to the question "Find the salaries of all distinct instructors that are less than the largest salary." is =
Intermediate_representation: select  distinct instructor.salary from instructor  where  @.@ < max ( instructor.salary )
SQL: SELECT DISTINCT salary FROM instructor WHERE salary  <  (SELECT max(salary) FROM instructor)

Q: "Find the names of students who have taken any course in the fall semester of year 2003."
Schema_links: [student.id,student.name,takes.id,takes.semester,fall,2003]
A: Let's think step by step. "Find the names of students who have taken any course in the fall semester of year 2003." can be solved by knowing the answer to the following sub-question "Find the students who have taken any course in the fall semester of year 2003.".
The SQL query for the sub-question "Find the students who have taken any course in the fall semester of year 2003." is SELECT id FROM takes WHERE semester  =  'Fall' AND YEAR  =  2003
So, the answer to the question "Find the names of students who have taken any course in the fall semester of year 2003." is =
Intermediate_representation: select student.name from student  where  takes.semester = \"Fall\"  and  takes.year = 2003
SQL: SELECT name FROM student WHERE id IN (SELECT id FROM takes WHERE semester  =  'Fall' AND YEAR  =  2003)

Q: "Find the minimum salary for the departments whose average salary is above the average payment of all instructors."
Schema_links: [instructor.salary,instructor.dept_name]
A: Let's think step by step. "Find the minimum salary for the departments whose average salary is above the average payment of all instructors." can be solved by knowing the answer to the following sub-question "What is the average payment of all instructors.".
The SQL query for the sub-question "What is the average payment of all instructors." is SELECT avg(salary) FROM instructor
So, the answer to the question "Find the minimum salary for the departments whose average salary is above the average payment of all instructors." is =
Intermediate_representation: select min(instructor.salary) , instructor.dept_name from instructor  where  avg ( instructor.salary )  > avg ( instructor.salary )   group by instructor.dept_name
SQL: SELECT min(salary) ,  dept_name FROM instructor GROUP BY dept_name HAVING avg(salary)  >  (SELECT avg(salary) FROM instructor)

Q: "What is the course title of the prerequisite of course Mobile Computing?"
Schema_links: [course.title,course.course_id = prereq.course_id,prereq.prereq_id,course.title,Mobile Computing]
A: Let's think step by step. "What is the course title of the prerequisite of course Mobile Computing?" can be solved by knowing the answer to the following sub-question "What are the ids of the prerequisite of course Mobile Computing?".
The SQL query for the sub-question "What are the ids of the prerequisite of course Mobile Computing?" is SSELECT T1.prereq_id FROM prereq AS T1 JOIN course AS T2 ON T1.course_id  =  T2.course_id WHERE T2.title  =  'Mobile Computing'
So, the answer to the question "What is the course title of the prerequisite of course Mobile Computing?" is =
Intermediate_representation: select course.title from course  where  @.@ in prereq.*  and  course.title = \"Mobile Computing\"
SQL: SELECT title FROM course WHERE course_id IN (SELECT T1.prereq_id FROM prereq AS T1 JOIN course AS T2 ON T1.course_id  =  T2.course_id WHERE T2.title  =  'Mobile Computing')

Q: "Give the title and credits for the course that is taught in the classroom with the greatest capacity."
Schema_links: [classroom.capacity,classroom.building = SECTION.building,classroom.room_number = SECTION.room_number,course.title,course.credits,course.course_id = SECTION.course_id]
A: Let's think step by step. "Give the title and credits for the course that is taught in the classroom with the greatest capacity." can be solved by knowing the answer to the following sub-question "What is the capacity of the largest room?".
The SQL query for the sub-question "What is the capacity of the largest room?" is (SELECT max(capacity) FROM classroom)
So, the answer to the question "Give the title and credits for the course that is taught in the classroom with the greatest capacity." is =
Intermediate_representation: select course.title , course.credits from classroom  order by classroom.capacity desc limit 1"
SQL: SELECT T3.title ,  T3.credits FROM classroom AS T1 JOIN SECTION AS T2 ON T1.building  =  T2.building AND T1.room_number  =  T2.room_number JOIN course AS T3 ON T2.course_id  =  T3.course_id WHERE T1.capacity  =  (SELECT max(capacity) FROM classroom)

'''

debug_prompt = """#### For the given question, use the provided tables, columns, foreign keys, and primary keys to fix the given SQLite SQL QUERY for any issues. If there are any problems, fix them. If there are no issues, return the SQLite SQL QUERY as is.
#### Use the following instructions for fixing the SQL QUERY:
1) Pay attention to the columns that are used for the JOIN by using the Foreign_keys.
2) Use DESC and DISTINCT when needed.
3) Pay attention to the columns that are used for the GROUP BY statement.
4) Pay attention to the columns that are used for the SELECT statement.
5) Only change the GROUP BY clause when necessary (Avoid redundant columns in GROUP BY).
6) The question may be in non-english language, the sql query has to be in english.
7) Don't include back-ticks around table names or columns names in the SQL query
"""

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"
SCHEMA_LINKING_MAX_TOKENS = 800
CLASSIFICATION_MAX_TOKENS = 400


def _openrouter_chat(prompt: str, api_key: str, model: str, max_tokens: int, stop: list[str] | None) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": max_tokens,
        "top_p": 1.0,
        "frequency_penalty": 0.0,
        "presence_penalty": 0.0,
    }
    if stop:
        payload["stop"] = stop

    response = requests.post(OPENROUTER_BASE_URL, headers=headers, data=json.dumps(payload), timeout=180)
    if response.status_code != 200:
        raise RuntimeError(f"OpenRouter API error: {response.status_code} - {response.text}")
    if(response.json()["choices"][0]["message"]["content"] == None): print(response.json())
    return response.json()["choices"][0]["message"]["content"]


class RateLimiter:
    def __init__(self, requests_per_minute: float):
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")
        self.interval = 60.0 / requests_per_minute
        self._lock = threading.Lock()
        self._next_allowed_time = 0.0

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                if now >= self._next_allowed_time:
                    self._next_allowed_time = now + self.interval
                    return
                wait_time = self._next_allowed_time - now
            time.sleep(wait_time)


def is_retryable_error(exc: Exception) -> bool:
    message = str(exc).lower()
    retry_markers = ["429", "rate limit", "timeout", "temporarily", "502", "503", "504", "connection reset"]
    return any(marker in message for marker in retry_markers)


def call_openrouter_with_retry(
    prompt: str,
    api_key: str,
    model: str,
    max_tokens: int,
    stop: list[str] | None,
    rate_limiter: RateLimiter,
    max_retries: int,
    retry_backoff_seconds: float,
) -> str:
    last_error: Exception | None = None
    current_stop = list(stop) if stop else None
    retried_without_stop = False

    for attempt in range(max_retries + 1):
        try:
            rate_limiter.acquire()
            return _openrouter_chat(
                prompt=prompt,
                api_key=api_key,
                model=model,
                max_tokens=max_tokens,
                stop=current_stop,
            )
        except Exception as exc:
            last_error = exc
            if attempt >= max_retries or not is_retryable_error(exc):
                raise
            time.sleep(retry_backoff_seconds * (2 ** attempt))
    if last_error is not None:
        raise last_error
    raise RuntimeError("OpenRouter call failed without an error")


def fetch_schema_frames(cursor, schema_name: str, db_label: str):
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema_name,),
    )
    tables = [r[0] for r in cursor.fetchall()]

    schema_rows = []
    pkey_rows = []
    fkey_rows = []

    for table in tables:
        schema_rows.append([db_label, table, "*", "text"])
        cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_name, table),
        )
        for col_name, data_type in cursor.fetchall():
            schema_rows.append([db_label, table, col_name, data_type])

        cursor.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """,
            (schema_name, table),
        )
        for (pk_col,) in cursor.fetchall():
            pkey_rows.append([db_label, table, pk_col])

        cursor.execute(
            """
            SELECT
              kcu.table_name AS first_table,
              ccu.table_name AS second_table,
              kcu.column_name AS first_col,
              ccu.column_name AS second_col
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
              AND tc.constraint_type = 'FOREIGN KEY'
            ORDER BY kcu.ordinal_position
            """,
            (schema_name, table),
        )
        for first_t, second_t, first_c, second_c in cursor.fetchall():
            fkey_rows.append([db_label, first_t, second_t, first_c, second_c])

    schema = pd.DataFrame(schema_rows, columns=["Database name", " Table Name", " Field Name", " Type"])
    primary = pd.DataFrame(pkey_rows, columns=["Database name", "Table Name", "Primary Key"])
    foreign = pd.DataFrame(
        fkey_rows,
        columns=["Database name", "First Table Name", "Second Table Name", "First Table Foreign Key", "Second Table Foreign Key"],
    )
    # print(foreign)
    return schema, primary, foreign


def find_foreign_keys_like(foreign: pd.DataFrame, db_name: str) -> str:
    df = foreign[foreign["Database name"] == db_name]
    if df.empty:
        return "[]"
    output = "["
    for _, row in df.iterrows():
        output += (
            row["First Table Name"]
            + "."
            + row["First Table Foreign Key"]
            + " = "
            + row["Second Table Name"]
            + "."
            + row["Second Table Foreign Key"]
            + ","
        )
    return output[:-1] + "]"


def find_fields_like(schema: pd.DataFrame, db_name: str) -> str:
    df = schema[schema["Database name"] == db_name]
    grouped = df.groupby(" Table Name")
    output = ""
    for name, group in grouped:
        output += f"Table {name}, columns = ["
        for _, row in group.iterrows():
            output += row[" Field Name"] + ","
        output = output[:-1] + "]\n"
    return output


def find_primary_keys_like(primary: pd.DataFrame, db_name: str) -> str:
    df = primary[primary["Database name"] == db_name]
    if df.empty:
        return "[]\n"
    output = "["
    for _, row in df.iterrows():
        output += row["Table Name"] + "." + row["Primary Key"] + ","
    return output[:-1] + "]\n"


# -------------------- Prompt builders --------------------
def schema_linking_prompt_maker(question: str, evidence: str, database: str, schema: pd.DataFrame, foreign: pd.DataFrame) -> str:
    fields = find_fields_like(schema, database)
    foreign = "Foreign_keys = " + find_foreign_keys_like(foreign, database) + "\n"
    # print(schema_linking_prompt + fields + foreign + f'Q: "{question}"\nEvidence: {evidence}\nA:')
    return schema_linking_prompt + fields + foreign + f'Q: "{question}"\nEvidence: {evidence}\nA:'


def classification_prompt_maker(
    question: str,
    evidence: str,
    database: str,
    schema_links: str,
    schema: pd.DataFrame,
    foreign: pd.DataFrame,
) -> str:
    fields = find_fields_like(schema, database)
    foreign = "Foreign_keys = " + find_foreign_keys_like(foreign, database) + "\n"
    return classification_prompt + fields + foreign + f'Q: "{question}"\nEvidence: {evidence}\nschema_links: {schema_links}\nA:'


def easy_prompt_maker(question: str, evidence: str, database: str, schema_links: str, schema: pd.DataFrame) -> str:
    fields = find_fields_like(schema, database)
    return easy_prompt + fields + f'Q: "{question}"\nEvidence: {evidence}\nSchema_links: {schema_links}\nSQL:'


def medium_prompt_maker(
    question: str,
    evidence: str,
    database: str,
    schema_links: str,
    schema: pd.DataFrame,
    foreign: pd.DataFrame,
) -> str:
    fields = find_fields_like(schema, database)
    foreign = "Foreign_keys = " + find_foreign_keys_like(foreign, database) + "\n"
    return medium_prompt + fields + foreign + f'Q: "{question}"\nEvidence: {evidence}\nSchema_links: {schema_links}\nA:'


def hard_prompt_maker(
    question: str,
    evidence: str,
    database: str,
    schema_links: str,
    schema: pd.DataFrame,
    foreign: pd.DataFrame,
) -> str:
    fields = find_fields_like(schema, database)
    foreign = "Foreign_keys = " + find_foreign_keys_like(foreign, database) + "\n"
    return hard_prompt + fields + foreign + f'Q: "{question}"\nEvidence: {evidence}\nSchema_links: {schema_links}\nA:'


def debug_prompt_maker(
    question: str,
    evidence: str,
    database: str,
    sql_text: str,
    schema: pd.DataFrame,
    foreign: pd.DataFrame,
    primary: pd.DataFrame,
) -> str:
    fields = find_fields_like(schema, database)
    fields += "Foreign_keys = " + find_foreign_keys_like(foreign, database) + "\n"
    fields += "Primary_keys = " + find_primary_keys_like(primary, database)
    return debug_prompt + fields + f"#### Question: {question}\nEvidence: {evidence}\n#### SQL QUERY:\n{sql_text}\n#### SQLite FIXED SQL QUERY\nSELECT "
    # prompt = instruction + fields+ '#### Question: ' + test_sample_text + '\n#### SQLite SQL QUERY\n' + sql +'\n#### SQLite FIXED SQL QUERY\nSELECT'



def parse_args() -> argparse.Namespace:
    default_questions = Path(__file__).resolve().parent / "INDIA_UDISE_SCHOOL_PROFILES_text2sql_20260224_153038.jsonl"
    default_output = Path(__file__).resolve().parent / "din_sql_predictions.jsonl"
    default_prompt_source = PROJECT_ROOT / "Few-shot-NL2SQL-with-prompting" / "DIN-SQL.py"

    parser = argparse.ArgumentParser(description="DIN-SQL pipeline with PostgreSQL schema introspection")
    parser.add_argument("--questions", type=str, default=str(default_questions), help="Questions JSON/JSONL path")
    parser.add_argument("--evidence", type=str, default=str(default_questions), help="Questions JSON/JSONL path")
    parser.add_argument("--output", type=str, default=str(default_output), help="Output JSON/JSONL path")
    parser.add_argument("--prompt-source", type=str, default=str(default_prompt_source), help="Reference DIN-SQL.py to load prompt blocks from")
    parser.add_argument("--config", type=str, help="Path to postgres credential json")
    parser.add_argument("--use-database", type=str, help="Target database name")
    parser.add_argument("--use-schema", type=str, help="Target schema name")
    parser.add_argument("--model", type=str, default=os.environ.get("OPENROUTER_MODEL", "deepseek/deepseek-chat"))
    parser.add_argument("--limit", type=int, help="Optional max number of questions")
    parser.add_argument("--max-workers", type=int, default=4, help="Number of questions to process in parallel")
    parser.add_argument("--max-requests-per-minute", type=float, default=60.0, help="Global OpenRouter request cap across all workers")
    parser.add_argument("--max-retries", type=int, default=6, help="Retries for retryable API failures")
    parser.add_argument("--retry-backoff-seconds", type=float, default=2.0, help="Base backoff seconds for retryable API failures")
    return parser.parse_args()


def load_questions(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".jsonl":
        return pd.read_json(path, lines=True)
    return pd.read_json(path)


def extract_label(classification: str) -> str:
    label = "NESTED"
    if "EASY" in classification:
        label = "EASY"
    elif "NON-NESTED" in classification:
        label = "NON-NESTED"
    return label


SQL_FENCE_RE = re.compile(r"```(?:sql)?\s*(.*?)\s*```", flags=re.IGNORECASE | re.DOTALL)
SQL_LABEL_RE = re.compile(
    r"(?is)(?:^|\n)\s*(?:SQL|SQLite(?:\s+FIXED)?\s+SQL\s+QUERY)\s*:?\s*(.*)"
)
SQL_SENTENCE_RE = re.compile(r"(?is)\bthe\s+sql\s+query\b.*?\bis\s+((?:with|select)\b.*)")
SQL_START_RE = re.compile(r"(?is)(?<![A-Z0-9_])(?:with|select)\b")
COMMENTARY_LINE_RE = re.compile(
    r"(?ix)^\s*(?:```|Q:|A:|Question:|Evidence:|Schema_links:|Explanation:|Reasoning:|Note:|Answer:|Output:|Result:|Final\s+SQL:)\s*"
)
SQL_LINE_START_RE = re.compile(
    r"""(?ix)
    ^\s*(?:
        with|select|from|where|group\s+by|order\s+by|having|limit|offset|fetch|
        union(?:\s+all)?|intersect|except|join|left(?:\s+outer)?\s+join|
        right(?:\s+outer)?\s+join|full(?:\s+outer)?\s+join|inner\s+join|
        cross\s+join|on|and|or|case|when|then|else|end|as|values|set|,|\(|\)
    )
    """
)
SQL_OPERATOR_HINT_RE = re.compile(
    r"""(?ix)
    (?:!=|<=|>=|[(),=*<>]|::|\b(?:in|like|between|is|null|distinct|count|sum|avg|min|max|asc|desc|exists)\b|'.*?'|".*?"|\b\w+\.\w+\b)
    """
)
SQL_IDENTIFIER_LINE_RE = re.compile(
    r"(?ix)^[A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*)*(?:\s+AS\s+[A-Za-z_][\w$]*)?$"
)
SQL_INLINE_STOP_RE = re.compile(
    r"(?is)\s+(?=(?:Explanation|Reasoning|Note|Answer|Output|Result)\s*:|This\s+query\b|The\s+query\b|It\s+returns\b)"
)


def _trim_to_first_sql_statement(text: str) -> str:
    in_single = False
    in_double = False
    in_backtick = False
    bracket_depth = 0
    paren_depth = 0
    i = 0

    while i < len(text):
        if not in_double and not in_backtick and text[i] == "'" and bracket_depth == 0:
            if in_single and i + 1 < len(text) and text[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
        elif not in_single and not in_backtick and text[i] == '"' and bracket_depth == 0:
            in_double = not in_double
        elif not in_single and not in_double and text[i] == "`" and bracket_depth == 0:
            in_backtick = not in_backtick
        elif not in_single and not in_double and not in_backtick:
            if text.startswith("```", i):
                return text[:i].strip()
            if text[i] == "[":
                bracket_depth += 1
            elif text[i] == "]" and bracket_depth > 0:
                bracket_depth -= 1
            elif text[i] == "(":
                paren_depth += 1
            elif text[i] == ")" and paren_depth > 0:
                paren_depth -= 1
            elif text[i] == ";" and paren_depth == 0 and bracket_depth == 0:
                return text[:i].strip()
        i += 1

    return text.strip()


def _line_looks_like_sql(line: str, previous_line: str | None) -> bool:
    stripped = line.strip()
    if not stripped or COMMENTARY_LINE_RE.match(stripped):
        return False
    if SQL_LINE_START_RE.match(stripped):
        return True
    if SQL_OPERATOR_HINT_RE.search(stripped):
        return True
    if previous_line:
        prev = previous_line.strip()
        if (
            re.search(r"(?ix)\b(?:select|by|having|and|or|on|when|then|else|set)\s*$", prev)
            or prev.endswith(",")
            or prev.endswith("(")
        ):
            return bool(SQL_IDENTIFIER_LINE_RE.match(stripped))
    return False


def _extract_sql_from_candidate(candidate: str) -> str:
    if not candidate:
        return ""

    text = candidate.replace("\r\n", "\n").replace("\r", "\n").strip()
    start_match = SQL_START_RE.search(text)
    if not start_match:
        return ""

    lines = text[start_match.start():].splitlines()
    collected: list[str] = []
    previous_line: str | None = None

    for line in lines:
        if "```" in line:
            line = line.split("```", maxsplit=1)[0]
            if line.strip():
                collected.append(line.rstrip())
            break
        if collected and not _line_looks_like_sql(line, previous_line):
            break
        collected.append(line.rstrip())
        previous_line = line

    sql = "\n".join(collected).strip()
    sql = _trim_to_first_sql_statement(sql)
    sql = SQL_INLINE_STOP_RE.split(sql, maxsplit=1)[0].strip()
    return sql.strip("`").strip()


def extract_sql_query(text: str | None) -> str:
    normalized = (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    candidates: list[str] = []
    candidates.extend(SQL_FENCE_RE.findall(normalized))
    candidates.extend(match.group(1) for match in SQL_LABEL_RE.finditer(normalized))
    candidates.extend(match.group(1) for match in SQL_SENTENCE_RE.finditer(normalized))
    candidates.append(normalized)

    for candidate in candidates:
        sql = _extract_sql_from_candidate(candidate)
        if sql:
            return sql

    return normalized


def process_question(
    args,
    idx: int,
    row: pd.Series,
    evidence_row: pd.Series,
    db_label: str,
    schema: pd.DataFrame,
    primary: pd.DataFrame,
    foreign: pd.DataFrame,
    api_key: str,
    model: str,
    rate_limiter: RateLimiter,
    max_retries: int,
    retry_backoff_seconds: float,
) -> dict[str, str]:
    question = str(row.get("question", "")).strip()
    evidence = str(evidence_row.get("evidence", "")).strip()
    pair_id = str(row.get("pair_id", "")).strip()
    gold_sql = str(row.get("sql", "")).strip()
    # db_label = str(row.get("db_id", "")).strip().lower()

    if not question:
        return {}

    print(f"[{idx}] {question}")
    try:
        out = call_openrouter_with_retry(
            schema_linking_prompt_maker(question, evidence, db_label, schema, foreign),
            api_key=api_key,
            model=model,
            max_tokens=SCHEMA_LINKING_MAX_TOKENS,
            stop=["Q:"],
            rate_limiter=rate_limiter,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
        )

        m = re.search(r"Schema_links\s*:\s*\*{0,2}\s*(\[.*?\])", out, flags=re.IGNORECASE | re.DOTALL)
        schema_links = m.group(1) if m else "[]"

        classification = call_openrouter_with_retry(
            classification_prompt_maker(question, evidence, db_label, schema_links, schema, foreign),
            api_key=api_key,
            model=model,
            max_tokens=CLASSIFICATION_MAX_TOKENS,
            stop=["Q:"],
            rate_limiter=rate_limiter,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
        )
        label = extract_label(classification)

        if label == "EASY":
            raw = call_openrouter_with_retry(
                easy_prompt_maker(question, evidence, db_label, schema_links, schema),
                api_key=api_key,
                model=model,
                max_tokens=10000,
                stop=["Q:"],
                rate_limiter=rate_limiter,
                max_retries=max_retries,
                retry_backoff_seconds=retry_backoff_seconds,
            )
        elif label == "NON-NESTED":
            raw = call_openrouter_with_retry(
                medium_prompt_maker(question, evidence, db_label, schema_links, schema, foreign),
                api_key=api_key,
                model=model,
                max_tokens=10000,
                stop=["Q:"],
                rate_limiter=rate_limiter,
                max_retries=max_retries,
                retry_backoff_seconds=retry_backoff_seconds,
            )
        else:
            raw = call_openrouter_with_retry(
                hard_prompt_maker(question, evidence, db_label, schema_links, schema, foreign),
                api_key=api_key,
                model=model,
                max_tokens=10000,
                stop=["Q:"],
                rate_limiter=rate_limiter,
                max_retries=max_retries,
                retry_backoff_seconds=retry_backoff_seconds,
            )

        sql_text = extract_sql_query(raw)

        fixed_body = call_openrouter_with_retry(
            debug_prompt_maker(question, evidence, db_label, sql_text, schema, foreign, primary),
            api_key=api_key,
            model=model,
            max_tokens=10000,
            stop=[],
            rate_limiter=rate_limiter,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
        ).strip()
        final_sql_text = extract_sql_query(fixed_body)
        print(f"[{idx}] completed")

        return {
            "question": question,
            "predicted_sql": final_sql_text,
            "raw_sql": sql_text,
            "gold_sql": gold_sql,
            "db_id": db_label,
            "evidence": evidence,
            "schema_links": schema_links,
            "predicted_class": label,
            "pair_id": pair_id,
            "error_info": None,
        }
    except Exception as exc:
        print(f"[{idx}] failed: {exc}")
        return {
            "question": question,
            "predicted_sql": "",
            "raw_sql": "",
            "gold_sql": gold_sql,
            "db_id": db_label,
            "evidence": evidence,
            "schema_links": "[]",
            "predicted_class": "",
            "pair_id": pair_id,
            "error_info": str(exc),
        }


def main() -> None:
    args = parse_args()
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY not found in environment")

    config_path = Path(args.config) if args.config else Path(get_default_config_path())
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    cfg = load_config(str(config_path))
    conn = get_connection(cfg, database=args.use_database.lower() if args.use_database else None)
    db_label = args.use_schema.lower()
    try:
        cursor = conn.cursor()
        schema, primary, foreign = fetch_schema_frames(cursor, args.use_schema.lower(), db_label)
    finally:
        conn.close()

    df = load_questions(Path(args.questions))
    edf = load_questions(Path(args.evidence))
    if args.limit:
        df = df.head(args.limit)
        edf = edf.head(args.limit)
    print(f"Loaded {len(df)} questions")
    rate_limiter = RateLimiter(args.max_requests_per_minute)
    indexed_rows = list(df.iterrows())
    indexed_erows = list(edf.iterrows())
    ordered_results: dict[int, dict[str, str]] = {}

    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
        future_to_meta = {}
        for (idx, row), (_, evidence_row) in zip(indexed_rows, indexed_erows):
            future = executor.submit(
                process_question,
                args,
                idx,
                row,
                evidence_row,
                db_label,
                schema,
                primary,
                foreign,
                api_key,
                args.model,
                rate_limiter,
                args.max_retries,
                args.retry_backoff_seconds,
            )
            future_to_meta[future] = (idx, row, evidence_row)

        for future in as_completed(future_to_meta):
            idx, row, evidence_row = future_to_meta[future]
            try:
                result = future.result()
            except Exception as exc:
                print(f"[{idx}] worker crashed: {exc}")
                result = {
                    "question": str(row.get("question", "")).strip(),
                    "predicted_sql": "",
                    "raw_sql": "",
                    "gold_sql": str(row.get("sql", "")).strip(),
                    "db_id": str(row.get("db_id", "")).strip().lower(),
                    "evidence": str(evidence_row.get("evidence", "")).strip(),
                    "schema_links": "[]",
                    "predicted_class": "",
                    "pair_id": str(row.get("pair_id", "")).strip(),
                    "error_info": f"worker crashed: {exc}",
                }
            if result:
                ordered_results[idx] = result

    results = [ordered_results[idx] for idx, _ in indexed_rows if idx in ordered_results]

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.suffix.lower() == ".jsonl":
        with out_path.open("w", encoding="utf-8") as f:
            for row in results:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    else:
        out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(results)} predictions to {out_path}")


if __name__ == "__main__":
    main()
