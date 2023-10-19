-- Consecutive Numbers
    -- Use self join , if its 3 consecutive then join 3 times
    select DISTINCT a.num from logs a join logs b on a.id  = b.id+1 and a.num = b.num 
    join logs c on a.logs = c.logs+2 and a.num = c.num 

====================================================================================================================================
-- find the no of days since the frist login for each player in postgres use window function 
        SELECT
        player_id,
        login_date,
        (login_date - MIN(login_date) OVER (PARTITION BY player_id)) AS days_since_first_login
        FROM
        user_logins;

-- to find the days since last login
        SELECT
        player_id,
        login_date,
        (login_date - LAG(login_date, 1, login_date) OVER (PARTITION BY player_id ORDER BY login_date)) AS days_since_last_login
        FROM
        user_logins;

-- find all players who logged in twice on same day
        SELECT
        player_id,
        login_date
        FROM
        user_logins
        GROUP BY
        player_id,
        login_date
        HAVING
        COUNT(*) >= 2;

    --SOLN2
        SELECT
        player_id,
        login_date
        FROM (
        SELECT
            player_id,
            login_date,
            COUNT(*) OVER (PARTITION BY player_id, login_date) AS login_count
        FROM
            user_logins
        ) AS subquery
        WHERE login_count = 2;

-- to find if player did 3 consective logins
        SELECT DISTINCT
        player_id,
        login_date AS first_login,
        lead_login AS second_login,
        lead_lead_login AS third_login
        FROM (
        SELECT
            player_id,
            login_date,
            LEAD(login_date, 1) OVER (PARTITION BY player_id ORDER BY login_date) AS lead_login,
            LEAD(login_date, 2) OVER (PARTITION BY player_id ORDER BY login_date) AS lead_lead_login
        FROM
            user_logins
        ) AS subquery
        WHERE
        lead_login = login_date + INTERVAL '1 day'
        AND lead_lead_login = login_date + INTERVAL '2 days';


-- to find if player has logged in a day before or day after 

        SELECT DISTINCT
        player_id,
        login_date AS current_login,
        lag_login AS login_before,
        lead_login AS login_after
        FROM (
        SELECT
            player_id,
            login_date,
            LAG(login_date, 1) OVER (PARTITION BY player_id ORDER BY login_date) AS lag_login,
            LEAD(login_date, 1) OVER (PARTITION BY player_id ORDER BY login_date) AS lead_login
        FROM
            user_logins
        ) AS subquery
        WHERE
        (login_date = lag_login + INTERVAL '1 day')
        OR (login_date = lead_login - INTERVAL '1 day');





======================================================================================================================================
-- Employees earning more than their managers
    --id | Name | Salary | Manager_id
   -- Use Self Join
   select Name from employee e1 join employee e2 on  e1.Manager_id = e2.id and e1.salary > e2.salary

======================================================================================================================================
-- Duplicate emails
    -- ID | EMAIL
    select email from (
        select email, count(emial) as records from email_table group by 1 
    )temp where temp.records > 1

    -- Another way is using HAVING
    select email, count(email) as records from email_table group by 1 having count(email) >1 

    -- Another way is self join
    select distinct email from email_table e1 join email_table e2 on e1.email = e2.email and e1.id != e2.id

    --- Third Way
    SELECT Email
FROM (
  SELECT
    Email,
    ROW_NUMBER() OVER (PARTITION BY Email ORDER BY Id) AS RN
  FROM Person
) ranked
WHERE RN > 1;

======================================================================================================================================
-- Customers who never Order 
    -- Customer Table -- >  ID | Name
    -- Orders Table -- > ID | Customer_ID
    select c.name from Customers c left join Orders o on c.id = o.id where o.Customer_ID is null
    -- second way is to use not in

======================================================================================================================================
-- Department HIghest Salary
-- Employee Table ID|NAME|SALARY|DEPT_ID
-- Dept Table ID|NAME

SELECT Department, Employee, Salary
FROM (SELECT D.Name AS Department, E.Name AS Employee, E.Salary,
        ROW_NUMBER() OVER (PARTITION BY D.Name ORDER BY E.Salary DESC) AS RN
        FROM Employee E JOIN Department D ON E.DepartmentId = D.Id
) ranked
WHERE RN = 1;

-- 2nd method
SELECT D.Name AS Department, E.Name AS Employee, E.Salary FROM Employee E JOIN Department D ON E.DepartmentId = D.Id
where (DEPT_ID, SALARY) in (Select DEPT_ID, max(salary) from Employee group by 1 )

======================================================================================================================================
-- Department Top 3 salaries
select * from(  
select d.name as department, e.name as employee, e.salary as salary, 
ROW_NUMBER() over(PARTITION by d.name order by e.salary desc) as rn
from employyes e  join Department d on e.id = d.id) ranked
where ranked.rn < 4

======================================================================================================================================
-- Delete Duplicate records

WITH DuplicateEmails AS (
  SELECT
    Id,
    Email,
    ROW_NUMBER() OVER (PARTITION BY Email ORDER BY Id) AS RN
  FROM Person
)
DELETE FROM DuplicateEmails
WHERE RN > 1;

-- Anoter way
delete p2 from person p1 join person p2. on p1.email = p2.email and p1.id > p2.id

======================================================================================================================================
-- Next row greater than previous row Rising temp

-- Use slef joinn
SELECT a.Id
FROM weather a
JOIN weather b ON a.Temperature > b.Temperature AND DATE_PART('day', a.recorddate - b.recorddate) = 1;

-- Using lag
SELECT Id
FROM (SELECT Id,Temperature,recorddate,
    LAG(Temperature) OVER (ORDER BY recorddate) AS prev_temperature,
    LAG(recorddate) OVER (ORDER BY recorddate) AS prev_recorddate
  FROM weather
) AS ranked
WHERE Temperature > prev_temperature AND prev_recorddate = recorddate - INTERVAL '1 day';


======================================================================================================================================
-- Trips and Users Cancellation Rate
    -- Using Pivot --> sum(case when true then 1 else 0 end)
    select Request_at, round(sum(case when status != 'completed' then 1 else 0 end)/count(*),2) as cancelation_rate
    group by Request_at
    where Request_at between '2013-10-01' and '2013-10-03'
    and client_id not in (select distcint user_id from users where banned = 'yes')
    and driver_id not in (select distcint user_id from users where banned = 'yes' )

======================================================================================================================================
-- game play
    -- Analysis 1 -- First Login Date for each player

    select base.player_id, base.event_date  as first_login from
    (select a.*, ROW_NUMBER() over(PARTITION by player_id order by event_date) as rn
    from Activity a) base where base.rn = 1

    -- solutinon 2
    Select player_id, min(event_date) as first_login
    from Activity
    Group by player_id 

    -- Analysis 2 --> Write a SQL query that reports the device that is first logged in for each player.
    select player_id,  device_id from (
        SELECT player_id, device_id, ROW_NUMBER() over(PARTITION by player_id order by event_date) as rn
    ) base where rn = 1

    -- solutinon 2
    select a.player_id,b.device_id from (
    (select player_id, min(event_date) form Activity group by 1) a join Activity b on a.player_id = b.player_id and a.event_date = b.event_date)

    -- Analysis 3 -->  SQL query that reports for each player and date, how many games played so far by the player.
    -- So far means rolling time or cumilative
    select player_id, event_date, 
    sum(games_played) over(partition by player_id order by event_date) as games_played_so_far
    from activity
    order by 1,2

   
    --Analysis 4 -- Fraction of Players to total that logged in again on same day
    -- In other words, you need to count the number of players that logged in for at least two consecutive 
    -- days starting from their first login date, then divide that number by the total number of players.

With t as 
(select player_id,
 min(event_date) over(partition by player_id) as min_event_date,
 case when event_date- min(event_date) over(partition by player_id) = 1 then 1 
 else 0 end as s
 from Activity)

select round(sum(t.s)/count(distinct t.player_id),2) as fraction 
from t


-- solution 2
SELECT ROUND(COUNT(DISTINCT b.player_id)::NUMERIC/COUNT(DISTINCT a.player_id),2)
FROM activity_550 a
LEFT JOIN activity_550 b ON a.player_id = b.player_id AND a.event_date + 1 = b.event_date;

-- solution 3
WITH PlayerFirstLogin AS (
  SELECT
    player_id,
    MIN(event_date) AS first_login_date
  FROM Activity
  GROUP BY player_id
),
PlayerLogins AS (
  SELECT
    player_id,
    event_date,
    LAG(event_date) OVER (PARTITION BY player_id ORDER BY event_date) AS previous_login_date
  FROM Activity
)
SELECT
  ROUND(SUM(CASE WHEN event_date = previous_login_date + INTERVAL '1 day' THEN 1 ELSE 0 END)::numeric / COUNT(DISTINCT player_id), 2) AS fraction
FROM PlayerLogins
JOIN PlayerFirstLogin ON PlayerLogins.player_id = PlayerFirstLogin.player_id
GROUP BY first_login_date;


        -- Analysis 5
with t1 as(
select *,
row_number() over(partition by player_id order by event_date) as rnk,
min(event_date) over(partition by player_id) as install_dt,
lead(event_date,1) over(partition by player_id order by event_date) as nxt
from Activity)

select distinct install_dt,
count(distinct player_id) as installs,
round(sum(case when nxt=event_date+1 then 1 else 0 end)/count(distinct player_id),2) as Day1_retention
from t1
where rnk = 1
group by 1
order by 1

-- soln2
WITH install_dates AS(
	SELECT player_id,MIN(event_date) AS install_date
	FROM activity_1097
	GROUP BY player_id
),
new AS(
	SELECT i.player_id,i.install_date,a.event_date
	FROM install_dates i
	LEFT JOIN activity_1097 a ON i.player_id = a.player_id AND i.install_date + 1 = a.event_date
)

SELECT install_date,COUNT(player_id),ROUND(COUNT(event_date)::NUMERIC/COUNT(player_id),2)
FROM new
GROUP BY install_date;


-- soln3

# Write your MySQL query statement below

with base as(
select 
player_id,
event_date,
min(event_date) over(partition by player_id) as first_date,
lead(event_date) over(partition by player_id order by event_date) as next_day
from Activity)
select first_date as install_date,
count(distinct player_id) as installs,
coalsece(round(sum(case when next_day - first_date = 1 then 1 else 0 end)/ count(distinct player_id),2) as Day1_retention
from base



======================================================================================================================================
-- managers with atleast 5 direct reportees
WITH managers AS(
	SELECT manager_id
	FROM employee_570
	GROUP BY manager_id
	HAVING COUNT(manager_id)>=5
)

SELECT name
FROM employee_570
WHERE id IN (SELECT * FROM managers);


========================================================================================================================================
-- Question7
-- There is a table courses with columns: student and class

-- Please list out all classes which have more than or equal to 5 students.

-- For example, the table:

-- +---------+------------+
-- | student | class      |
-- +---------+------------+
-- | A       | Math       |
-- | B       | English    |
-- | C       | Math       |
-- | D       | Biology    |
-- | E       | Math       |
-- | F       | Computer   |
-- | G       | Math       |
-- | H       | Math       |
-- | I       | Math       |
-- +---------+------------+

-- Solution
select class
from courses
group by class
having count(distinct student)>=5

========================================================================================================================================
-- Human Traffic of Stadium -- prev and next consecutive rows

-- Soln 1
with q1 as (
select *, id - row_number() over() as id_diff
from stadium
where people > 99
)
select id, visit_date, people
from q1
where id_diff in (select id_diff from q1 group by id_diff having count(*) > 2)
order by visit_date


-- Soln 2
WITH ranked AS (
	SELECT *,
		id-ROW_NUMBER() OVER (ORDER BY id) AS diff
	FROM stadium_601
	WHERE people >= 100
),
consecutives AS (
	SELECT *,
		COUNT(id) OVER (PARTITION BY diff) AS cnt
	FROM ranked
)
SELECT id,visit_date,people
FROM consecutives
WHERE cnt >= 3
ORDER BY visit_date;

========================================================================================================================================
-- Exchange seats
WITH ranked AS(
	SELECT id,student,
		LAG(id) OVER (ORDER BY id) AS lag,
		LEAD(id) OVER (ORDER BY id) AS lead
	FROM seat
)

SELECT 
	CASE WHEN MOD(id,2) = 1 AND lead IS NOT NULL THEN lead
	     WHEN MOD(id,2) = 0 THEN lag
	     ELSE id
	END AS id,
	student
FROM ranked
ORDER BY id;

--soln2
select row_number() over (order by (if(id%2=1,id+1,id-1))) as id, student
from seat

========================================================================================================================================
-- Sales Analysis -1

Select a.seller_id
from
(select seller_id, 
rank() over(order by sum(price) desc) as rk
from sales
group by seller_id) a
where a.rk=1



SELECT seller_id
FROM sales_1082
GROUP BY seller_id
HAVING SUM(price) IN (
			SELECT SUM(price) AS m_sum
			FROM sales_1082
			GROUP BY seller_id
			ORDER BY m_sum DESC 
			LIMIT 1
			);


========================================================================================================================================
-- Sales Analysis -2

select buyer_id from sales join  product on sales.prouct_id = product.product_id
group by buyer_id having sum(case when product_name = 'S8' then 1 else 0 end) > 0
and sum(case when product_name = 'iphone' then 1 else 0) = 0

========================================================================================================================================
-- Sales Analysis -3

SELECT Product.product_id, Product.product_name FROM Product 
JOIN Sales 
ON Product.product_id = Sales.product_id 
GROUP BY Sales.product_id 
HAVING MIN(Sales.sale_date) >= "2019-01-01" AND MAX(Sales.sale_date) <= "2019-03-31";


========================================================================================================================================
-- employee manager with not more than 3 hirearchies reporting to ceo 

SELECT e1.employee_id
FROM employees_1270 e1 
INNER JOIN employees_1270 e2 
ON e1.manager_id = e2.employee_id 
INNER JOIN employees_1270 e3 
ON e2.manager_id = e3.employee_id 
WHERE e3.manager_id = 1 AND e1.employee_id <> 1


========================================================================================================================================
-- Activity with neither max nor min participants
WITH cte AS (
	SELECT activity,COUNT(activity) AS cnt
	FROM friends_1355
	GROUP BY activity
),
cte1 AS (
	SELECT activity,cnt,
		MAX(cnt) OVER () AS max_cnt,
		MIN(cnt) OVER () AS min_cnt
	FROM cte
)
SELECT activity
FROM cte1 
WHERE cnt <> max_cnt AND cnt <> min_cnt; 


select activity from friends group by activity
having count(*) != (select count(*) from friends group by activity order by count(*) limit 1)
and count(*) != (select count(*) from friends group by activity order by count(*) desc limit 1)


========================================================================================================================================
-- Trusted contacts
select invoice_id, customer_name, price, 
count(contact_name) as contact_cnt,
sum(case when contact_name in (select distinct name from customers) then 1 else 0 end) as trusted_contact
from customers left join contacts on customer.customer_id = contact.user_id 
join invoice on customer.customer_id = invoice.user_id 
group by invoice_id order by invoice_id;




========================================================================================================================================
-- Day of Week
WITH category_sales AS (
    SELECT
        i.item_category AS Category,
        o.order_date,
        SUM(o.quantity) AS total_units
    FROM
        Orders o
    JOIN
        Items i ON o.item_id = i.item_id
    GROUP BY
        i.item_category, o.order_date
)
SELECT
    Category,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM order_date) = 0 THEN total_units END), 0) AS Monday,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM order_date) = 1 THEN total_units END), 0) AS Tuesday,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM order_date) = 2 THEN total_units END), 0) AS Wednesday,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM order_date) = 3 THEN total_units END), 0) AS Thursday,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM order_date) = 4 THEN total_units END), 0) AS Friday,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM order_date) = 5 THEN total_units END), 0) AS Saturday,
    COALESCE(SUM(CASE WHEN EXTRACT(DOW FROM order_date) = 6 THEN total_units END), 0) AS Sunday
FROM
    category_sales
GROUP BY
    Category
ORDER BY
    Category;

========================================================================================================================================
-- Second most recent activity and if user has only one activity ouput that as well

with base as (
    select username, activity, start_date, end_date, 
    row_number() over(partition by username order by end_Date desc) as rn,
    count(*)over(partition by username) as c -- This is used only when we want to output users with only single activity
    from useractivity
)select username, activity, start_date, end_date from base where rn = 2 or c= 1


========================================================================================================================================
-- Active Users - Users who logged in 5 or more conscutive days

-- soln 1
select distinct a.id, accounts.name 
from login a 
join login b 
on a.id = b.id and EXTRACT(day from a.login_date) - extract(day from b.login_date) between 1 and 4
join accounts on a.id = accounts.id
group by id, login_date 
having count(distinct b.login_date) = 4;

--soln2
with dedup as (
    select * from login 
    group by id, login_date  -- There is no primary key for this table, it may contain duplicates.
),
cte as (
    select id, login_date,
    lead(login_date,4) over(partition by id order by login_date) as day_5
    from dedup
)
select c.id, a.name from cte c join accounts a on a.id = c.id 
where extract(day from c.day_5) - extract(day from a.login_date) = 4;

========================================================================================================================================
-- PRODUCTS ORDERED FREQUENTLY

SELECT customer_id, T.product_id, product_name
FROM(
    SELECT customer_id, product_id,
    RANK() OVER( PARTITION BY customer_id ORDER BY COUNT(*) DESC ) AS RK
    FROM Orders o
    GROUP BY customer_id, product_id
) T
LEFT JOIN Products p on p.product_id = t.product_id
WHERE RK=1

========================================================================================================================================
-- PRODUCTS PRICE FOR EACH STORE -- PIVOT THE DATA
Products table:
+-------------+--------+-------+
| product_id  | store  | price |
+-------------+--------+-------+
| 0           | store1 | 95    |
| 0           | store3 | 105   |
| 0           | store2 | 100   |
| 1           | store1 | 70    |
| 1           | store3 | 80    |
+-------------+--------+-------+
Result table:
+-------------+--------+--------+--------+
| product_id  | store1 | store2 | store3 |
+-------------+--------+--------+--------+
| 0           | 95     | 100    | 105    |
| 1           | 70     | null   | 80     |
+-------------+--------+--------+--------+

SELECT PRODUCT_ID,
SUM(cASE WHEN STORE = 'STORE1' THEN PRICE ELSE NULL END) AS STORE1,
SUM(CASE WHEN STORE='STORE2' THEN PRICE ELSE NULL END) AS STORE2,
SUM(CASE WHEN STORE='STORE3' THEN PRICE ELSE NULL END) AS STORE3
FROM PRODUCTS GROUP BY 1

========================================================================================================================================
-- APPLE ORANGES COUNT

SELECT SUM(CASE WHEN BOX.APPLE_COUNT IS NOT NULL THEN BOX.APPLE_COUNT ELSE 0 END) + SUM(CASE WHEN CHEST.APPLE_COUNT IS NOT NULL THEN CHEST.APPLE_COUNT ELSE 0 END) AS APPLE_COUNT,
SUM(CASE WHEN BOX.orange_count  IS NOT NULL THEN BOX.orange_count  ELSE 0 END) + SUM(CASE WHEN CHEST.orange_count  IS NOT NULL THEN CHEST.orange_count  ELSE 0 END) AS orange_count 
FROM BOX LEFT JOIN CHEST ON  box.chest_id = chest.chest_id;

SELECT SUM(COALESCE(BOX.APPLE_COUNT,0) + COALESCE(CHEST.APPLE_COUNT,0)) AS APPLE_COUNT,
...



==========================================================================================================================================
-- GRAND SLAM
#Solution 2:
WITH cte
     AS (SELECT wimbledon AS id
         FROM   championships
         UNION ALL
         SELECT fr_open AS id
         FROM   championships
         UNION ALL
         SELECT us_open AS id
         FROM   championships
         UNION ALL
         SELECT au_open AS id
         FROM   championships)
SELECT player_id,
       player_name,
       Count(*) AS grand_slams_count
FROM   players
       INNER JOIN cte
               ON players.player_id = cte.id
GROUP  BY 1, 2
ORDER  BY NULL;

==========================================================================================================================================
-- Safest country to invest -- Average call of each country greater than overall average

-- Best approach
select country.name 
from person join calls on calls.caller_id = person.id or calls.calle_id = person.id 
join country on country.country_code = left(person.phone_number,3)
group by country_name
having avg(duration) > (Select avg(duration) form calls)

-- Another way is 
WITH cte AS (
	SELECT caller_id AS person_id,duration
	FROM calls_1501
	UNION
	SELECT callee_id AS person_id,duration
	FROM calls_1501
),
avg_duration AS(
	SELECT cn.name AS country_name,c.duration AS duration,
		AVG(duration) OVER () avg_global_duration,
		AVG(duration) OVER (PARTITION BY cn.name) avg_country_duration
	FROM cte c
	INNER JOIN person_1501 p ON c.person_id=p.id
	INNER JOIN country_1501 cn ON cn.country_code=SUBSTR(p.phone_number,1,3)
)
SELECT DISTINCT country_name
FROM avg_duration
WHERE avg_country_duration>avg_global_duration;

==========================================================================================================================================
-- Warehouse problem
SELECT a.name AS warehouse_name,
SUM(a.units * b.Width * b.Length * b.Height) AS volume
FROM Warehouse AS a
LEFT JOIN Products AS b
ON a.product_id = b.product_id
GROUP BY a.name;


==========================================================================================================================================
-- Averqage machine Process time
SELECT s.machine_id,
ROUND(AVG(e.timestamp-s.timestamp)::NUMERIC,3) AS processing_time
FROM activity_1661 s
INNER JOIN activity_1661 e 
ON s.activity_type = 'start' AND e.activity_type = 'end' AND
	s.machine_id = e.machine_id AND s.process_id = e.process_id
GROUP BY s.machine_id;

with base as (
    select machine_id, 
    case when activity_type = 'start' then tmestamp end as start_time, 
    case when activity_type = 'end' then tmestamp end as end_time
    from activity
)select machine_id, round(avg(end_time - start_time),3)  as processing_timeform base


==========================================================================================================================================
-- Football Tournament
SELECT Teams.team_id, Teams.team_name,
    SUM(CASE WHEN team_id=host_team AND host_goals>guest_goals THEN 3 ELSE 0 END) +
    SUM(CASE WHEN team_id=host_team AND host_goals=guest_goals THEN 1 ELSE 0 END) +
    SUM(CASE WHEN team_id=guest_team AND host_goals<guest_goals THEN 3 ELSE 0 END) +
    SUM(CASE WHEN team_id=guest_team AND host_goals=guest_goals THEN 1 ELSE 0 END) AS num_points
FROM Teams LEFT JOIN Matches
ON Teams.team_id = Matches.host_team OR Teams.team_id = Matches.guest_team
GROUP BY Teams.team_id
ORDER BY num_points DESC, Teams.team_id ASC

==========================================================================================================================================
-- Median Employee Salary
SELECT t1.Id AS Id, t1.Company, t1.Salary
FROM Employee AS t1 JOIN Employee AS t2
ON t1.Company = t2.Company
GROUP BY t1.Id
HAVING abs(sum(CASE WHEN t2.Salary<t1.Salary THEN 1
                  WHEN t2.Salary>t1.Salary THEN -1
                  WHEN t2.Salary=t1.Salary AND t2.Id<t1.Id THEN 1
                  WHEN t2.Salary=t1.Salary AND t2.Id>t1.Id THEN -1
                  ELSE 0 END)) <= 1
ORDER BY t1.Company, t1.Salary, t1.Id




============================================================================================================================================
-- Salary Tax cal   
#Solution 1:
WITH t1 AS (
SELECT company_id, employee_id, employee_name, salary AS sa, MAX(salary) OVER(PARTITION BY company_id) AS maximum
FROM salaries)

SELECT company_id, employee_id, employee_name,
CASE WHEN t1.maximum<1000 THEN t1.sa
WHEN t1.maximum BETWEEN 1000 AND 10000 THEN ROUND(t1.sa*.76,0)
ELSE ROUND(t1.sa*.51,0)
END AS salary
FROM t1

#Soltion 2:
SELECT Salaries.company_id, Salaries.employee_id, Salaries.employee_name,
    ROUND(CASE WHEN salary_max<1000 THEN Salaries.salary
               WHEN salary_max>=1000 AND salary_max<=10000 THEN Salaries.salary * 0.76
               ELSE Salaries.salary * 0.51 END, 0) AS salary
FROM Salaries INNER JOIN (
    SELECT company_id, MAX(salary) AS salary_max
    FROM Salaries
    GROUP BY company_id) AS t
ON Salaries.company_id = t.company_id


=============================================================================================================================================
-- Triange Problem
-- here the sum of length of 2 sides should be greater than than half of length of longest side 

select x, y, z,
case 
when x+y > z and x+z > y and  y+z > x  then 'Yes' 
when x=y and y=z then 'Yes'
else 'No'
end as Triangle
from triangle


============================================================================================================================================
-- Immediate Food Delivery 1
#Solution- 1:
SELECT
ROUND(SUM(CASE WHEN order_date=customer_pref_delivery_date THEN 1 ELSE 0 END)/count(1)*100, 2) immediate_percentage
FROM Delivery;

#Solution- 2:
SELECT
ROUND(avg(CASE WHEN order_date=customer_pref_delivery_date THEN 1 ELSE 0 END)*100,2) AS immediate_percentage
FROM delivery

============================================================================================================================================
-- Immediate Food Delivery 2
with first_order as (
    select customer_id, order_date, 
    row_number() over(partition by customer_id order by order_date) as rn 
    from Delivery
)select round(sum(case when order_date = customer_pref_delivery_date then 1 else 0 end)/count(*)*100,2) as immediate_percentage
from base where rn = 1;

Elevator Problem -- Cum SUm
-- SOlution 1
With t1 as
(
select *,
sum(weight) over(order by turn) as cum_weight
from queue
order by turn)

select t1.person_name
from t1
where turn = (select max(turn) from t1 where t1.cum_weight<=1000)

-- Solution 2
select a.turn, sum(b.turn)
from queue a join queue b 
on a.turn >= b.turn group by 1 
having sum(b.turn) <= 1000 
order by a.turn 

============================================================================================================================================
-- Student with invalid dept
-- Here invalid is dept so all students on left and only matching dpet on right and filetr where dept is null 
Select s.id, s.name
from students s left join
departments d
on s.department_id = d.id
where d.name is null


============================================================================================================================================
-- Percentage of users who participated in contest
SELECT contest_id,ROUND((COUNT(DISTINCT user_id)*100.0)/(SELECT COUNT(*) AS cnt FROM users_1633),2) AS percentage
FROM register_1633
GROUP BY contest_id
ORDER BY percentage DESC,contest_id;

============================================================================================================================================
-- Customers facing largest no of orders
With t1 as 
(
  Select customer_number, 
  Rank() over(order by count(customer_number) desc) as rk
  from orders
  group by customer_number
) 

Select t1.customer_number
from t1
where t1.rk=1

==============================================================================================================================================-- Quite Students
WITH cte AS(
	SELECT *,
		MIN(score) OVER (PARTITION BY exam_id) AS lowest_score,
		MAX(score) OVER (PARTITION BY exam_id) AS highest_score
	FROM exams_1412
),
cte1 AS(
	SELECT DISTINCT student_id
	FROM exams_1412
	EXCEPT
	SELECT DISTINCT student_id
	FROM cte c
	WHERE score = lowest_score OR score = highest_score
)
SELECT s.*
FROM cte1 c 
INNER JOIN students_1412 s ON c.student_id = s.student_id;

==============================================================================================================================================
-- No of calls between 2 users
select
case when from_id < to_id then from_id else to_id end  as person_1,
case when from_id > to_id then from_id else to_id end  as person_2,
count(1) as call_count,
sum(duration) as total_duraion
from calls group by 1,2;
b 

select least(from_id, to_id) as person_1, greatest(from_id, to_id) as person_2,
count(1) as call_count,
sum(duration) as total_duraion
from calls group by person_1 and [person_2] 
