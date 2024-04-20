SELECT manager_id, salary
FROM employees
WHERE salary IN 
(SELECT MIN(salary)
FROM employees);
