SELECT department_name
FROM departments
WHERE department_id IN (SELECT department_id
FROM employees
WHERE salary IN
(SELECT MAX(salary) FROM employees));
