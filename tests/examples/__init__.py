import department, employee, salary, forex

hr_examples = [(department, department.Department), (employee, employee.Employee), (salary, salary.Salary)] #dependent on each other, need to be in this order

examples = hr_examples + [(forex, forex.Forex)]