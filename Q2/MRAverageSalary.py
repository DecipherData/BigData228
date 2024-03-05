from mrjob.job import MRJob
from mrjob.step import MRStep

class MRAverageSalary(MRJob):

     def mapper_get_salaries(self, _, line):
        # Split the line into fields
        fields = line.split(',')
        try:
            # Here we are extracting company, department and salary columns from the csv.
            company = fields[1].strip()
            department = fields[2].strip()
            salary = float(fields[7].strip())
            # Since we wanna know average salaries for each department in a company, we have to take company and department as keys.
            yield (company, department), salary
        except ValueError:
            # For any mssing fields skip lines
            pass

     def reducer_average_salary(self, key, values): # key will be company and department and values list of salaries per key
        # Calculate the average salary for each company and department
        salaries = list(values)
        average_salary = sum(salaries) / len(salaries)
        yield key, average_salary

     def steps(self):
        return [
            MRStep(mapper=self.mapper_get_salaries,   # here it gives the steps for mapReduce and returns a list of MRStep object
                   reducer=self.reducer_average_salary) ### so each MRSTep object is configured with a mapper and reducer function
        ]

if __name__ == '__main__':
    MRAverageSalary.run()
