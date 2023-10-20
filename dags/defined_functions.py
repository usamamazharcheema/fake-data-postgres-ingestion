import csv

class MyFunctions:

    def greet(self, name):
        greeting = "Hello " + name
        return greeting

    def calculate_sum(self, a, b, c):
        total = a + b + c
        return total

    def prepare_csv_data(self, file_path, data, column_separator, row_separator):
        with open(file_path, 'w', newline='') as csvfile:
            csv_data = csv.writer(csvfile, delimiter=column_separator, lineterminator=row_separator)
            csv_data.writerows(data)
