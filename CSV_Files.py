import csv

# The DictWriter and The DictReader is not covered
# In this example


def write_csv(data, filename):
    if filename[-4:] != '.csv':
        filename += '.csv'

    if type(data) not in (type([]), type(())):
        print(type(data))
        raise TypeError

    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        for line in data:
            writer.writerow(line)


def read_csv(filename):
    data = []

    try:
        open(filename)
    except FileNotFoundError as e:
        filename += '.csv'

        try:
            open(filename)
        except FileNotFoundError:
            raise e

    with open(filename, 'r', newline='') as file:
        reader = csv.reader(file)
        for line in reader:
            data.append(tuple(line))
    return tuple(data)


def main():
    data = (
        ('Name', 'Class', 'Section', 'Subjects'),
        ('Suchith', 12, 'C', 'Computer, Math'),
        ('Edel', 12, 'A', 'Physics, Math'),
        ('Monniiesh', 12, 'A', 'Chemistry')
    )

    write_csv(data, 'sample_data')
    data = read_csv('sample_data')
    display_table(data)


if __name__ == '__main__':
    from custom_packages import __main__
    from custom_packages.print_tables import display as display_table

    # main()
