from random import randrange
from datetime import timedelta
from datetime import datetime


def random_date(start, end):
    
    #returns a random datetime between two datetime objects.
    
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def print_date():    
    #prints/returns a random datetime object within the year 2021
    d1 = datetime.strptime('1/1/2021 12:00 AM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('12/31/2021 11:59 PM', '%m/%d/%Y %I:%M %p')
    randomized_date2021 = random_date(d1, d2)
    print(randomized_date2021)
    return randomized_date2021

print_date()