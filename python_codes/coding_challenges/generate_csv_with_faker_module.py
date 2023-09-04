import csv
import random
import uuid
from faker import Faker
import datetime
from timeit import default_timer as timer
from datetime import timedelta

def datagenerate(records, headers):
    start = timer()
    fake = Faker('en_US')
    with open("challenge_generated_data_csv.csv", 'w', newline='') as csvFile:

        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()


        for i in range(records):
            product_Id = random.randint(1, 10000)
            uid = uuid.uuid4()
            Transaction_Id = uid.hex
            start_date = datetime.date(year=2021, month=1, day=1)
            sales_date = fake.date_between(start_date)

            items_sold = fake.random_int(1, 100)
            items_returned = fake.random_int(1, 100)
            if items_returned > items_sold:
                items_returned = items_sold

            amount = product_Id * 10 * items_returned
            return_reasons = ["Not useful", "Not working", "Need different", "No reason",
                              "Need latest", "Not compatible", "Got it broken"]
            reason = fake.word(return_reasons)

            first_name = fake.first_name()
            last_name = fake.last_name()
            email = first_name + '.' + last_name + '@' + fake.domain_name()

            #print(product_Id,"\n",Transaction_Id)
            writer.writerow({"Product Id": product_Id,"Transaction Id": Transaction_Id, "Sales Date" : sales_date, "Region" : fake.country(), "Zip Code" : fake.zipcode(),"Items Sold":items_sold, "Items Returned":items_returned, "Amount": amount,  "Reason" : reason, "Customer First Name": fake.first_name(),"Customer Last Name": fake.last_name(), "Email" : email, "Phone Number" : fake.msisdn()})
            records = records - 1
    end = timer()
    print("Time taken: ", timedelta(seconds=end - start))


if __name__ == '__main__':

    headers = ["Product Id", "Transaction Id", "Sales Date", "Region",  "Zip Code", "Items Sold" , "Items Returned","Amount", "Reason", "Customer First Name", "Customer Last Name", "Email", "Phone Number"]

    records = 0
    try:
        records = int(input("How much records to generate [default rows is 2]?: "))
    except ValueError:
        if records != 0:
            print("Please enter only integer")
    if records == 0:
        records = 2

    datagenerate(records, headers)
    print("CSV generation complete!")
