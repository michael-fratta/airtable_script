# python scheduler
import schedule
import time

def job(): # define the whole script as a function

    from dotenv import load_dotenv
    import os
    load_dotenv()
    import requests

    # initialise slackbot
    slack_token = os.getenv('slackbot_password')
    slack_channel = '#insurance-script'
    # define func
    def post_message_to_slack(text):
        return requests.post('https://slack.com/api/chat.postMessage', {
            'token': slack_token,
            'channel': slack_channel,
            'text': text,
        }).json()

    from pyairtable import Table

    AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')

    CARS_DB_ID = os.getenv('CARS_DB_ID')

    # get all of Airtable 'Specifications' table
    table = Table(AIRTABLE_API_KEY, CARS_DB_ID, 'Specifications')

    # get list of air caps
    air_caps = []
    for record in table.iterate():
        for each_record in record:
            air_caps.append(each_record['fields']['Cap Code'])

    # connect to FaunaDB
    from faunadb import query as q
    from faunadb.objects import Ref
    from faunadb.client import FaunaClient
    client = FaunaClient(secret=os.getenv('secret'),domain="db.fauna.com",port=443,scheme="https")

    # create obj referencing 'car_objects' collection in 'car_ratebooks' db in FaunaDB
    query = client.query(q.map_(q.lambda_(["X"], q.get(q.var("X"))),q.paginate(q.documents(q.collection('car_objects')),size=100000)))

    # get list of just CAPs in car_objects
    fauna_caps = []
    for car in query['data']:
        fauna_caps.append(car['data']['cap_code'])

    # get list of CAPs not in Airtable
    not_in_air = []
    for i in fauna_caps:
        if i not in air_caps:
            not_in_air.append(i)

    # connect to SFTP and read cap_ignore_list.csv
    import pandas as pd
    import pysftp 
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys=None
    hostname = os.getenv('hostname')
    username = os.getenv('username')
    password = os.getenv('password')

    with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
        print("Ignore above warning!\n")
        print("Connection succesfully established...")
        with sftp.open('cap_ignore_list.csv') as f:
            df = pd.read_csv(f)
            print("Successfully read cap_ignore_list.csv\n")

    # get list of CAPs to ignore from ignore_list
    caps_to_ignore = []
    for cap in df['cap_code']:
        caps_to_ignore.append(cap)

    # compare with list of CAPs not_in_air, and create definitive list of CAPs to add to Airtable
    caps_for_air = []
    for cap in not_in_air:
        if cap not in caps_to_ignore:
            caps_for_air.append(cap)

    # initialise row_counter
    row_counter = 0
    # initialise list to hold new car details
    new_cars = []
    ### ADD ROWS TO AIRTABLE ###
    for car in query['data']:
        if car['data']['cap_code'] in caps_for_air:
            try:
                p11d = float(car['data']['prices']['lender_rates'][0]['p11d_pence']/100)
                cap_code = car['data']['cap_code']
                name = car['data']['variant']
                id_name = car['data']['variant']
                cap_id = car['data']['cap_id']
                model = car['data']['model']
                if car['data']['model_year'] != 0:
                    model_year = car['data']['model_year']
                else:
                    model_year = None
                if car['data']['insurance_group'] != 'nan':
                    insurance = car['data']['insurance_group']
                    insurance = insurance[:-1]
                else:
                    insurance = ''
                table.create({'Cap Code': f'{cap_code}',
                            'Name': f'{name}',
                            'Id': f'{id_name}',
                            'Cap ID': cap_id,
                            'P11D': p11d,
                            'Insurance Group': insurance,
                            'Long Name': model,
                            'Model Year': model_year,
                            'Status' : 'Draft',
                            'Quote Tool Status' : 'Draft',
                            'Application Types': ['Business Contract Hire Rates','Salary Sacrifice Rates'],
                            })
                row_counter += 1
                new_cars.append(car['data']['cap_code'])
            except:
                pass


    ### CHECK LL API ###
    from xml.dom import minidom

    ll_user = os.getenv('lloyd_latchford_username')
    ll_pass = os.getenv('lloyd_latchford_password')

    # initialise insurance_list
    insurance_list = []

    for car in query['data']:
        if car['data']['cap_code'] in new_cars:
            cap_id = car['data']['cap_id']

            # SOAP request URL
            url = os.getenv('soap_url')

            # structured XML
            payload = f"""<?xml version="1.0" encoding="utf-8"?>
                        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                            <soap:Body>
                                <Calculate xmlns="http://www.lloydlatchford.com/webservices/">
                                    <Username>{ll_user}</Username>
                                    <Password>{ll_pass}</Password>
                                    <CustomerData>
                                        <RestrictionsId>restrictionsID</RestrictionsId>
                                        <DateOfBirth>DOB</DateOfBirth>
                                        <PostCode>postcode</PostCode>
                                    </CustomerData>
                                    <VehicleData>
                                        <CapId>{cap_id}</CapId>
                                        <VehicleId>VehicleID</VehicleId>
                                    </VehicleData>
                                </Calculate>
                            </soap:Body>
                        </soap:Envelope>"""
            # headers
            headers = {
                'Content-Type': 'text/xml; charset=utf-8'
            }
            # POST request
            response = requests.request("POST", url, headers=headers, data=payload)
            uglyxml = response.text
            doc = minidom.parseString(uglyxml)
            
            if len(doc.getElementsByTagName("Message")) > 0:
                insurance_list.append(car['data']['cap_code'])
            else:
                pass

    ### update insurance_group if present in Fauna (on subsequent runs) but not in Airtable ###

    # initialise list to hold fauna caps and insurance vals
    fauna_caps_and_insurance = []
    # if fauna has insurance - append cap code and insurance to list
    for car in query['data']:
        try:
            if car['data']['insurance_group'] != 'nan':
                insurance = car['data']['insurance_group']
                insurance = insurance[:-1] # don't need letter for airtable
            else:
                insurance = ''
            fauna_caps_and_insurance.append([car['data']['cap_code'],insurance])
        except:
            pass

    # sort fauna for comparison later
    sorted_fauna = sorted(fauna_caps_and_insurance, key=lambda x: x[0], reverse=False)

    # initialise list to hold air cap and ref id and insurance group of each row in airtable
    air_cap_and_id_and_ins = []
    # append to list
    for record in table.iterate():
        for each_record in record:
            try: # in try-except block because doesn't like it when field is null
                air_cap_and_id_and_ins.append([each_record['fields']['Cap Code'],each_record['id'],each_record['fields']['Insurance Group']])
            except:
                air_cap_and_id_and_ins.append([each_record['fields']['Cap Code'],each_record['id'],'']) # empty string as Insurance Group, if not present

    # sort aircap for comparison later
    sorted_aircap = sorted(air_cap_and_id_and_ins, key=lambda x:x[0], reverse=False)

    # create a subset (if fauna cap exists in aircap) of the two sorted lists
    fauna_air_insurance = []
    for i in sorted_fauna:
        for j in sorted_aircap:
            if i[0] == j[0] and i[1] != j[2]: # if fauna capcode matches airtable capcode but insurance groups don't match
                # append to list: fauna capcode, airtable ref id, fauna insurance
                fauna_air_insurance.append([i[0],j[1],i[1]])

    # update relevant row in airtable
    # initialise list to hold results
    air_ins_updates = []
    for item in fauna_air_insurance:
        table.update(f'{item[1]}',{'Insurance Group': item[2],})
        air_ins_updates.append(item[0])

    ### END OF JOB ###
    print("Job done!\n")
    if row_counter > 0:
        print(row_counter,"rows were created in Airtable!")
    else:
        print("No new rows were created in Airtable!")

    if len(insurance_list) > 0:
        if len(insurance_list) == 1:
            slack_info = f"The following new car was added to Airtable and does not have insurance: {insurance_list}"
            print(slack_info)
            post_message_to_slack(slack_info)
        else:
            slack_info = f"The following new cars were added to Airtable and do not have insurance: {insurance_list}"

    if row_counter > 0 and len(insurance_list) == 0:
        if row_counter == 1:
            slack_info = f"The following new car (with insurance) was added to Airtable: {new_cars}"
            print(slack_info)
            post_message_to_slack(slack_info)
        else:
            slack_info = f"The following new cars (with insurance) were added to Airtable: {new_cars}"
            print(slack_info)
            post_message_to_slack(slack_info)

    if len(air_ins_updates) > 0:
        if len(air_ins_updates) == 1:
            slack_info = f"The following car's insurance was updated: {air_ins_updates}"
            post_message_to_slack(slack_info)
            print(slack_info)
        else:
            slack_info = f"The following {len(air_ins_updates)} cars' insurance was updated: {air_ins_updates}"
            post_message_to_slack(slack_info)
            print(slack_info)

# run script every day at 14:00
schedule.every().day.at("14:00").do(job)
while True:
    schedule.run_pending()
    time.sleep(1)
