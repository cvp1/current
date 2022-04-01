import boto3
import logging
#Logs for script based execution
logging.basicConfig(filename = 'aws_session.log', level=logging.INFO, format = '%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')


def getmail():
    gm = 0
    hm = 0
    fm_list = []
    with open("/Users/craigvandeputte/pyscripts/working/addresses.csv") as add:
        for x in add:
            FirstMail = x.strip().split("@")
            fm_list.append(FirstMail[0]) 
            if FirstMail[1] == "gmail.com":
                gm+=1
            if FirstMail[1] == "hotmail.com":
                hm+=1 
        print("There are",gm,"gmail addresses")
        print("There are",hm,"hotmail addresses")
        with open("/Users/craigvandeputte/pyscripts/working/results.csv","w") as results:
                for x in fm_list:
                    y = x+"\n"
                    results.write(y)
    return print(fm_list)

getmail()
s3_client = boto3.client('s3')
response = s3_client.list_buckets()
# Output the bucket names
print('Existing buckets:')
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')




