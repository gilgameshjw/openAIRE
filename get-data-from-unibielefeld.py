
import requests
import time
import tqdm
import pandas as pd
import xmltodict
import json
import utils


t_wait = 0.005 # waiting time, to potentially avoid to be detected as an attacker...
data_total = []

# Init
url_bielfeld = 'http://pub.uni-bielefeld.de/oai?verb=ListRecords&metadataPrefix=oai_datacite'
r = requests.get(url_bielfeld)
o = xmltodict.parse(r.content)

data = o['OAI-PMH']['ListRecords']['record'] 
r_token = o['OAI-PMH']['ListRecords']['resumptionToken']['#text']
[data_total.append(d) for d in data]

# Loop over all the ata while finding the resumption token...
while True:
    url_bielfeld = 'https://pub.uni-bielefeld.de/oai?verb=ListRecords&resumptionToken='+r_token
    r = requests.get(url_bielfeld)
    o = xmltodict.parse(r.content)
    data = o['OAI-PMH']['ListRecords']['record'] 
    if not o['OAI-PMH']['ListRecords']['resumptionToken'].get('#text', False):
        print('********** data loaded **********')
        break
    r_token = o['OAI-PMH']['ListRecords']['resumptionToken']['#text']
    [data_total.append(d) for d in data]
    if len(data_total) % 1000 == 0:
        print(len(data_total))
    time.sleep(t_wait)



# Process data

ks_data = ['identifier','timestamp','authors','orci_authors','title','subject_list','typology']

#processed_data = []
#for i in tqdm.tqdm(range(len(data_total))):
#    d = data_total[i]
#    processed_data.append(utils.get_data_dict(d, ks_data))
processed_data = [utils.get_data_dict(data_total[i], ks_data)
                  for i in tqdm.tqdm(range(len(data_total)))]


# Build Dataframe
df = pd.DataFrame(dict([(k,[d[k] for d in processed_data]) for k in ks_data]))
df['year'] = [int(s[:4]) for s in df['timestamp'].to_list()]
df['typology'] = [t[0] for t in df['typology'].to_list()]
df['authors'] = [json.dumps(a) for a in df['authors']]
df['orci_authors'] = [json.dumps(a) for a in df['orci_authors']]

# Save onto data/df.csv
df.to_csv('data/df.csv')
