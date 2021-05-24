
"""
    Data fight with XML...
"""

def get_identifier(d):
    return d['header']['identifier']

def get_timestamps(d):
    return d['metadata']['resource']['dates']['date'][0]['#text'][:4] 

def get_title(d):
    if type(d['metadata']['resource']['titles']['title'])!=list:
        return d['metadata']['resource']['titles']['title'].lower()
    else:
        return d['metadata']['resource']['titles']['title'][1]['#text'].lower()

def get_subjects(d):
    if d['metadata']['resource'].get('subjects', False):
        return [s.lower() for s in d['metadata']['resource']['subjects']['subject'] if type(s)==str]  
    else: 
        return []
    
def get_orci_authors(d):
    if type(d['metadata']['resource']['creators']['creator']) == list:
        return ['orcid:'+c['nameIdentifier']['#text'].lower() 
                for c in d['metadata']['resource']['creators']['creator'] if c.get('nameIdentifier', False)]
    else:
        return ['orcid:'+d['metadata']['resource']['creators']['creator']['nameIdentifier']['#text'].lower()] \
            if d['metadata']['resource']['creators']['creator'].get('nameIdentifier', False) else []
                 
def get_authors(d):
    if type(d['metadata']['resource']['creators']['creator']) == list:
        return [c['givenName'].lower()+' '+c['familyName'].lower() for c in
                d['metadata']['resource']['creators']['creator']]
    else:
        return [d['metadata']['resource']['creators']['creator']['givenName'].lower()+' '+d['metadata']['resource']['creators']['creator']['familyName'].lower()]

def get_typology(d):
    return d['header']['setSpec']

def get_data(d, label):
    try: 
        if label=='identifier':
            return get_identifier(d)
        elif label=='timestamp':
            return get_timestamps(d)
        elif label=='authors':
            return get_authors(d)
        elif label=='orci_authors':
            return get_orci_authors(d)
        elif label=='title':
            return get_title(d)
        elif label=='subject_list': 
            return get_subjects(d)
        elif label=='typology':
            return get_typology(d)
    except:
        return 'error::'+label

    
def get_data_dict(d, ks_data):    
    return dict([(k,get_data(d,k)) for k in ks_data])
