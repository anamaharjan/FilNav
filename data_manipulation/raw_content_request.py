# quick and simple pubsub cloud function for the hack, can be improved for sure

import certifi
import requests
import json
from urllib import parse
import pymongo
import time
import copy
import base64


m_db=pymongo.MongoClient("mongodb+srv://$MONGO_USERNAME:$MONGO_PASSWORD@$DB_NAME.$DB_ID.mongodb.net/?retryWrites=true&w=majority", tlsCAFile=certifi.where())


def urlParse(url):
  parts = parse.urlparse(url)
  directories = parts.path.strip('/').split('/')
  elements = {
      'scheme': parts.scheme,
      'netloc': parts.netloc,
      'path': parts.path,
      'params': parts.params,
      'queries': dict(parse.parse_qsl(parts.query)),
      'fragment': parts.fragment,
      'directories': directories,
  }
  return elements

def sourceRequest(source_name,query,identifier):
  print("sourceRequest",source_name,query,identifier)
  request_map={
      'filrep':{
          'miners':{'query_1':'miners','query_2':0,'url':'https://api.filrep.io/api/v1/miners?search='+identifier,'headers':{'content-type': 'application/json'},'method':'GET'}
          },
      'filmine':{
        'storage-provider':{'query_1':'pageProps','query_2':"data",'url':f"https://filmine.io/_next/data/N_f0Sbys2weNDeMrpm5AZ/en/filgram/storage-provider/{identifier}.json",'headers':{'content-type': 'application/json'},'method':'GET'}
      },
      'filswan':{
        'miner':{'query_1':'data','query_2':'miner','url':f"https://api.filswan.com/miners/{identifier}",'headers':{},'method':'GET'}
      },
      'filplus':{
        'notary-leaderboard':{'query_1':'pageProps','query_2':'verifiers','url':'https://filplus.dev/_next/data/DZ9ATGOKHOhdOg6_sy4d1/index.json','headers':{'accept': '*'},'method':'GET','payload':{}}
      },
       'twitter':{
        'screen-name':{'url':f"https://api.twitter.com/1.1/users/show.json?screen_name={identifier}",'headers':{'Content-Type': 'application/json','Authorization':'Bearer $API-KEY'},'method':'GET'},
       
        },
    'messari':{
          'dao':{'query_1':'data','query_2':'governanceBySlug','url':"https://graphql.messari.io/query",'method':'POST','headers':{'content-type': 'application/json'},'payload':{"operationName":"getGovernanceBySlug","variables":{"slug":identifier},"query":"query getGovernanceBySlug($slug: String!) {\n  governanceBySlug(slug: $slug) {\n    __typename\n    id\n    name\n    slug\n    details\n    logoUrl\n    structure\n    tags\n    type\n    links {\n      link\n      name\n      __typename\n    }\n    asset {\n      id\n      name\n      slug\n      symbol\n      logo\n      metrics {\n        id\n        pricing {\n          id\n          price\n          __typename\n        }\n        marketcap {\n          id\n          reported\n          __typename\n        }\n        returnOnInvestment {\n          id\n          change(span: ONE_DAY)\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    propositionStatusMetrics {\n      id\n      totalCount\n      __typename\n    }\n    process {\n      details\n      phases {\n        order\n        name\n        details\n        __typename\n      }\n      __typename\n    }\n     governingBodies {\n      id\n      name\n      details\n      logoUrl\n      children {  name  id details      __typename children {  name  id details      __typename children {  name  id details      __typename }} }  tools {\n        id\n        url\n        tool {\n          id\n          name\n          description\n          logoUrl\n          types\n          tags\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n  }\n}\n"}},
          },

      }
      
  source_map=request_map[source_name][query]
  print('source_map',source_map)
  method=source_map.get('method')
  url=source_map.get('url')
  headers=source_map.get('headers')
  payload=source_map.get('payload')
  
  query_1=source_map.get('query_1')
  query_2=source_map.get('query_2')
  query_3=source_map.get('query_3')
  #not best way, but good enough for the hackathon!
    
  request = {"url":urlParse(url),"headers":headers,"method":method}
  
  if payload:
    payload_dumped=json.dumps(payload)
    request["data"]=payload
    #print(f'requests.request({method}, {url}, headers={headers}, data={payload})')
    try:
      response = requests.request(method, url, headers=headers, data=payload_dumped)
    except requests.exceptions.RequestException as e:  # This is the correct syntax
      #setError(None,request,SystemExit(e))
      print('da error',e)
      return "error"
  else:
    try:
      response = requests.request(method, url, headers=headers)
    except requests.exceptions.RequestException as e:  # This is the correct syntax
      #setError(None,request,SystemExit(e))
      return "error"
  response_json = response.json()
  if type(response_json)==dict:
    errors=response_json.get('errors')
    if errors:
      #setError(None,request,response_json)
      print('errors',errors)
      return "error"
    
    status=response_json.get('status')
    if status:
      del response_json['status']
    response_code=response_json.get('code')
    if response_code==50001:
        print('error',response_json)
        return response_code

  else:
      print('not dict resp.')

  
  timestamp=time.time()

  del request['headers']
  #headers=request['headers']
  #if headers:
  response_query=None
  if query_1!=None:
    response_query=copy.deepcopy(str(query_1))
    print('q1')
    try:
      response_json=response_json[query_1]
    except:
      print('1 skip')
      response_json="skip"
  if query_2!=None:
    response_query=response_query+"."+str(query_2)
    print('q2')
    try:
      response_json=response_json[query_2]
    except:
      response_json="skip"
  if query_3!=None:
    response_query=response_query+"."+str(query_3)
    print('q3')
    try:
      response_json=response_json[query_3]
    except:
      response_json="skip"
  #print(response_json)
  #return response_json
  if response_json=="skip":
    print('will skip')
    return 'skip'
  else:
    if True==False:
      #keeping here in case we need any checks...
      pass

    else:
        source_data = {
            u'timestamp':timestamp,
            u'response':response_json,
            u'request':request
        }
        if response_query:
          source_data['response_query']=response_query
        doc=m_db['raw_content_db'][source_name.lower()].insert_one(source_data)
        print(doc.inserted_id)
        print("one success")

#%%
#sourceRequest("filswan","miner","f01780906")
#%%


def dataDirector(data_dict):
    source_name=data_dict.get('source_name')
    query=data_dict.get('query')
    identifier=data_dict.get('identifier')
    sourceRequest(source_name,query,identifier)


def hello_pubsub(event, context):
    print("""This Function was triggered by messageId {} published at {} to {}
    """.format(context.event_id, context.timestamp, context))
    print("event",event)
    if 'data' in event:
        raw_dict = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        data_dict=raw_dict.get('data')
        #print(data_dict)
        #print(type(data_dict))
        if data_dict:
          dataDirector(data_dict)
        else:
          print('no message')
    else:
        print('no Message')

# %%
