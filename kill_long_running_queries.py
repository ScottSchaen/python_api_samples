import yaml ### install the pyyaml package
from lookerapi import LookerApi
from datetime import datetime
from pprint import pprint
from pytz import timezone
import math

### ------- HERE ARE PARAMETERS TO CONFIGURE -------
# Set the host that you'd like to access (as aliased in config.yml)
host = "sandbox"
# Set the number of max number of seconds a query can be run before it's killed
threshold = 500
# Set query sources you don't want to kill
# Possible values:
# - API 3
# - CSV Dashboard Download
# - Dashboard
# - Dashboard Prefetch
# - Drill Modal
# - Explore
# - Merge Query
# - PDT Regenerator
# - Private Embed
# - Public Embed
# - Query
# - Renderer
# - Saved Look
# - Scheduled Task
# - SQL Runner
# - Suggest Filter

sources_to_exclude = ["query","dashboard","explore"]

print("\n### Script Started " + datetime.utcnow().strftime('%a %b %d %H:%M:%S') + " UTC ###")

### ------- OPEN THE CONFIG FILE and INSTANTIATE API -------
f = open('config.yml')
params = yaml.load(f)
f.close()

my_host = params['hosts'][host]['host']
my_secret = params['hosts'][host]['secret']
my_token = params['hosts'][host]['token']

looker = LookerApi(host=my_host,
                 token=my_token,
                 secret = my_secret)


queries = looker.get_running_queries()

print(str(len(queries)) + " Queries Running:")

kill_count = 0
# Loop through running queries
for i in queries:
    query_created_at = datetime.strptime(
        i['created_at'].replace('T',' '), '%Y-%m-%d %H:%M:%S.%f+00:00'
    )
    tz = i['query']['query_timezone']
    # Compare query start time with system time.
    # Need to ensure timezones are setup correctly
    running_time = (datetime.utcnow() - query_created_at).total_seconds()

    source = i['source']
    view = i["query"]["view"]
    model = i["query"]["model"]
    user = i["user"]["display_name"]
    share_url = i["query"]["share_url"]
    qid = i["id"]
    
    # Comment out next 5 rows for less verbose logging
    print("\n\t[" + source + "] - Query ID " + str(qid) + " - " + model + "/" + view)
    print("\tRun by " + user)
    print("\t" + share_url)
    print("\tStarted at " + query_created_at.strftime('%a %b %d %H:%M:%S') + " UTC")
    print("\tRunning for " + str(math.floor(running_time)) + " seconds")

    if running_time > threshold and source not in sources_to_exclude:
        print('\tKILLING QUERY: {}'.format(i['query_task_id']))
        looker.kill_query(i['query_task_id'])
        kill_count +=1

print('Killed {} queries'.format(kill_count))
print("### Script Ended " + datetime.utcnow().strftime('%a %b %d %H:%M:%S') + " UTC ###")
