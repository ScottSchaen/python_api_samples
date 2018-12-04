import yaml ### install the pyyaml package
from lookerapi import LookerApi
from datetime import datetime
from pprint import pprint
from argparse import ArgumentParser
### ------- HERE ARE PARAMETERS TO CONFIGURE -------
# parser = ArgumentParser()
# parser.add_argument("-u", "--users", dest="users",help="comma separated list of users that need email credentials and saml credentials deleted")
# args = parser.parse_args()
host = 'localhost'
project_name = 'hub'
branch_name = 'dev-spoke-spoke-pwrc'
user_id = 945
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

#add role to get access to core model

looker.set_user_role(user_id,[{"id":135},{"id":136}])

#begin issuing calls as the user who needs to catch up to production

looker.login_user(user_id)

looker.update_session_workspace()

looker.switch_git_branch(project_name,branch_name)

looker.reset_to_production(project_name)

#might need to use ref  in production catch up rather than reset to prod but seems fine in testing
#alternate would be PUT /projects/{project_id}/git_branch with  {"ref":"master_ref"} as body which we could get as the admin

# #need to go back to old authentication with admin
admin_looker = LookerApi(host=my_host,
                 token=my_token,
                 secret = my_secret)

# #remove role added

admin_looker.set_user_role(user_id,body=[{"id":135}])
