#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""Delta Load for NHL Players

This script handles delta load for NHL Players.

Data comes from the below URL:
https://records.nhl.com/site/api/player

Data is written into the following:
Database: NHL_Analytics
Table: nhl_players

"""


# In[2]:


app = 'delta_load_nhl_players'


# In[3]:


import pandas as pd
import json
import sys
import time
import datetime

sys.path.append(r'../utilities/py')

import util_psql_db as pdb
import util_scraping as scr
import util_logging as logging

sys.path.append('../')

import settings

db = settings.database['local']
psql = pdb.UtilPostgreSQL(db['host'], db['port'], db['user'], db['pass'], db['db'])

logger = logging.UtilLogging('../logs/delta_load_nhl_players.log', app)
log = logger.initiateLogger()


# In[4]:


log.info("START {0}".format(app))


# In[5]:


"""
Retrieve Data
"""
URL = r'https://records.nhl.com/site/api/player'
json_data = scr.getURLJson(URL)


# In[6]:


log.info("Retrieving URL: {0}".format(URL))


# In[7]:


"""
Create Player Dataframe
"""
data = json_data['data']

player_df = pd.DataFrame(data)


# In[8]:


log.info("Dataframe Shape: {0}".format(player_df.shape))


# In[9]:


psql.writeDFToTable(player_df, 'nhl_players_2', 'replace')
try: psql.renameTable('nhl_players', 'nhl_players_b')
except: pass
try: psql.renameTable('nhl_players_2', 'nhl_players')
except: pass


# In[10]:


log.info("FINISH {0}".format(app))

