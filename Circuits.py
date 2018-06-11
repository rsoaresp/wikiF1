#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 10 12:09:08 2018

@author: rsoares
"""


import re
import numpy as np
import pandas as pd

import unidecode

import matplotlib.pylab as plt

from joblib import Parallel, delayed



def getTimeSeries(race, year):
    """Return the table with the information about the race.
    
    It uses the pandas' read_html function to get the html table from wikipedia
    with information about the specified race at a given season.
    
    Arguments
    race -- The name of the race desired, Brazilian, French, Italian...
    year -- The year of the race
    """
                       
    print('Loading info for year %i' % year);
    link = 'https://en.wikipedia.org/wiki/' + str(year) + '_' + race + '_Grand_Prix';

    listYears = 'no race found in this season';      
    
    # We try to read the table from the html. If the page does not exist, them
    # we just return a string informing that no race occurred that season.
    try:    
        
        temp = pd.read_html(link);   
        
        # Among all the tables found, we search the one with the desired information,
        # determined to be the onde containing the index 'Pole position' and
        # 'Fastest lap'. This rule proved to be efficient and fast.
        for i in range(0, 10):

            temp[i].set_index(0, inplace = True);
            if 'Pole position' in temp[i].index and 'Fastest lap' in temp[i].index:
                listYears = temp[i];
                break
            
    except:
        
        listYears = 'no race found in this season';            
    
    return listYears




def convert(timeString):
    """Convert the string obtained from wikipedia to seconds.
    
    It used simple regex rules to clean the string and then calculate its 
    duration in seconds, for both pole position time and fastest lap.
    
    Arguments
    timeString -- the string with the time obtained from wikipedia
    """
    
    time = 0;

    # Remove citations that may be together with the time.
    temp = re.sub('\\[.*\\]', '', timeString);
    
    # In the 2005 season the rules for classification were altered, so we may
    # find the following words together with the pole time. Therefore we remove
    # then whenever is the case.
    temp = re.sub(' \\(aggregate\\)', '', temp);
    temp = re.sub(' \\(2 laps\\)', '', temp);
    temp = re.sub(' \\(lap record\\)', '', temp);
    
    temp = re.split(":|\\.|'", temp);
    
    if len(temp) == 2:
        time += float(temp[0])*60 + float(temp[1]);
    else:
        if 'on' not in temp[2]:
            # The pole position's time.
            time += float(temp[0])*60 + float(temp[1]) + float(temp[2])/(10**len(temp[2]));
        else:
            # Fastest lap.
            temp2 = temp[2].split(' ')[0];
            time += float(temp[0])*60 + float(temp[1]) + float(temp2)/(10**len(temp2));            
    
    return time




def Circuits(race, y0, yf):
    """Creates a pandas' data frame with information about a specific F1 race.
    
    It uses the previous functions to return a pandas' dataframe with basic 
    information about a race in the time range specified by the initial and final
    year desired. It returns the pole position and fastest lap, the drivers 
    that were on the podium, the pole sitter and the constructors.
    
    Arguments
    race -- The name of the race desired, Brazilian, French, Italian...
    y0 -- The first year of the time range
    yf -- The last year of the time range (inclusive)
    
    Example
    Circuits('Italian', 1950, 2017) -- Returns the informations about the Italian
    race from 1950 to 2017 (inclusive).
    """
    
    # For performance we use the joblib's package to accelerate the process
    # of getting the tables and processing them.
    num = range(y0, yf + 1);
    inte = zip([race]*len(num), num);
    listYears = Parallel(n_jobs = 3)(delayed(getTimeSeries)(*i) for i in inte);
           
    poleTime = np.zeros(len(listYears));
    fastestTime = np.zeros(len(listYears));

    polePosition = np.empty(len(listYears), dtype = 'object');
    polePositionC = np.empty(len(listYears), dtype = 'object');

    driverFastest= np.empty(len(listYears), dtype = 'object');
    driverFastestC = np.empty(len(listYears), dtype = 'object');
    
    # Driver's podium.
    first = np.empty(len(listYears), dtype = 'object');
    second = np.empty(len(listYears), dtype = 'object');
    third = np.empty(len(listYears), dtype = 'object');    

    # Constructor's podium.
    firstC = np.empty(len(listYears), dtype = 'object');
    secondC = np.empty(len(listYears), dtype = 'object');
    thirdC = np.empty(len(listYears), dtype = 'object');    
    
    for i in range(0, len(listYears)):
        
        if type(listYears[i]) != str:
            
            # Get the pole and fastest lap times from the table.
            poleTime[i] = convert(listYears[i].loc['Time'].iloc[0,:][1]);
            fastestTime[i] = convert(listYears[i].loc['Time'].iloc[1,:][1]);
                        
            # We pass the names under the unidecode function to remove special characters.
            first[i] = unidecode.unidecode(listYears[i].loc['First'].loc[1]);
            second[i] = unidecode.unidecode(listYears[i].loc['Second'].loc[1]);
            third[i] = unidecode.unidecode(listYears[i].loc['Third'].loc[1]);

            firstC[i] = unidecode.unidecode(listYears[i].loc['First'].loc[2]);
            secondC[i] = unidecode.unidecode(listYears[i].loc['Second'].loc[2]);
            thirdC[i] = unidecode.unidecode(listYears[i].loc['Third'].loc[2]);
            
            polePosition[i] = unidecode.unidecode(listYears[i].loc['Driver'].iloc[0][1]);
            polePositionC[i] = unidecode.unidecode(listYears[i].loc['Driver'].iloc[0][2]);
            
            driverFastest[i] = unidecode.unidecode(listYears[i].loc['Driver'].iloc[1][1]);
            driverFastestC[i] = unidecode.unidecode(listYears[i].loc['Driver'].iloc[1][2]);

        else:
            poleTime[i] = 0;
            fastestTime[i] = 0;
            
            first[i] = '';
            second[i] = '';
            third[i] = '';
    
    # We create a data frame with the information gathered, with index the years.
    indexDF = pd.date_range(str(y0), periods = len(num), freq = 'AS');
    df = pd.DataFrame(poleTime, columns = ['pole'], index = indexDF);
    
    df['fastest'] = fastestTime;
    
    df['first'] = first;
    df['second'] = second;
    df['third'] = third;
    
    df['first (constructor)'] = firstC;
    df['second (constructor)'] = secondC;
    df['third (constructor)'] = thirdC;
    
    df['pole position'] = polePosition;
    df['pole position (constructor)'] = polePositionC;

    df['fastest lap'] = driverFastest;
    df['fastest lap (constructor)'] = driverFastestC;
            
    return df
            
        
        
