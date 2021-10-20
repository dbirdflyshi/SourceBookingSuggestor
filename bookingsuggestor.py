###############################################################################
###############################################################################
##                                                                           ##
##                           ~-.Booking Suggestor.-~                         ##
##                             -Dane Anderson-                               ##
##                                                                           ##
###############################################################################
###############################################################################
#Purpose: The LC team gets around 250-300 orders per day to book manually that
#         PSO can't book automatically. This script aims to lower that number
#         even more using more logic.

# Linked Sources : nPrinting, all are tied to the boot.xlsx
# - Sheet name: Sheet 1 : CAMP : CH87(Plant Capacity), TB14(Open Orders) : Runs Every Hour from 10:30AM-11:30PM GMT and lasts 15 minutes
# - Sheet name: Volumes OTP : OTP : CH687(Volumes & OTP Data) : Runs at 11:03AM GMT Weekdays and lasts 45 minutes
# - Sheet name: Lane Cost: Volumes : CH639(Lane Cost) : Runs at 3:15AM GMT Weekdays&Sunday and lasts for 1 hour (not used in script, but in dashboard)

# Importing all the packages
import numpy as np
import pandas as pd
import os
from datetime import datetime,date, timedelta
import time
from skcriteria import Data, MIN, MAX
#this library class is used to calculate the distance between ideal/anit-ideal solution to the alternative options we have
from skcriteria.madm import closeness
import traceback
import smtplib
from datetime import datetime
from shutil import copyfile
import glob
#import logging
#
#
#log = logging.getLogger('console')
#log.setLevel(logging.DEBUG)
#
#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#
#fh = logging.FileHandler("//usdcvms212/d$/Prod Projects/C Stock Booker Script/Data/log.txt")
#fh.setLevel(logging.DEBUG)
#fh.setFormatter(formatter)
#log.addHandler(fh)
#============================================================    
try:   
    # Don't Change These Three Lines
    login = pd.read_csv('redacted')
    server = smtplib.SMTP('redacted','redacted')
    server.login(login.Username[0],login.Password[0])
    
    # The To Field(s)
    TO =  'redacted'
    todayapifmt = datetime.today().strftime('%Y-%m-%d, Hour %H')



    # Setting the current directory to the script location 
    os.chdir('redacted')
    todaytime = datetime.today()
    # Modifyable variables to adjust the output
    buffer = .20 # Changing from half a day of coverage restriction to 1/4 day of coverage restriction
    # Changing from .75 days coverage to .5 4/7/2020 as per chris torres
    metrichistorylength = 8
    cost = .329
    loh = .499
    otp = .152
    lanecount = .037
    availability = .071
  
    # Creating the custom weights as agreed upon in the 2/10/2020 meeting
    #time.sleep(30) #added by the jr. guy to pause here.
    weights = np.asarray([cost,loh,otp,lanecount,availability])
    # Building a matrix with the criteria values. criteria: Cost, LOH, OTP, Lane Count, Availability
    # TODO: Create a comment version of this to explain how it works
    # old matrix
    #mtx = np.array([
    #       [1,3,7,0.111,0.333],
    #       [.333,1,.142,.2,0.333],
    #       [.142,7,1,.333,0.333],
    #       [9,5,3,1,7],
    #       [3,3,3,0.14,1]
    #       ])
    
    # NEW matrix, sort of
    #mtx = np.array([
    #       [1,4,6,10,8],
    #       [.25,1,6,10,4],
    #       [.167,.167,1,.167,0.167],
    #       [.1,.1,6,1,.1],
    #       [.125,.25,6,10,1]
    #       ])
    
    #             | Cost | LOH | OTP | Lane Count | Availability 
    #Cost         |  1   |  3  |  7  |    0.111   |    0.333
    #LOH          |0.333 |  1  |0.142|    0.2     |    0.333
    #OTP          |0.142 |  7  |  1  |    0.333   |    0.333
    #Lane Count   |  9   |  5  |  3  |    1       |    7
    #Availability |  3   |  3  |  3  |    0.14    |    1
    # Go either horizontal or vertical and compare row by row:
    # On a scale of 1/10 how important is LOH to Cost? ex.3, then the inverse (Cost to LOH) is 1/3. 
    ######################################################
    ##                 Preprocessing                    ##
    ######################################################
    
    # Storing the last modified time to make help with the next step below
    lastruntime = os.path.getmtime('redacted')
    # objectstime = os.path.getmtime('//usdcvms212/d$/Prod Projects/C Stock Booker Script/Data/C Stock Booker Script Objects.xlsx')
    newobjecttime = os.path.getmtime('redacted')
    
    # This will not allow the file to run until the objects file has been updated, this will prevent 
    # the script from updating old data. If the sources object made at the end of the script is 
    # newer than the objects file that nprinting makes, then that means that nprinting did not finish
    # yet and we need to wait for it to update, it waits 10 seconds and tries again.
    counter = 0
    while newobjecttime < lastruntime: #checking if Boot Objects file mod time is < readyorders time
        print('There Is No New File Yet')
        # Waits 10 seconds to retry
        time.sleep(10)
        newobjecttime = os.path.getmtime('redacted')
        counter = counter+1
        # Creating a new failsafe where i think boot fails frequently
        if counter >= 1000:
            server = smtplib.SMTP('redacted',redacted)
            server.login(login.Username[0],login.Password[0])
            MSG = 'BOOT waited too long to check for a new file'
            FROM = "redacted"
            # No need to change below 
            server.sendmail(FROM,TO,MSG)  
            exit()
    
    #####################################
    #               Volumes             #
    #####################################
    # We have two different sources for this data due to inconsistencies presented in the Volumes OTP sheet here. It's been decided
    # that it's better to use the full historical average cost and LOH and use Volumes OTP to read the 8 Week Avg of OTP and Lane Count
    # since the these two numbers are more relative to choosing a better location at the current time of choosing.
    volumes = pd.read_excel('redacted', sheet_name = 'Volumes OTP')
    volumes['Actual GI Date'] = pd.to_datetime(volumes['Actual GI Date'], format='%m/%d/%Y')
    history = (todaytime-timedelta(days=7*metrichistorylength))
    volumes = volumes[volumes['Actual GI Date'] >= history]
    volumes['Lane Count'] = volumes['DGlobal'].astype(str)+volumes['OGlobal']
    volumes = volumes.replace('-', np.nan)
    volumes['LOH'] = volumes['LOH'].str.replace(',', '')
    volumes[['Cost', 'LOH', 'OTP']] = volumes[['Cost', 'LOH', 'OTP']].apply(pd.to_numeric) 
    volumes = volumes.drop(columns = ['Actual GI Date'])
    volumes = volumes.groupby('Lane Count').agg({'Lane Count':'count','OGlobal':'first','DGlobal':'first','Cost':np.mean,'LOH':np.mean, 'OTP':np.mean, 'PlantType': 'first'})   
    volumes = volumes.reset_index(drop = True)
    volumes = volumes[['OGlobal','DGlobal','Cost','LOH','OTP','PlantType','Lane Count']]
    
    # We will need lane volume to link the other tables
    volumes['Lane'] = volumes['DGlobal'].astype(str)+volumes['OGlobal']
    
    
    Lanecost = pd.read_excel('redacted', sheet_name = 'Sheet1')
    Lanecost['OGlobal'] = Lanecost['First Pick Location Reference Number'].str.split('-').str[0]
    Lanecost['DGlobal'] = Lanecost['Last Drop Location Reference Number'].str.split('-').str[0]
    Lanecost['Lane'] = Lanecost['DGlobal'].astype(str)+Lanecost['OGlobal']
    Lanecost = Lanecost.groupby('Lane').agg({'Lane':'first','Payable Total Rate':np.mean})   
    Lanecost = Lanecost.reset_index(drop = True)
    
    
    volumes = volumes.merge(Lanecost, on='Lane', how='left')
    volumes["Cost"] = volumes.apply(lambda x: x['Payable Total Rate'] if (x['Payable Total Rate'] >0)  else x['Cost'], axis = 1)
    volumes.to_excel(r'redacted', encoding =  "ISO-8859-1")  

    volumes2 = pd.read_excel('redacted', sheet_name = 'LaneData')
    volumes2 = volumes2[['OGlobal','DGlobal','Ship-to Zip','Cost','LOH','PlantType']]
    volumes2['Lane'] = volumes2['DGlobal'].astype(str)+volumes2['OGlobal']
    # Merging just lane count to the old volumes because that's really the only important piece to make sure its an X week history limitation
    volumes2 = volumes2.merge(volumes[['Lane','Lane Count','OTP']], how = 'left', on = ['Lane'])
    # Writing over the old volumes
    volumes = volumes2
    volumes = volumes.dropna(subset=['Lane Count','OTP'])

    plantpercent = pd.DataFrame(volumes)
    plantpercentgb = volumes.groupby('OGlobal').sum()[['Lane Count']]
    
    plantpercent = plantpercent.merge(plantpercentgb, how = 'left', on = ['OGlobal'])
    plantpercent['Plant Percent'] = plantpercent['Lane Count_x']/plantpercent['Lane Count_y']
    plantpercent['Lane'] = plantpercent['Lane'].astype(str)

    #####################################
    #            Open Orders            #
    #####################################
    # Table with the current open orders needing to be booked
    openorders = pd.read_excel('redacted', sheet_name = 'OO')
    openorders = openorders.dropna(subset=['Sales Order Item'])
    ooemail = openorders

    # We need So-item so we can link tables together
    openorders['SO-Item'] = openorders['Sales Order'].astype(str)+openorders['Sales Order Item'].astype(str)
    
    # #get open orders that were already booked by boot in all previous runs
    # bookedorders = pd.read_csv("//usdcvms212/d$/Prod Projects/C Stock Booker Script/Data/PrevBookedOrders.csv")
    # bookedorders['SO-Item'] = bookedorders['SO-Item'].astype(str)
    # merged = openorders.merge(bookedorders, how='left', indicator=True, on = 'SO-Item')
    # openorders = merged[merged['_merge']=='left_only']
    
    # Combining the Material and Batch in order to key it for joins
    openorders['DFU'] = openorders['Material'].astype(str)+'-'+openorders['Batch'].astype(str)
    # We don't want supply
    supplyorders = openorders.loc[openorders['Delivery Type'] == 'Return']
    openorders = openorders.drop(openorders[openorders['Delivery Type'].map(lambda x: str(x) == 'Return')].index)
    # Cutting the crap and reordering
    openorders = openorders[['FiscalDate','Customer','SO-Item','Sales Order','Sales Order Item','DFU','Delivery Type','Delivery Quantity','P1','P2','P3','Closest_Plant']]
    openorders['Customer'] = openorders['Customer'].astype(str)
    openorders['Batch'] = openorders['DFU'].str[5:]
   
    # Including the mapping 
    mappings = pd.read_excel('redacted', sheet_name = 'Mapping')
    mappings['DFU'] = mappings['Material'].astype(str).replace('\.0', '', regex=True)+'-'+mappings['Batch']
    mappings = mappings.rename(columns = {'P1':'Map1','P2':'Map2','P3':'Map3'})
    mappings['Customer'] = mappings['Customer'].astype(str)

    openorders = openorders.merge(mappings, how = 'left', on = ['Customer','Batch'])
    openorders = openorders[['FiscalDate','Customer','SO-Item','Sales Order','Sales Order Item','Delivery Type','Delivery Quantity','Closest_Plant','DFU_x','Map1','Map2','Map3']]
    openorders = openorders.rename(columns = {'Map1':'P1','Map2':'P2','Map3':'P3','DFU_x':'DFU'})
    
    # We don't want anything that's not 4055 RU PLUS
    not4055 = openorders.loc[openorders['DFU'] != '4055-RU PLUS']
    openorders = openorders.drop(openorders[openorders['DFU'].map(lambda x: str(x) != '4055-RU PLUS')].index)
    # Combining each service center and the customer to generate lanes
    openorders['P1 Lane'] = openorders['Customer'].astype(str)+openorders['P1']
    openorders['P2 Lane'] = openorders['Customer'].astype(str)+openorders['P2']
    openorders['P3 Lane'] = openorders['Customer'].astype(str)+openorders['P3'] 
    # Sometimes there's a set of open orders that have no P2 or P3, this causes problems,
    # because it converts it to float64 which can't be merged further down
    openorders['P1'] = openorders['P1'].astype('object')    
    openorders['P2'] = openorders['P2'].astype('object')    
    openorders['P3'] = openorders['P3'].astype('object')  
    
    openorders = openorders.drop_duplicates()

    
    #####################################
    #           Plant Summary           #
    #####################################
    ## Table with plant details from production application like cgen etc
    #plantdetails = pd.read_excel('//usdcvms212/d$/Prod Projects/C Stock Booker Script/Data/C Stock Booker Script Objects.xlsx', sheet_name = 'Plant Summary')
    #plantdetails = plantdetails[['Plant','Type','Inven Date','C-Gen Target']]
    
    #####################################
    #            CAMP Details           #
    #####################################
    # Table with plant capacity like booked quantity
    plantcapacity = pd.read_excel('redacted', sheet_name = 'Sheet1')
    plantcapacity = plantcapacity[['Plant','Date','AvgQuantity','BookedQuantity',"Today's Inventory",'RUPlusCGen']]
    # We dont need no blank dates in our lives, they aint be good for nobody
    plantcapacity = plantcapacity.drop(plantcapacity[plantcapacity['Date'].map(lambda x: str(x) == '-')].index)
    # Just making date a datetime object so we can join tables
    plantcapacity['Date'] = pd.to_datetime(plantcapacity.Date, format = '%Y-%m-%d')
    plantcapacity = plantcapacity.replace('-', 0)
    plantcapacity['BookedQuantity'] = plantcapacity['BookedQuantity'].astype(np.int64)
    plantcapacity['RUPlusCGen'] = plantcapacity['RUPlusCGen'].astype(np.int64)
    
    # After a terrible time to figure this out, stackoverflow eventually helped figure this piece out..
    # What we're doing here is updating the inventory to be realistic to what the inventory should be to the best of our knowledge
    plants = plantcapacity['Plant'].unique()
    plantCapWUpdatedInvs = pd.DataFrame([])
    for plant in plants:
        plantcapacity2 = plantcapacity.loc[plantcapacity['Plant']== plant ].sort_values(by = 'Date').reset_index(drop = True)
        plantcapacity2.loc[:,'Adj Inventory'] = plantcapacity2.loc[:,"Today's Inventory"].values[0] # need to initialize
        for i in range(1, len(plantcapacity2)):
            plantcapacity2.loc[i, 'Adj Inventory'] = plantcapacity2.loc[i-1, 'RUPlusCGen'] + ( plantcapacity2.loc[i-1, 'Adj Inventory'] - plantcapacity2.loc[i-1, 'BookedQuantity'] ) 
        plantCapWUpdatedInvs = pd.concat([plantcapacity2,plantCapWUpdatedInvs],axis = 0, join = 'outer',  ignore_index = False, sort = True )
    plantcapacity = plantCapWUpdatedInvs[['Date','Plant','AvgQuantity','BookedQuantity','Adj Inventory','RUPlusCGen']]
    plantcapacity = plantcapacity.rename(columns = {'Adj Inventory':"Today's Inventory"})
    # We should consider a certain amount of pallets untouchable at the plant to account for emergency or late loads as well as give plants some breathing room when working in case 
    # there isn't enough b stock or there are some callouts at the plant resulting in suboptimal c-gen levels
    plantcapacity['Buffer'] = np.where(plantcapacity['RUPlusCGen']*buffer >= 541, plantcapacity['RUPlusCGen']*buffer,541)
    
    
    #####################################
    #        Combining Them All         #
    #####################################
    # Adding the Plant Capacity information and Plant Details for P1, renaming, then cutting and reorganizing the columns
    oo = openorders.merge(plantcapacity, how = 'left', left_on = ['P1','FiscalDate'], right_on = ['Plant','Date'])
    #oo = oo.merge(plantcapacity2, how = 'left', left_on = ['P1'], right_on = ['PlantCode'])
    #oo = oo.merge(plantdetails, how = 'left', left_on = 'P1', right_on = 'Plant')
    oo = oo.merge(volumes[['Lane','Cost','PlantType']], how = 'left', left_on = 'P1 Lane', right_on = 'Lane')
    oo = oo.rename(columns = {'PlantType':'P1 Plant Type','Cost':'P1 Lane Cost','LOH':'P1 Lane LOH','OTP':'P1 Lane OTP','Capacity':'P1 Cap','Current Inventory':'P1 CI','Current Plant Cap %':'P1 CPCP','Stock':'P1 Stock','RUPlusCGen':'P1 C-Gen','BookedQuantity':'P1 Booked',"Today's Inventory":'P1 Today Inventory','AvgQuantity':'P1 Funnel','Buffer':'P1 Buffer'})
    
    # Adding the Plant Capacity information for P2, renaming, then cutting and reorganizing the columns
    oo = oo.merge(plantcapacity, how = 'left', left_on = ['P2','FiscalDate'], right_on = ['Plant','Date'])
    #oo = oo.merge(plantcapacity2, how = 'left', left_on = ['P2'], right_on = ['PlantCode'])
    #oo = oo.merge(plantdetails, how = 'left', left_on = 'P2', right_on = 'Plant')
    oo = oo.merge(volumes[['Lane','Cost','PlantType']], how = 'left', left_on = 'P2 Lane', right_on = 'Lane')
    oo = oo.rename(columns = {'PlantType':'P2 Plant Type','Cost':'P2 Lane Cost','LOH':'P2 Lane LOH','OTP':'P2 Lane OTP','Capacity':'P2 Cap','Current Inventory':'P2 CI','Current Plant Cap %':'P2 CPCP','Stock':'P2 Stock','RUPlusCGen':'P2 C-Gen','BookedQuantity':'P2 Booked',"Today's Inventory":'P2 Today Inventory','AvgQuantity':'P2 Funnel','Buffer':'P2 Buffer'})
    
    # Adding the Plant Capacity information for P3, renaming, then cutting and reorganizing the columns
    oo = oo.merge(plantcapacity, how = 'left', left_on = ['P3','FiscalDate'], right_on = ['Plant','Date'])
    #oo = oo.merge(plantcapacity2, how = 'left', left_on = ['P3'], right_on = ['PlantCode'])
    #oo = oo.merge(plantdetails, how = 'left', left_on = 'P3', right_on = 'Plant')
    oo = oo.merge(volumes[['Lane','Cost','PlantType']], how = 'left', left_on = 'P3 Lane', right_on = 'Lane')
    oo = oo.rename(columns = {'PlantType':'P3 Plant Type','Cost':'P3 Lane Cost','LOH':'P3 Lane LOH','OTP':'P3 Lane OTP','Capacity':'P3 Cap','Current Inventory':'P3 CI','Current Plant Cap %':'P3 CPCP','Stock':'P3 Stock','RUPlusCGen':'P3 C-Gen','BookedQuantity':'P3 Booked',"Today's Inventory":'P3 Today Inventory','AvgQuantity':'P3 Funnel','Buffer':'P3 Buffer'})
    
    # Cutting and reordering the columns 
    oo = oo[['FiscalDate','Customer','SO-Item','Sales Order','Sales Order Item','DFU','Delivery Type','Delivery Quantity','P1','P1 C-Gen','P1 Booked','P1 Today Inventory','P1 Buffer','P1 Lane Cost','P1 Funnel','P1 Plant Type','P2','P2 C-Gen','P2 Booked','P2 Today Inventory','P2 Buffer','P2 Lane Cost','P2 Funnel','P2 Plant Type','P3','P3 C-Gen','P3 Booked','P3 Today Inventory','P3 Buffer','P3 Lane Cost','P3 Funnel','P3 Plant Type','Closest_Plant']]
    
    #####################################
    #        Cleansing The Data         #
    #####################################
    # Need to put the orders with no P1 in a special table for extra care
    noP1 = oo[oo['P1'].isnull()]
    # Cutting these from the automated table
    oo = oo.dropna(subset = ['P1'])
    # Creating the first table of non-automated loads, these loads dont have any info for their P1, therefore 
    # they need extra care
    #noP1Info = oo[(oo['P1 C-Gen'].isnull()) | (oo['P1 Booked'].isnull()) | (oo['P1 Today Inventory'].isnull())]
    noP1Info = oo[(oo['P1 Booked'].isnull()) | (oo['P1 Today Inventory'].isnull())]
    # Cutting these from the automated table
    #oo = oo.dropna(subset = ['P1 C-Gen','P1 Booked','P1 Today Inventory'], how = 'any')
    oo = oo.dropna(subset = ['P1 Booked','P1 Today Inventory'], how = 'any')
    # Gotta make it an integer so it's sortable so we can to FIFO these loads
    oo['SO-Item'] = oo['SO-Item'].astype(float).astype(np.int64).astype(str).astype(np.int64)
    oo = oo.replace('-', np.nan)
    
    # Sometimes there's a set of open orders that have no P2 or P3, this causes problems,
    # because it converts it to float64 which can't be merged further down.
    # Before I reach through this screen and press my finger against your lips to prevent
    # you from speaking, yes, this is here twice, because for some strange reason, these 
    # columns decided to go back to float64 if there's no source in the entire column...
    oo['P1'] = oo['P1'].astype('object')   
    oo['P1 Booked'] = oo['P1 Booked'].astype(int)  
    oo['P2'] = oo['P2'].astype('object')  
    oo['P2 Booked'].fillna(0,inplace = True)
    oo['P2 Booked'] = oo['P2 Booked'].astype(int) 
    oo['P3'] = oo['P3'].astype('object')  
    oo['P3 Booked'].fillna(0,inplace = True) 
    oo['P3 Booked'] = oo['P3 Booked'].astype(int)  
    oo['Delivery Quantity'] = oo['Delivery Quantity'].astype(int)  
    
    #####################################
    #     First Steps of Phase 1        #
    #####################################
    # Making the P availabilities: Daily Outbound Capacity - Currently Booked
    oo['P1 Available'] = oo.loc[:,'P1 Funnel'] - oo.loc[:,'P1 Booked']
    oo['P2 Available'] = oo.loc[:,'P2 Funnel'] - oo.loc[:,'P2 Booked']
    oo['P3 Available'] = oo.loc[:,'P3 Funnel'] - oo.loc[:,'P3 Booked'] 
    oo['P1 Adj Today Inventory'] = oo['P1 Today Inventory'] + oo.loc[:,'P1 C-Gen']
    oo['P2 Adj Today Inventory'] = oo['P2 Today Inventory'] + oo.loc[:,'P2 C-Gen']
    oo['P3 Adj Today Inventory'] = oo['P3 Today Inventory'] + oo.loc[:,'P3 C-Gen']
    
    p1 = oo[['FiscalDate','P1','P1 Available','P1 Today Inventory']].rename(columns = {'P1':'Source','P1 Available':'Available','P1 Today Inventory':'Inventory'})
    p2 = oo[['FiscalDate','P2','P2 Available','P2 Today Inventory']].rename(columns = {'P2':'Source','P2 Available':'Available','P2 Today Inventory':'Inventory'})
    p3 = oo[['FiscalDate','P3','P3 Available','P3 Today Inventory']].rename(columns = {'P3':'Source','P3 Available':'Available','P3 Today Inventory':'Inventory'})
    allsources = pd.concat([p1,p2,p3]).dropna()
    allsources.drop_duplicates(keep = 'first', inplace = True) 
    
    # The sources availabilities are now compared to the current on hand inventory.
    # As long as both fields are above zero, we will use the available column as the correct availability as long as the inventory supports it, otherwise, we will use the inventory number since that's what the plant has on hand.
    allsources['Available'] = np.where((allsources['Available'] > 0) & (allsources['Inventory'] > 0) & (allsources['Available'] < allsources['Inventory']),allsources['Available'], allsources['Inventory'])
    allsources = allsources.sort_values(by = ['FiscalDate','Source'])
    
    ######################################################
    ##                      Phase 1                     ##
    ######################################################        
    # Making the husks in order for the new tables to join to it
    readyorders = pd.DataFrame(columns = oo.columns)
    leftovers = pd.DataFrame(columns = oo.columns)
    p2 = pd.DataFrame(columns = oo.columns)
    p3 = pd.DataFrame(columns = oo.columns)
    oo = oo.sort_values(by = ['FiscalDate','P1 Lane Cost'])
    
    # Before start spinnin wheels, lets cut some of the low hanging fruit from the tree by removing the plants whos orders P1 2 or 3 are not more than the buffer
    leftovers = oo.loc[((oo['P1 Available'] < oo['P1 Buffer']) | (np.isnan(oo['P1 Available'])) ) & ((oo['P2 Available'] < oo['P2 Buffer']) | (np.isnan(oo['P2 Available'])) ) & ((oo['P3 Available'] < oo['P3 Buffer']) | (np.isnan(oo['P3 Available'])) )]        
    oo2 = oo.drop(oo[((oo['P1 Available'] < oo['P1 Buffer']) | (np.isnan(oo['P1 Available']))) & ((oo['P2 Available'] < oo['P2 Buffer']) | (np.isnan(oo['P2 Available']))) & ((oo['P3 Available'] < oo['P3 Buffer']) | (np.isnan(oo['P3 Available'])))].index)
    # Since joining does not make each single buffer column 541, doing it here
    oo2 = oo2.replace({'P1 Buffer': np.nan,'P2 Buffer': np.nan,'P3 Buffer': np.nan}, 541)
    oo2 = oo2.drop_duplicates()
    
    # Predefined code functions used to update the sources table, state what source will be used and add it to the ready orders
    def p1ordered():
        global readyorders
        allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P1'].values[0]) & (allsources.loc[:,'FiscalDate'] >= oo2i.loc[:,'FiscalDate'].values[0]),'Available'] = allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P1'].values[0]) & (allsources.loc[:,'FiscalDate'] >= oo2i.loc[:,'FiscalDate'].values[0]),'Available'] - oo.loc[oo.loc[:,'SO-Item'] == oo2i.loc[:,'SO-Item'].values[0]]['Delivery Quantity'].values[0]     
        oo2i.loc[:,'Source'] = oo2i.loc[:,'P1'].values[0]
        readyorders = pd.concat([oo2i, readyorders],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
    def p2ordered():
        global readyorders
        allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P2'].values[0]) & (allsources.loc[:,'FiscalDate'] >= oo2i.loc[:,'FiscalDate'].values[0]),'Available'] = allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P2'].values[0]) & (allsources.loc[:,'FiscalDate'] >= oo2i.loc[:,'FiscalDate'].values[0]),'Available'] - oo.loc[oo.loc[:,'SO-Item'] == oo2i.loc[:,'SO-Item'].values[0]]['Delivery Quantity'].values[0]     
        oo2i.loc[:,'Source'] = oo2i.loc[:,'P2'].values[0]
        readyorders = pd.concat([oo2i, readyorders],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
    def p3ordered():
        global readyorders
        allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P3'].values[0]) & (allsources.loc[:,'FiscalDate'] >= oo2i.loc[:,'FiscalDate'].values[0]),'Available'] = allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P3'].values[0]) & (allsources.loc[:,'FiscalDate'] >= oo2i.loc[:,'FiscalDate'].values[0]),'Available'] - oo.loc[oo.loc[:,'SO-Item'] == oo2i.loc[:,'SO-Item'].values[0]]['Delivery Quantity'].values[0]     
        oo2i.loc[:,'Source'] = oo2i.loc[:,'P3'].values[0]
        readyorders = pd.concat([oo2i, readyorders],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
    
    ##############################
    ##         P1 Loop          ##
    ##############################
    # Runs pass 1 for all orders that work with P1.
    # We split it up because we want to make sure that a P2 plant doesnt get priority over a P1 Plant
    items = oo2['SO-Item'].unique()
    auditTable = pd.DataFrame(data = [], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
    for item in items:
        # Isolate just the item
        oo2i= oo2.loc[oo2.loc[:,'SO-Item']== item].reset_index(drop = True)
        
        # Grab most up to date availability from the allsources table
        # P1
        if len(allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P1'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available']) > 0:
             oo2i.loc[0,'P1 Available'] = allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P1'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available'].values[0]
        else:
             oo2i.loc[0,'P1 Available'] = np.nan    
        # P2    
        if len(allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P2'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available']) > 0:
             oo2i.loc[0,'P2 Available'] = allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P2'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available'].values[0]
        else:
             oo2i.loc[0,'P2 Available'] = np.nan
        # P3
        if len(allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P3'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available']) > 0:
             oo2i.loc[0,'P3 Available'] = allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P3'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available'].values[0]
        else:
             oo2i.loc[0,'P3 Available'] = np.nan
        
        # P1 Only, as long as P1 is over the buffer and the others are not and p1 cost is not blank
        if (oo2i.loc[:,'P1 Available'].values[0] >= oo2i['P1 Buffer'].values[0] and (oo2i.loc[:,'P2 Available'].values[0] < oo2i['P2 Buffer'].values[0] or np.isnan(oo2i.loc[:,'P2 Available'].values[0])) and (oo2i.loc[:,'P3 Available'].values[0] < oo2i.loc[:,'P3 Buffer'].values[0] or np.isnan(oo2i.loc[:,'P3 Available'].values[0]))):
            p1ordered()    
            orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P1'].values[0], '1','Only P1 Is There']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
            # Adding the load's response to the main audit table
            auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True)          
            print('Booked item' +str(item)+' P1 Because it was the only one')
        # P1 and P2 are viable options
        elif (oo2i.loc[0,'P1 Available'] >= oo2i.loc[:,'P1 Buffer'].values[0] and oo2i.loc[:,'P2 Available'].values[0] >= oo2i.loc[:,'P2 Buffer'].values[0] and (oo2i.loc[:,'P3 Available'].values[0] < oo2i.loc[:,'P3 Buffer'].values[0] or np.isnan(oo2i.loc[:,'P3 Available'].values[0]))):   
            # Business rule: TPMs have priority, if P1 is a TPM it's the choice.. always
            if oo2i.loc[0,'P1 Plant Type'] == 'TPM':
                p1ordered()    
                orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P1'].values[0], '1','P1 Is A TPM']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
                # Adding the load's response to the main audit table
                auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True)
                print('Booked ' +str(item)+' P1 Because it was a TPM')
            # P1 is cheaper than P2
            elif oo2i.loc[0,'P1 Lane Cost'] < oo2i.loc[:,'P2 Lane Cost'].values[0]:            
                p1ordered()
                orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P1'].values[0], '1','P1 Is Cheaper']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
                # Adding the load's response to the main audit table
                auditTable = pd.concat([auditTable,orderaudit],join = 'outer', ignore_index = False, sort = True)  
                print('Booked ' +str(item)+' P1 because it was cheaper')
            else:
                p2 = pd.concat([oo2i, p2],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
                print(str(item)+' going to p2 phase because all P1s have been exhausted')        
        # P1 and P3 are viable options
        elif ( oo2i.loc[:,'P1 Available'].values[0] >= oo2i['P1 Buffer'].values[0] and (oo2i.loc[:,'P2 Available'].values[0] < oo2i['P2 Buffer'].values[0] or np.isnan(oo2i.loc[:,'P2 Available'].values[0])) and oo2i.loc[:,'P3 Available'].values[0] >= oo2i['P3 Buffer'].values[0] ):   
            # Business rule: TPMs have priority, if P1 is a TPM it's the choice.. always
            if oo2i.loc[0,'P1 Plant Type'] == 'TPM':
                p1ordered()
                orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P1'].values[0], '1','P1 Is A TPM']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
                # Adding the load's response to the main audit table
                auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True)  
                print('Booked ' +str(item)+' P1 Because it was a TPM')
            # P1 is cheaper than P3
            elif oo2i.loc[0,'P1 Lane Cost'] < oo2i.loc[:,'P3 Lane Cost'].values[0]:  
                p1ordered()            
                orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P1'].values[0], '1','P1 Is Cheaper']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
                # Adding the load's response to the main audit table
                auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True)  
                print('Booked ' +str(item)+' P1 Because it was Cheaper')
            else:
                p2 = pd.concat([oo2i, p2],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
                print(str(item)+' going to p2 phase because all P1s have been exhausted')   
        # If all 3 Ps have more than the buffer of availability
        elif (oo2i.loc[:,'P1 Available'].values[0] >= oo2i['P1 Buffer'].values[0] and oo2i.loc[:,'P2 Available'].values[0] >= oo2i['P2 Buffer'].values[0] and oo2i.loc[:,'P3 Available'].values[0] >= oo2i['P3 Buffer'].values[0]):   
   
            # Business rule: TPMs have priority, if P1 is a TPM it's the choice.. always
            if oo2i.loc[0,'P1 Plant Type'] == 'TPM':
                p1ordered() 
                orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P1'].values[0], '1','P1 Is A TPM']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])            
                # Adding the load's response to the main audit table
                auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True)  
                print('Booked ' +str(item)+' P1 Because it was a TPM')
            # P1 is cheapest - allowing p1 cost and p2 cost to equal and choosing p1 because sometimes p1 and p2 are the same plant
            elif (oo2i.loc[0,'P1 Lane Cost'] <= oo2i.loc[:,'P2 Lane Cost'].values[0]) & (oo2i.loc[:,'P1 Lane Cost'].values[0] < oo2i.loc[:,'P3 Lane Cost'].values[0]):            
                p1ordered() 
                orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P1'].values[0], '1','P1 Is Cheaper']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])            
                # Adding the load's response to the main audit table
                auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True)  
                print('Booked ' +str(item)+' P1 because it was cheaper')
            else:
                p2 = pd.concat([oo2i, p2],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
                print(str(item)+' going to p2 phase because all P1s have been exhausted')   
        # P1 is not usable, now checking all the P2 possibilities
        else:  
            p2 = pd.concat([oo2i, p2],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
            print(str(item)+' going to p2 phase because all P1s have been exhausted')           
           
    ##############################
    ##         P2 Loop          ##
    ##############################    
    items = p2['SO-Item'].unique()
    for item in items:
       # Isolate just the item
        oo2i= p2.loc[p2['SO-Item']== item].reset_index(drop = True)
        # Grab most up to date availability from the allsources table
        # P1
        if len(allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P1'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available']) > 0:
             oo2i.loc[0,'P1 Available'] = allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P1'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available'].values[0]
        else:
             oo2i.loc[0,'P1 Available'] = np.nan    
        # P2    
        if len(allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P2'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available']) > 0:
             oo2i.loc[0,'P2 Available'] = allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P2'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available'].values[0]
        else:
             oo2i.loc[0,'P2 Available'] = np.nan
        # P3
        if len(allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P3'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available']) > 0:
             oo2i.loc[0,'P3 Available'] = allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P3'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available'].values[0]
        else:
             oo2i.loc[0,'P3 Available'] = np.nan
        # P2 Only, as long as P2 is over the buffer and the others are not
        if ((oo2i.loc[:,'P1 Available'].values[0] < oo2i.loc[:,'P1 Buffer'].values[0] or np.isnan(oo2i.loc[:,'P1 Available'].values[0])) and oo2i.loc[:,'P2 Available'].values[0] >= oo2i.loc[:,'P2 Buffer'].values[0] and (oo2i.loc[:,'P3 Available'].values[0] < oo2i.loc[:,'P3 Buffer'].values[0] or np.isnan(oo2i.loc[:,'P3 Available'].values[0]))):          
            p2ordered()   
            orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P2'].values[0], '1','Only P2 Is There']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
            # Adding the load's response to the main audit table
            auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True) 
            print('Booked item' +str(item)+' P2 Because it was the only one')
        # P1 and P2 are viable options
        elif (oo2i.loc[:,'P1 Available'].values[0] >= oo2i.loc[:,'P1 Buffer'].values[0] and oo2i.loc[:,'P2 Available'].values[0] >= oo2i.loc[:,'P2 Buffer'].values[0] and (oo2i.loc[:,'P3 Available'].values[0] < oo2i.loc[:,'P3 Buffer'].values[0] or np.isnan(oo2i.loc[:,'P3 Available'].values[0]))):      
            # P2 is cheaper than P1
            if oo2i.loc[:,'P1 Lane Cost'].values[0] > oo2i.loc[:,'P2 Lane Cost'].values[0]:            
                p2ordered()
                orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P2'].values[0], '1','P2 Is Cheaper']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
                # Adding the load's response to the main audit table
                auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True)  
                print('Booked ' +str(item)+' P2 because it was cheaper')
            else:  
                p3 = pd.concat([oo2i, p3],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
                print(str(item)+' going to p3 phase because all P2s have been exhausted')
        # P2 and P3 are viable options
        elif (oo2i.loc[:,'P1 Available'].values[0] < oo2i.loc[:,'P1 Buffer'].values[0] or np.isnan(oo2i.loc[:,'P1 Available'].values[0])) and (oo2i.loc[:,'P2 Available'].values[0] >= oo2i.loc[:,'P2 Buffer'].values[0] and oo2i.loc[:,'P3 Available'].values[0] >= oo2i.loc[:,'P3 Buffer'].values[0]):      
            # P2 is cheaper than P3
            if oo2i.loc[:,'P2 Lane Cost'].values[0] < oo2i.loc[:,'P3 Lane Cost'].values[0]:            
                p2ordered()  
                orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P2'].values[0], '1','P2 Is Cheaper']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
                # Adding the load's response to the main audit table
                auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True)  
                print('Booked ' +str(item)+' P2 because it was cheaper')
            else:  
                p3 = pd.concat([oo2i, p3],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
                print(str(item)+' going to p3 phase because all P2s have been exhausted')
                
        # If all 3 Ps have more than the buffer of availability
        elif (oo2i.loc[:,'P1 Available'].values[0] >= oo2i.loc[:,'P1 Buffer'].values[0] and oo2i.loc[:,'P2 Available'].values[0] >= oo2i.loc[:,'P2 Buffer'].values[0] and oo2i.loc[:,'P3 Available'].values[0] >= oo2i.loc[:,'P3 Buffer'].values[0]):   
            # P2 is cheapest
            if (oo2i.loc[:,'P2 Lane Cost'].values[0] < oo2i.loc[:,'P3 Lane Cost'].values[0]) & (oo2i.loc[:,'P2 Lane Cost'].values[0] < oo2i.loc[:,'P1 Lane Cost'].values[0]):            
                p2ordered()  
                orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P2'].values[0], '1','P2 Is Cheaper']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
                # Adding the load's response to the main audit table
                auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True)  
                print('Booked ' +str(item)+' P2 because it was cheaper')                
            else:  
                p3 = pd.concat([oo2i, p3],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
                print(str(item)+' going to p3 phase because all P2s have been exhausted')
        # P1 is not usable, now checking all the P2 possibilities
        else:  
            p3 = pd.concat([oo2i, p3],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
            print(str(item)+' going to p3 phase because all P2s have been exhausted')
               
    ##############################
    ##         P3 Loop          ##
    ##############################  
    items = p3['SO-Item'].unique()
    for item in items:
        # Isolate just the item
        oo2i= p3.loc[p3['SO-Item']== item].reset_index(drop = True)
        # Grab most up to date availability from the allsources table
        # P1
        if len(allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P1 Buffer'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available']) > 0:
             oo2i.loc[:,'P1 Available'] = allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P1'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available'].values[0]
        else:
             oo2i.loc[:,'P1 Available'] = np.nan    
        # P2    
        if len(allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P2'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available']) > 0:
             oo2i.loc[:,'P2 Available'] = allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P2'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available'].values[0]
        else:
             oo2i.loc[:,'P2 Available'] = np.nan
        # P3
        if len(allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P3'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available']) > 0:
             oo2i.loc[:,'P3 Available'] = allsources.loc[(allsources.loc[:,'Source'] == oo2i.loc[:,'P3'].values[0]) & (allsources.loc[:,'FiscalDate'] == oo2i.loc[:,'FiscalDate'].values[0]),'Available'].values[0]
        else:
             oo2i.loc[:,'P3 Available'] = np.nan
        # P3 Only, as long as P3 is over the buffer and the others are not
        if ((oo2i.loc[:,'P1 Available'].values[0] < oo2i.loc[:,'P1 Buffer'].values[0] or np.isnan(oo2i.loc[:,'P1 Available'].values[0])) and (oo2i.loc[:,'P2 Available'].values[0] < oo2i.loc[:,'P2 Buffer'].values[0] or np.isnan(oo2i.loc[:,'P2 Available'].values[0])) and oo2i.loc[:,'P3 Available'].values[0] >= oo2i.loc[:,'P3 Buffer'].values[0]):          
            p3ordered() 
            orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P3'].values[0], '1','Only P3 Is There']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
            # Adding the load's response to the main audit table
            auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True)         
            print('Booked item' +str(item)+' P3 Because it was the only one')
        # P1 and P3 are viable options
        elif ( oo2i.loc[:,'P1 Available'].values[0] >= oo2i.loc[:,'P1 Buffer'].values[0] and (oo2i.loc[:,'P2 Available'].values[0] < oo2i.loc[:,'P2 Buffer'].values[0] or np.isnan(oo2i.loc[:,'P2 Available'].values[0])) and oo2i.loc[:,'P3 Available'].values[0] >= oo2i.loc[:,'P3 Buffer'].values[0] ):        
            # P1 is cheaper than P3
            if oo2i.loc[:,'P1 Lane Cost'].values[0] > oo2i.loc[:,'P3 Lane Cost'].values[0]:            
                p3ordered()  
                orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P3'].values[0], '1','P3 Is Cheaper']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
                # Adding the load's response to the main audit table
                auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True)              
                print('Booked ' +str(item)+' P3 because it was cheaper')
            else:  
                leftovers = pd.concat([oo2i, leftovers],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
                print(str(item)+' going to phase 2 because all P3s have been exhausted')  
        # P2 and P3 are viable options
        elif (oo2i.loc[:,'P1 Available'].values[0] < oo2i.loc[:,'P1 Buffer'].values[0] or np.isnan(oo2i.loc[:,'P1 Available'].values[0])) and (oo2i.loc[:,'P2 Available'].values[0] >= oo2i.loc[:,'P2 Buffer'].values[0] and oo2i.loc[:,'P3 Available'].values[0] >= oo2i.loc[:,'P3 Buffer'].values[0]):      
            # P3 is cheaper than P2
            if oo2i.loc[:,'P2 Lane Cost'].values[0] > oo2i.loc[:,'P3 Lane Cost'].values[0]:            
                p3ordered()    
                orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P3'].values[0], '1','P3 Is Cheaper']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
                # Adding the load's response to the main audit table
                auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True)              
                print('Booked ' +str(item)+' P3 because it was cheaper')
            else:  
                leftovers = pd.concat([oo2i, leftovers],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
                print(str(item)+' going to phase 2 because all P3s have been exhausted')  
        # If all 3 Ps have more than the buffer of availability
        elif (oo2i.loc[:,'P1 Available'].values[0] >= oo2i.loc[:,'P1 Buffer'].values[0] and oo2i.loc[:,'P2 Available'].values[0] >= oo2i.loc[:,'P2 Buffer'].values[0] and oo2i.loc[:,'P3 Available'].values[0] >= oo2i.loc[:,'P3 Buffer'].values[0]):   
            # P3 is cheapest
            if (oo2i.loc[:,'P3 Lane Cost'].values[0] < oo2i.loc[:,'P2 Lane Cost'].values[0]) & (oo2i.loc[:,'P3 Lane Cost'].values[0] < oo2i.loc[:,'P1 Lane Cost'].values[0]):            
                p2ordered() 
                orderaudit = pd.DataFrame(data = [[oo2i.loc[:,'FiscalDate'].values[0],oo2i.loc[:,'Customer'].values[0],item,oo2i.loc[:,'P3'].values[0], '1','P3 Is Cheaper']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
                # Adding the load's response to the main audit table
                auditTable = pd.concat([auditTable,orderaudit],join = 'outer',  ignore_index = False, sort = True) 
                print('Booked ' +str(item)+' P3 because it was cheaper')
            else:  
                leftovers = pd.concat([oo2i, leftovers],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
                print(str(item)+' going to phase 2 because all P3s have been exhausted')  
        # P3 is not usable, now putting them in leftovers for phase 2
        else:  
            leftovers = pd.concat([oo2i, leftovers],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
            print(str(item)+' going to phase 2 because all P3s have been exhausted')           
    
    #readyorders = readyorders[['FiscalDate','Customer','DFU','SO-Item','Sales Order','Sales Order Item','Delivery Type','Source','Delivery Quantity']]
    
    ######################################################
    ##                      Phase 2                     ##
    ######################################################        
    leftovers['Reason'] = 'No Availability in P1,2,3'
    noP1Info['Reason'] = 'No P1 Info'
    noP1['Reason'] = 'No P1'
    # Creating the phase 2 list of orders to figure out the best way to book them            
    phase2 = pd.concat([leftovers,noP1Info,noP1], sort = True)  
    # Cuts the columns we won't use
    #phase2['Customer'] = phase2['Customer']
    phase2 = phase2[['FiscalDate','Customer','SO-Item','Sales Order','Sales Order Item','DFU','Delivery Type','Delivery Quantity']]
    
    # Collecting the volumes data and formatting it to what we need and cutting anything that isn't a service center or doesnt have the selection criteria
    phase2volumes = volumes[volumes.DGlobal.isin(phase2['Customer'].unique())]
    phase2volumes = phase2volumes.replace('-', np.nan)
    phase2volumes = phase2volumes.dropna(subset = ['Cost','LOH','OTP'], how = 'any')
    phase2volumes = phase2volumes.drop(phase2volumes[phase2volumes['PlantType'].map(lambda x: str(x) != 'SC')].index)

    # Creating the capacities table because we need to make sure the plant capacities data matches from Phase 1. 
    # To do this, we need to stack them, doing this here in these lines of code the lazy way.    
    leftovers['P1 Usable Stock'] = leftovers['P1 Available'] - leftovers['P1 Buffer']
    leftovers['P2 Usable Stock'] = leftovers['P2 Available'] - leftovers['P2 Buffer']
    leftovers['P3 Usable Stock'] = leftovers['P3 Available'] - leftovers['P3 Buffer']
    leftoveravailabilities = leftovers[['FiscalDate','Customer','SO-Item','Sales Order','Sales Order Item','DFU','Delivery Type','Delivery Quantity','P1','P1 Usable Stock','P2','P2 Usable Stock','P3','P3 Usable Stock']]    
    capacities1 = leftoveravailabilities[['FiscalDate','Customer','SO-Item','Sales Order','Sales Order Item','DFU','Delivery Type','Delivery Quantity','P1','P1 Usable Stock']]
    capacities1 = capacities1.rename(columns = {'P1':'Plant','P1 Usable Stock':'Available2'})
    capacities2 = leftoveravailabilities[['FiscalDate','Customer','SO-Item','Sales Order','Sales Order Item','DFU','Delivery Type','Delivery Quantity','P2','P2 Usable Stock']]
    capacities2 = capacities2.rename(columns = {'P2':'Plant','P2 Usable Stock':'Available2'})
    capacities3 = leftoveravailabilities[['FiscalDate','Customer','SO-Item','Sales Order','Sales Order Item','DFU','Delivery Type','Delivery Quantity','P3','P3 Usable Stock']]
    capacities3 = capacities3.rename(columns = {'P3':'Plant','P3 Usable Stock':'Available2'})
    capacities = pd.concat([capacities1,capacities2,capacities3])
    capacities['Customer'] = capacities['Customer'].astype(np.int64)
    capacities = capacities.dropna(subset = ['Plant'], how = 'any')

    
    # Collecting the availability and formatting it
    origins = plantcapacity[plantcapacity.Plant.isin(phase2volumes['OGlobal'].unique())]
    origins = origins.merge(allsources[['Source','Available','FiscalDate']], how = 'left', left_on = ['Date','Plant'], right_on = ['FiscalDate','Source'])


    # origins = allsources[['Source','Available','FiscalDate']]
    # origins = origins.merge(plantcapacity[plantcapacity.Plant.isin(phase2volumes['OGlobal'].unique())],how = 'left', right_on = ['Date','Plant'], left_on = ['FiscalDate','Source'])

    origins['Inventory'] = origins["Today's Inventory"] + origins['RUPlusCGen']
    origins['Available2'] = origins['AvgQuantity'] - origins['BookedQuantity'].astype(np.int64)
    origins['Available3'] = np.where((origins['Inventory'] > 0) & (origins['Available2'] < origins['Inventory']),origins['Available2'], origins['Inventory'])
    origins['Available'].fillna(origins['Available2'], inplace=True)
    origins = origins[['Date','Plant','Available','Buffer']]
    
    # Combining the availabilities and possible sources tables
    origins = origins.merge(phase2volumes, how = 'left', left_on = ['Plant'], right_on = ['OGlobal'])
    # Cutting the locations that don't have availability
    origins = origins.drop(origins[origins['Available'] <= origins['Buffer']].index)
    origins['Available'] = origins['Available'] - origins['Buffer']
    
    # Combining the last 2 tables into the actual orders that need to be fulfilled
    phase2Orders = phase2.merge(origins, how = 'left', right_on = ['Date','DGlobal'], left_on = ['FiscalDate','Customer'])
    phase2Orders['Delivery Quantity'] = phase2Orders['Delivery Quantity'].astype(np.int64)    
    phase2Orders['Customer'] = phase2Orders['Customer'].astype(np.int64)    


    phase2Orders2 = phase2Orders.merge(capacities, how = 'left',on = ['FiscalDate','Customer','SO-Item','Sales Order','Sales Order Item','DFU','Delivery Type','Delivery Quantity','Plant'])
    phase2Orders2['Available2'].fillna(phase2Orders2['Available'], inplace=True)

    phase2Orders2 = phase2Orders2.drop(['Available'], axis = 1)
    phase2Orders = phase2Orders2.rename(columns = {'Available2':'Available'})
    # For some odd reason, phase 2 had dupes, removing the dupes so it can correctly count the orders with multiple possible plants
    phase2Orders = phase2Orders.drop_duplicates()
          
    # We need to separate out the customers that have sources to work with, and ones that don't
    phase2Ordersgb = phase2Orders[['FiscalDate','Customer','SO-Item','Plant']].groupby(['FiscalDate','Customer','SO-Item']).count().reset_index()
    # Making a list that of orders no possible plants to put in phase 3
    noPlantOrders = phase2Ordersgb.loc[phase2Ordersgb['Plant'] == 0]['SO-Item']
    # Making a list of orders with one singlular possibility
    oneplantorders = phase2Ordersgb.loc[phase2Ordersgb['Plant'] == 1]['SO-Item']
    # Making a list of orders with more than one possibility to put through phase 2
    usableOrders = phase2Ordersgb.loc[phase2Ordersgb['Plant'] > 1]['SO-Item']
    
    # Partitions off just the customers who have no possible sources  
    leftovers = phase2Orders[phase2Orders['SO-Item'].isin(noPlantOrders)]
    leftovers['Reason'] = 'No Possible Sources'
    
    # Partitions off just the customers who have one viable option
    onePlantOrders = phase2Orders[phase2Orders['SO-Item'].isin(oneplantorders)]
    # We want to focus on using the cheapest lanes first, so we focus on date and cost sorting to make sure we get the cheapest lane and availability 
    onePlantOrders = onePlantOrders.sort_values(by = ['FiscalDate','Cost'])
    
    # We want to force the decision making process on the LC if the cost is over 1200$
    tooexpensive = pd.DataFrame([])
    tooexpensive = onePlantOrders[onePlantOrders['Cost'] >= 1200]
    tooexpensive['Reason'] = 'Too Expensive'
    onePlantOrders = onePlantOrders[onePlantOrders['Cost'] < 1200]
    
    # Joining the first leftovers to the next piece to make it all one leftover object
    leftovers = pd.concat([leftovers,tooexpensive],axis = 0, join = 'outer',  ignore_index = False, sort = True)
    
    # Partitions off the rest of them
    multiplePlantOrders = phase2Orders[phase2Orders['SO-Item'].isin(usableOrders)]
    
    # multiplePlantOrders2 = multiplePlantOrders[['SO-Item','Plant']].groupby(['SO-Item']).count().reset_index()
    # multiplePlantOrders2 = multiplePlantOrders2.rename(columns = {'Plant':'Total'})
    # multiplePlantOrders3 = multiplePlantOrders[['SO-Item','Plant']][multiplePlantOrders[['SO-Item','Plant','Cost']]['Cost'] >=1200].groupby(['SO-Item']).count().reset_index()
    # multiplePlantOrders3 = multiplePlantOrders3.rename(columns = {'Plant':'Over 1200'})
    # multiplePlantOrders4 = multiplePlantOrders3.merge(multiplePlantOrders2, how = 'left', on = ['SO-Item']).fillna(0)

    
    # Stops the orders that are too expensive to the network for multiple orders
    # First it takes just the so-items of the list
    soitems = multiplePlantOrders['SO-Item'].unique()
    # loops through each of the orders
    for item in soitems:
        # Isolates the order in the loop
        order = multiplePlantOrders.loc[multiplePlantOrders['SO-Item'] == item].reset_index(drop = True)
        # Tracks the amount of possible sources
        lanecount = order.index.size
        # Tracks the amount of expensive sources
        expensiveorders = order[order.Cost >= 1200].count()['Cost']
        # If all possible sources are too expensive, it removes the order from the multiple orders table and adds it to leftovers for lcs to book
        if expensiveorders == lanecount:
            # Takes just the first row because none of the others matter
            order = order.head(1)
            order['Reason'] = 'Too Expensive'
            # Adding the order to the leftovers
            leftovers = pd.concat([order, leftovers],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
            multiplePlantOrders = multiplePlantOrders.loc[multiplePlantOrders['SO-Item'] != item]
        else:
            multiplePlantOrders = multiplePlantOrders.drop(multiplePlantOrders[(multiplePlantOrders['SO-Item'].map(lambda x: x == item)) & (multiplePlantOrders['Cost'].map(lambda x: x >= 1200))].index)

            
    # We need to separate out the customers that have 1 source only, first we make a groupby to figure this out
    multiplePlantOrdersgb = multiplePlantOrders[['FiscalDate','Customer','SO-Item','Plant']].groupby(['FiscalDate','Customer','SO-Item']).count().reset_index()
    # Figuring out which ones have 1 source
    singletons = multiplePlantOrdersgb.loc[multiplePlantOrdersgb['Plant'] == 1]['SO-Item']  
    # Making a table with them to add it to the one plant orders phase
    singletonsdf = multiplePlantOrders[multiplePlantOrders['SO-Item'].isin(singletons)]
    # Adding it to the one plant orders phase
    onePlantOrders = pd.concat([singletonsdf,onePlantOrders],axis = 0, join = 'outer',  ignore_index = False, sort = True)
    # Removing them from the multiple plant orders now
    multiplePlantOrders = multiplePlantOrders[~multiplePlantOrders['SO-Item'].isin(singletons)]

    
    ##############################
    ##      Singleton Loop      ##
    ##############################  
    # Updating each of the singletons and their availability for the multiples next
    # Setting up a catch where if the updated value is less than 540 it doesnt change it and moves it to phase 3
    soItems = onePlantOrders['SO-Item'].unique()
    for item in soItems:
        # Isolates the specific SO
        onePlantOrdersSO = onePlantOrders.loc[onePlantOrders['SO-Item'] == item].reset_index(drop = True)
        # We want to make sure that it's usable
        if onePlantOrdersSO['Available'].values[0] > onePlantOrdersSO['Delivery Quantity'].values[0] :
            # We rename plant to source because source is the consistent verbage
            onePlantOrdersSO = onePlantOrdersSO.rename(columns = {'Plant':'Source'})
            # Collecting the Average LOH for all historically used plants            
            soVolumes = phase2volumes.loc[phase2volumes['DGlobal'] == onePlantOrdersSO['Customer'].values[0]]
            # Calculating the average LOH
            avgloh = np.mean(soVolumes['LOH'])*1.25
            # Creating the restriction where if the singular plant is 25% above the average LOH, it will be sent to the LC to book             
            if onePlantOrdersSO['LOH'].values[0] > avgloh:
                onePlantOrdersSO['Reason'] = 'Single Source LOH Over Threshold' 
                leftovers = pd.concat([onePlantOrdersSO, leftovers],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
                print('singleton :'+str(item)+' LOH Above Threshold, putting in leftovers for phase3')
            else:
                # We know that this is usable, so we add it to the readyorders for winshuttle to use
                readyorders = pd.concat([onePlantOrdersSO[['FiscalDate','Customer','DFU','SO-Item','Sales Order','Sales Order Item','Delivery Type','Source','Delivery Quantity']], readyorders],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
                # We are updating the oneplantorders table
                onePlantOrders.at[onePlantOrdersSO.index.values[0],'Available'] = onePlantOrdersSO.at[onePlantOrdersSO.index.values[0],'Available'] - onePlantOrdersSO.at[onePlantOrdersSO.index.values[0],'Delivery Quantity']
                # We need to update the allsources table, some sources are not in this table, this updates the available ones and adds to it if it doesnt already exist
                allsources.loc[(allsources['Source'] == onePlantOrdersSO['Source'].values[0]) & (allsources['FiscalDate'] == onePlantOrdersSO['Date'].values[0]),'Available'] = onePlantOrdersSO.at[onePlantOrdersSO.index.values[0],'Available'] - onePlantOrdersSO.at[onePlantOrdersSO.index.values[0],'Delivery Quantity']                
                print('singleton :'+str(item)+' has been set to be ordered')
                onepoaudittable = pd.DataFrame(data = [[onePlantOrdersSO['Date'].values[0],onePlantOrdersSO['Customer'].values[0],item,onePlantOrdersSO['Source'].values[0], '2','Only Source Available']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
                auditTable = pd.concat([auditTable,onepoaudittable],join = 'outer',  ignore_index = False, sort = True)                      
                # We are checking to see if this specific source/date combo is in the allsources table, if not, we will add it to it
                if len(allsources.loc[(allsources['Source'] == onePlantOrdersSO['Source'].values[0]) & (allsources['FiscalDate'] == onePlantOrdersSO['FiscalDate'].values[0])]) == 0  :
                    allsources = pd.concat([onePlantOrdersSO[['FiscalDate','Source','Available']], allsources], sort = True)
        else: 
            onePlantOrdersSO.loc[0,'Reason'] = 'Single Source Has No Availability' 
            leftovers = pd.concat([onePlantOrdersSO, leftovers],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
            print('singleton :'+str(item)+' has no available plant, putting in leftovers for phase3')
    # Making the availability table to link it to the multiples table
    onePlantOrdersAvailability = onePlantOrders[['Date','Plant','Available']]
    # Linking the multiples table
    multiplePlantOrders = multiplePlantOrders.merge(onePlantOrdersAvailability, how = 'left', on = ['Date','Plant'])
    # Replacing the old numbers with the new numbers
    multiplePlantOrders['Available_y'].fillna(multiplePlantOrders['Available_x'], inplace=True)
    multiplePlantOrders = multiplePlantOrders.drop(columns = ['Available_x'])
    multiplePlantOrders = multiplePlantOrders.rename(columns = {'Available_y':'Available'})
    multiplePlantOrders['SO-Item'] = multiplePlantOrders['SO-Item'].astype(float).astype(np.int64)
    multiplePlantOrders['Source'] = multiplePlantOrders['Plant']
    # We need to merge oneplantordersavailability and multi table but it duplicates some rows, this fixes that...sorry
    multiplePlantOrders = multiplePlantOrders.drop_duplicates()
    
    # We are using linear - sum normalization technique to calculate the weights. 
    #There are several other normalization techniques like vector normalization,  linear (MAx- Min), Linear (Max) and Logorithmic normalization
    #but we want to put it simple and use linear (sum) normalization for AHP
    #column_sums = mtx.sum(axis=0)
    #norm_matrix = mtx / column_sums[np.newaxis,:]
    # Calculating weights as mean of the rows of our normalized matrix
    #weights = norm_matrix.mean(axis = 1)
    leftoversize = 1
    multiOrderLeftovers = pd.DataFrame([])
    # It's not neccesarily needed here, but it's here just to shut up the compiler's complaints
    rank1Orders = pd.DataFrame([])
    # Since this loops as a while, I will derive my own P numbers here
    pnum = 1
    # Creating the rankings audit table
#    moRanksTable = pd.DataFrame([])
#    added by ravali to get rid of SO-Item error
    moRanksTable = pd.DataFrame(data = [], columns = ['FiscalDate','Customer','SO-Item','Sales Order','Sales Order Item','DFU', 'Delivery Type','Delivery Quantity','Buffer','OGlobal','DGlobal','Cost','Rank','LOH','OTP','PlantType','Lane Count','Lane','Payable Total Rate','Date','Plant','Available','Source'])

#    multiplePlantOrders.to_csv(path_or_buf = "//usdcvms212/d$/Prod Projects/C Stock Booker Script/Data/multiplePlantOrdersbeforewhile.csv", index = False, float_format = "%0f", index_label = 'Date')


    ##############################
    ##     Multi-Order Loop     ##
    ##############################  
    # Begin the Phase 2 Multiorder loop
    while leftoversize > 0:
        # I know there's a better way to do this, but i don't know how. 
        # Just in case there is an order with all the possibilities being below 540, we need to isolate them
        # Isolates one SO and checks to see if all of the possible sources are unavailable, if they all are, takes the first one and puts it in leftovers
        try:
            # This is only useful for the second+ passes this while loop goes through in order to update the available columns
            # This keeps just the leftovers and updates the columns to pass again
            multiplePlantOrders = multiplePlantOrders.loc[multiplePlantOrders['SO-Item'].isin(multiOrderLeftovers['SO-Item'].unique())]
            multiplePlantOrders = multiplePlantOrders.merge(multiOrderLeftovers[['Date','Plant','SO-Item','Available']], how = 'left', on = ['Date','Plant','SO-Item'])
            try:            
                multiplePlantOrders = multiplePlantOrders.merge(rank1Orders[['Date','Plant','Available']], how = 'left', on = ['Date','Plant'])
                multiplePlantOrders = multiplePlantOrders.drop_duplicates()
                multiplePlantOrders['Available_y'].fillna(multiplePlantOrders['Available'], inplace=True)
                multiplePlantOrders['Available_y'].fillna(multiplePlantOrders['Available_x'], inplace=True)
                multiplePlantOrders = multiplePlantOrders.drop(['Available_x','Available'], axis = 1).rename(columns = {'Available_y':'Available'})
            except:
                multiplePlantOrders['Available_y'].fillna(multiplePlantOrders['Available_x'], inplace=True)
                multiplePlantOrders = multiplePlantOrders.drop(['Available_x'], axis = 1).rename(columns = {'Available_y':'Available'})
        except: filler = '' # Can't figure out a better way to do this, i want the try to work but dont need an except
        soItems = multiplePlantOrders['SO-Item'].unique()
        rank1Orders = pd.DataFrame([])
        multiplePlantOrders = multiplePlantOrders.drop_duplicates()
        # Isolates all the possibilities into just the number one possibility

        for item in soItems:
            # Goes SO by SO and figures out the ranks 
            multiplePlantOrdersSO = multiplePlantOrders.loc[multiplePlantOrders['SO-Item'] == item]
            sourcesbelowbuffer = multiplePlantOrdersSO[multiplePlantOrdersSO.Available < multiplePlantOrdersSO.Buffer].count()[0] 
            totalsources = multiplePlantOrdersSO.index.size
            if sourcesbelowbuffer != totalsources:
                multiplePlantOrdersSO = multiplePlantOrdersSO.drop(multiplePlantOrdersSO[multiplePlantOrdersSO['Available'] < multiplePlantOrdersSO['Buffer']].index)
                candidates = multiplePlantOrdersSO[['Cost','LOH','OTP','Lane Count','Available']]
                mtx = candidates.values
                # defining if our criteria should be MIn or MAX for the TOPSIS algorithm to understand our requirements while calculating ideal and anti ideal alternative
                # TOPSIS generated criteria values for ideal and anti idea alternatives - measure the distance of these points from the alternative pointst which we provide - rank the alternatives based on the distance
                criteria = [MIN,MIN,MAX,MAX,MAX]
                # naming the alternatives for our convinience
                alternatives = multiplePlantOrdersSO['Plant']
                # naming the criteria for our convinience
                crit = candidates.columns
                # putting all the above processed information in in-built object - Data
                TOPSISdata = Data(mtx, criteria,weights = weights, anames=alternatives, cnames=crit)
                #calling the in-built method TOPSIS
                dm = closeness.TOPSIS()
                #feeding out data to the TOPSIS instance to measure the distance and rank the alternatives
                rank = dm.decide(TOPSISdata)
                multiplePlantOrdersSO['Rank'] = rank.rank_
                if pnum == 1:
                    moRanksTable = pd.concat([moRanksTable, multiplePlantOrdersSO], sort = True) 
                    # Adding the multiple lanes and their costs
                multiplePlantOrdersSOrank1 = multiplePlantOrdersSO.loc[multiplePlantOrdersSO['Rank'] == 1]
                rank1Orders = pd.concat([multiplePlantOrdersSOrank1, rank1Orders],axis = 0, join = 'outer',  ignore_index = False, sort = True)          
            else:
                mopleftover = multiplePlantOrdersSO.iloc[0:1,:]
                mopleftover['Reason'] = 'No Available Lanes'
                # This takes the first entry of the SO table because the other ranks dont matter if they are not available
                leftovers = pd.concat([mopleftover, leftovers],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
                print('Item '+str(item)+' has no available lane, moving it to leftovers')
        # Sometimes the final order in the list will need to go to leftovers, therefore not allow the loop to complete
        if rank1Orders.index.size != 0:        
            rank1Orders['SO-Item'] = rank1Orders['SO-Item'].astype(float).astype(np.int64)
            rank1Orders = rank1Orders.sort_values(by = ['SO-Item'])
            rank1Orders['Source'] = rank1Orders['Plant']
            # Updating each of the singletons and their availability for the multiples next
            # Setting up a catch where if the updated value is less than 540 it doesnt change it and moves it to phase 3
            soItems = rank1Orders['SO-Item'].unique().tolist()
            multiOrderLeftovers = pd.DataFrame([])
            # Loops through to book that rank 1
            for item in soItems:
                rank1OrdersSO = rank1Orders.loc[rank1Orders['SO-Item'] == item]
                rank1OrdersSO.loc[:,'Delivery Quantity'] = rank1OrdersSO.loc[:,'Delivery Quantity'].astype(np.int64)
                if rank1OrdersSO['Available'].values[0] >= rank1OrdersSO['Buffer'].values[0] :
                    readyorders = pd.concat([rank1OrdersSO[['FiscalDate','Customer','DFU','SO-Item','Sales Order','Sales Order Item','Delivery Type','Source','Delivery Quantity']], readyorders],axis = 0, join = 'outer',  ignore_index = False, sort = True)                          
                    rank1Orders.loc[(rank1Orders['Plant'] == rank1OrdersSO['Plant'].values[0]) & (rank1Orders['FiscalDate'] == rank1OrdersSO['FiscalDate'].values[0]),'Available'] = rank1OrdersSO.at[rank1OrdersSO.index.values[0],'Available'] - rank1OrdersSO.at[rank1OrdersSO.index.values[0],'Delivery Quantity']
                    allsources.loc[(allsources['Source'] == rank1OrdersSO['Plant'].values[0]) & (allsources['FiscalDate'] == rank1OrdersSO['FiscalDate'].values[0]),'Available'] = rank1OrdersSO.at[rank1OrdersSO.index.values[0],'Available'] - rank1OrdersSO.at[rank1OrdersSO.index.values[0],'Delivery Quantity']        
                    # If the source value does not exist in the allsources, it adds it here
                    if len(allsources.loc[(allsources['Source'] == rank1Orders['Source'].values[0]) & (allsources['FiscalDate'] == rank1Orders['FiscalDate'].values[0])]) == 0  :
                        allsources = pd.concat([rank1OrdersSO[['FiscalDate','Source','Available']], allsources], sort = True)            
                    print('Item '+str(item)+' Has been booked as the rank '+str(pnum))
                    onepoaudittable = pd.DataFrame(data = [[rank1OrdersSO['Date'].values[0],rank1OrdersSO['Customer'].values[0],item,rank1OrdersSO['Source'].values[0], '2','Rank '+str(pnum)+' Is Best Available']], columns = ['Date','GLID','Order','Plant','Phase','Reason'])
                    auditTable = pd.concat([auditTable,onepoaudittable],join = 'outer',  ignore_index = False, sort = True)                  
                else: 
                    multiOrderLeftovers = pd.concat([rank1OrdersSO, multiOrderLeftovers],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
            leftoversize = multiOrderLeftovers.index.size
        # This if statement is here to make sure the loop completes by adjusting the leftover size manually 
        else: 
            leftoversize = leftoversize-1
        #pnum+1 means it's adding the rank value to signify it's choosing the next best rank
        pnum = pnum+1    
    
    ######################################################
    ##                   Wrapping Up                    ##
    ######################################################     
    # Takes the output of the audit table and combines all the data the code used to make up its decision. It takes
    # a lot of steps because there are quite a few tables it has to read from. It's important we have this 
    # because it will allow everyone to have visibility into the platform in case anyone questions it. 
    # One of the original claims to the superiority of this product is that it's an open book with full insights into the logic,
    # therefore this is neccesary to follow through with this claim. 
    ##############################
    ##     Audit Reporting      ##
    ##############################  
    # We are combining some tables together to show why the logic chose what it chose
    auditTable = auditTable[['Date','GLID','Order','Phase','Plant','Reason']]
    auditTable.columns = ['Date','Customer','SO-Item','Phase','Chosen Plant','Reason']
    auditTable2 = auditTable.merge(moRanksTable,how = 'left', left_on = ['SO-Item','Chosen Plant'], right_on = ['SO-Item','Plant'])
    auditTable3 = auditTable2.merge(onePlantOrders,how = 'left', left_on = ['SO-Item','Chosen Plant'], right_on = ['SO-Item','Plant'])
    auditTable3 = auditTable3.drop(['Date','Customer'], axis = 'columns')
    
    # Since we're merging two tables, we gotta get them to look right
    # This removes the X and the Y suffix and merges them together into one column
    # Yes it's a very bad way of doing it, but i am lazy and it's not going to hurt anyone >:)
    cols = ['Available','Buffer','Cost','Customer','DFU','DGlobal','Date','Delivery Quantity','Delivery Type','FiscalDate','LOH','Lane','Lane Count','OGlobal','OTP','Plant','PlantType','Sales Order','Sales Order Item']
    for col in cols:
        auditTable3[col+'_x'].fillna(auditTable3[col+'_y'], inplace=True)
        auditTable3 = auditTable3.drop(columns = [col+'_y'])
        auditTable3 = auditTable3.rename({col+'_x':col}, axis = 'columns')
    # Removing the unneccessary columns
    auditTable3 = auditTable3[['Date','SO-Item','Customer','Chosen Plant','Reason','Available','Buffer','Cost','DFU','Delivery Quantity','Delivery Type','LOH','Lane Count','OTP','Rank','PlantType','Phase']]
    
    
    # Finding just the rows that come from phase 1
    # Isolating just the Pase 1 to get what we need out of them
    auditTable3t = auditTable3.loc[auditTable3['Phase']=='1']
    # Gathering the P1/P2/P3 data 
    auditTable3t2 = auditTable3t.merge(oo2,how = 'left', left_on = ['SO-Item'], right_on = ['SO-Item'])
    # Creating the husk 
    auditTable4 = pd.DataFrame([])
    # Since we've got 3Ps
    for p in range(1,4):
        print(p)
        auditTable3tp = auditTable3t2.loc[auditTable3t2['Reason'].str.contains('P'+str(p))][['Date','SO-Item','Customer_x','P'+str(p),'P'+str(p)+' Available','P'+str(p)+' Buffer','P'+str(p)+' Lane Cost','DFU_y','Delivery Quantity_y','Delivery Type_y','P'+str(p)+' Plant Type','Reason']]
        auditTable3tp = auditTable3tp.rename({'Date':'Date','SO-Item':'SO-Item','Customer_x':'Customer','P'+str(p):'Chosen Plant','P'+str(p)+' Available':'Available','P'+str(p)+' Buffer':'Buffer','P'+str(p)+' Lane Cost':'Cost','DFU_y':'DFU','Delivery Quantity_y':'Delivery Quantity','Delivery Type_y':'Delivery Type','P'+str(p)+' Plant Type':'PlantType','Reason':'Reason'}, axis = 'columns')
        auditTable4 = pd.concat([auditTable4,auditTable3tp],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
    auditTable4['Phase'] = 1
    auditTable5 = auditTable3.loc[auditTable3['Phase']!='1']
    auditTable4 = pd.concat([auditTable4,auditTable5],axis = 0, join = 'outer',  ignore_index = False, sort = True)  
    auditTable4['RunDate'] = todaytime
    
    ##############################
    ##    Exporting The Data    ##
    ##############################  
    #multiplePlantOrders = multiplePlantOrders.merge(rank1Orders, how = 'left', on = ['Date','Plant'])
    phase3 = leftovers
    phase3['RunDate'] = datetime.today()
    phase3['SO-Item'] = phase3['SO-Item'].astype(float).astype(np.int64)
    # Something is wrong with the phase 2 code that some reason does not want to work correctly and duplicates the last batch like 10 times 
    # This will remove those dupes
    phase3 = phase3.drop_duplicates()

    phase3.to_csv(path_or_buf = "redacted", index = False, float_format = "%0f")
    readyorders.to_csv(path_or_buf = "redacted", index = False, float_format = "%0f")
    # Storing the plant availabilies to csv that should be reality after the end of the script
    allsources.to_csv(path_or_buf = "redacted", index = False, float_format = "%0f")
    # Qlikview is super fickle, therefore we gotta have everything explicitly marked as a string
    auditTable4['SO-Item'] = auditTable4['SO-Item'].astype(str)
    phase3['SO-Item'] = phase3['SO-Item'].astype(str)
    moRanksTable['SO-Item'] = moRanksTable['SO-Item'].astype(str)
    moRanksTable['RunDate'] = todaytime
    moRanksTable['Ratios'] = 'Cost: '+str(cost)+', LOH:'+str(loh)+', OTP:'+str(otp)+', Lane Count:'+str(lanecount)+', Availability:'+str(availability)
    
    #######################################
    ##         Archiving The Data        ##
    ####################################### 
    # Outputting the data into a single excel file
    writer = pd.ExcelWriter("redacted "+todayapifmt+".xlsx", engine='xlsxwriter')
    phase3.to_excel(writer,sheet_name='For LCs to Book', index = False)
    auditTable4.to_excel(writer,sheet_name='What BOOT will book', index = False)
    moRanksTable.to_excel(writer,sheet_name='Post PSO Rankings', index = False)
    writer.save()
    
    writer = pd.ExcelWriter("redacted", engine='xlsxwriter')
    phase3.to_excel(writer,sheet_name='For LCs to Book', index = False)
    auditTable4.to_excel(writer,sheet_name='What BOOT will book', index = False)
    moRanksTable.to_excel(writer,sheet_name='Post PSO Rankings', index = False)
    writer.save()
    
    #######################################
    #        Making The Email File        #
    #######################################    
    # In order for us to make the email file, we've gotta get older file runs, otherwise it will only pull new orders for each run
    
    # Including the SO-Item and turning it into a number so it can all join together    
    ooemail['SO-Item'] = ooemail['Sales Order'].astype(str) + ooemail['Sales Order Item'].astype(str)  
    ooemail['SO-Item'] = ooemail['SO-Item'].astype(float).astype(np.int64)

    #ooemail = ooemail.merge(auditTable4,how = 'left', on = ['SO-Item'])
    # Combining all the previously run files
    recentfile = pd.read_excel(glob.glob('redacted*')[-1], sheet_name = 'What BOOT will book')
    recentfile2 = pd.read_excel(glob.glob('redacted*')[-2], sheet_name = 'What BOOT will book')
    recentfile3 = pd.read_excel(glob.glob('redacted*')[-3], sheet_name = 'What BOOT will book')
    recentfile4 = pd.read_excel(glob.glob('redacted*')[-4], sheet_name = 'What BOOT will book')
    recentfile5 = pd.read_excel(glob.glob('redacted*')[-5], sheet_name = 'What BOOT will book')
    recentfile6 = pd.read_excel(glob.glob('redacted*')[-6], sheet_name = 'What BOOT will book')

    recentfiles = pd.concat([recentfile,recentfile2,recentfile3,recentfile4,recentfile5,recentfile6,auditTable4])
    # Cleaning up the concatenation
    recentfiles = recentfiles[['Chosen Plant','Cost','LOH','Lane Count','OTP','Phase','Rank','Reason','SO-Item','RunDate']]
    recentfiles['Phase'] = recentfiles['Phase'].astype(int)
    recentfiles['SO-Item'] = recentfiles['SO-Item'].astype(float).astype(np.int64)
       
    # # Combining the two files together to join to the open orders
    # recentfiles = pd.concat([recentfiles,phase3recentfiles])

    # Joining the files to the open orders to fill everything up
    ooemail = ooemail.merge(recentfiles,how = 'left', on = ['SO-Item'])
    # No need to keep dupes, dropping them and keeping just the most recent one.
    ooemail = ooemail.sort_values('RunDate').drop_duplicates('SO-Item',keep='last')
    # Cutting the crap
    ooemail = ooemail[['FiscalDate','Region','LC','Delivery Type','Sales Order','Sales Order Item','SO-Item','Shipment Condition','Material','Batch','Equipment','Drop Swap Type','Delivery Quantity','Chosen Plant','Cost','LOH','Lane Count','OTP','Phase','Rank','Reason','P1','P2','P3','Closest_Plant','Customer','Customer Name','Customer City','Customer State','Customer Zip','Delivery Window','P1 Restriction','P2 Restriction','P3 Restriction','Restriction Type','Restriction Notes']]
    
    # Renaming phases to mapping and optimizer for people to understand better
    ooemail.loc[ooemail['Phase'] == 1, 'Phase'] = 'Mapping'
    ooemail.loc[ooemail['Phase'] == 2, 'Phase'] = 'Optimizer'
  
    ooemail['Lane'] = ooemail['Customer'].astype(str).str[:10]+ooemail['Chosen Plant'].astype(str)
    
 #   ooemail2 = ooemail.join(plantpercent[['Lane','Plant Percent']], on = 'Lane', how = 'left')
    ooemail = ooemail.merge(plantpercent[['Lane','Plant Percent']], how = 'left', on = 'Lane')
    ooemail.drop_duplicates(keep = 'first', inplace = True) 

    # Exporting it to go to camp and the archive
    ooemail.to_excel("redacted", index = False, float_format = "%0f")
    ooemail.to_excel("redacted"+todayapifmt+".xlsx", index = False, float_format = "%0f")


    #######################################
    #         Managing The Archive        #
    #######################################
    # Storing the archive to the backup folder

    todayapifmt = datetime.today().strftime('%Y-%m-%d, Hour %H')
    readyorders.to_csv(path_or_buf = "redacted"+todayapifmt+".csv", index = False, float_format = "%0f", index_label = 'Date')
        
    # Just in case something goes belly-up, we are making an archival process here. 
    # It only keeps the newest 1,500. 2 years, twice a day = 1,460. Rounding up for simplicity.
    list_of_files = glob.glob('redacted*')
    if len(list_of_files) >= 1500:
        oldest_file = min(glob.glob('redacted*'), key=os.path.getctime)
        oldest_file2 = min(glob.glob('redacted'), key=os.path.getctime)
        oldest_file3 = min(glob.glob('redacted*'), key=os.path.getctime)
        os.remove(oldest_file)
        os.remove(oldest_file2)
        os.remove(oldest_file3)
            
except Exception as e:
        
    with open('redacted, 'w') as f:
        f.write(str(e))
        f.write(traceback.format_exc())
        
      
    server = smtplib.SMTP('mailhost.chep.com',25)
    server.login(login.Username[0],login.Password[0])
    MSG = traceback.format_exc()
    FROM = "redacted"
    # No need to change below 
    server.sendmail(FROM,TO,MSG)          
