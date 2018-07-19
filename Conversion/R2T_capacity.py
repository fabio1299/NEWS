"""
R2T_capacity.py
"""

from math import inf
from . import R2T_Globals
from R2T_Globals import *
from R2T_readinputfiles import findNext, nextCell
global ActivePCA
global EndYearInDatabase

"""""
globals
"""""
MaxPlantID = 0

PCA_to_state ={1:53,2:53,3:53,4:53,5:41,6:41,7:41,8:6,9:6,10:6,11:6,12:32,13:32,
               14:16,15:16,16:16,17:30,18:30,19:30,20:30,21:56,22:56,23:56,24:56,
                25:49,26:49,27:4,28:4,29:4,30:4,31:35,32:46,33:8,34:8,35:30,36:38,
                37:38,38:46,39:31,40:31,41:31,42:27,43:27,44:27,45:19,46:55,47:35,
                48:48,49:40,50:40,51:40,52:20,53:20,54:29,55:29,56:5,57:48,58:22,
                59:48,60:48,61:48,62:48,63:48,64:48,65:48,66:48,67:48,68:27,69:19,
                70:19,71:29,72:29,73:29,74:26,75:55,76:55,77:55,78:55,79:55,80:17,
                81:17,82:17,83:17,84:29,85:5,86:22,87:28,88:28,89:1,90:1,91:12,92:47,
                93:21,94:13,95:45,96:45,97:37,98:37,99:51,100:51,101:12,102:12,103:26,
                104:26,105:18,106:18,107:18,108:21,109:21,110:21,111:39,112:39,113:39,
                114:39,115:42,116:54,117:54,118:51,119:42,120:42,121:24,122:42,123:24,
                124:51,125:10,126:34,127:36,128:36,129:50,130:33,131:25,132:9,133:44,134:23}


"""""""""
plant ops
"""""""""

def retire(r,retired,capoutput,effoutput,cells,y):
    # Retires all the power plants whose index are in r
    # Most of the times only one index is passed in r
    year="NP_"+str(y)
    for i in r:
        # We first get the required values to set the retired record
        # Get the CEllID of the power plant location
        # And the cooling techology
        CellID=int(capoutput.loc[i,'CellID'])
        Cooling=int(capoutput.loc[i,'Cooling'])
        #if cells.loc[cells['CellID'] == CellID , 'CoordID'].values[0] == '-89.025_30.425':
        #    print('Here')
        # We set the capacity and efficiency of the power plant to 0. This also takes care of the water
        # availablity of the cell
        newCapacity(0, i, capoutput, effoutput, cells, y)
        # There are a number of conditions that need to be met in order to fully retire a power plant:
        #   1) if the current analysis year is equal or past the maximum year in the power plant database
        #   2) if the power plant is set to be retired from the value set in the RetireYear field
        #
        # And we really don't want to fully retire any power plant that has capacity installed in between
        # the current year and the EndYearInDatabase value
        # If the conditions above are not met then we skip updating the rest of the power plant's entry
        if (((EndYearInDatabase <= y < R2T_Globals.YEAR_E) & (capoutput.loc[i,'YearRetired'] == 9999)) |
                ((y < EndYearInDatabase) & (0 < capoutput.loc[i, 'RetireYear'] < 9999))):
            # Otherwise we update the various entries
            # First we set the retirement year for the power plant (this tracks whether a power
            # plant is already retired (RetireYear <= Current Year) or not yet)
            capoutput.loc[i,'YearRetired']=y
            # We now decrease the occupancy of the cell by one and store the value in the network
            # table (cells)
            Occupancy=cells.loc[CellID,'Occupied'] - 1
            if Occupancy < 0: # <-- THIS SHOULD NOT HAPPEN, BUT...
                Occupancy = 0
            cells.loc[CellID,'Occupied'] = Occupancy
            # if we are retiring a power plant with cooling technology 1 or 4 then we need to
            # check/update the lock status of the cell downstream
            if Cooling in [1,4]:
                # check if there is another non-retired power plants with cooling 1 or 4 in the same cell
                KeepLock = len(capoutput.loc[(capoutput['CellID'] == CellID) &
                                             (capoutput['Cooling'].isin([1,4]) &
                                             (capoutput['YearRetired'] == 9999))].index) > 0
                if not KeepLock:
                    cells.loc[CellID, 'Locked'] = ''
            # If the plant is not LakeOcean and the network cell is now empty, we fill the
            # corresponding record in the retired table so that it can be recycled if needed
            if (Occupancy == 0) and (not capoutput.loc[i,'LakeOcean']) and (cells.loc[CellID,'TotAvgSumDisch'] >= 10):
                retired.loc[i,'Index']=i
                retired.loc[i, 'CellID'] =CellID
                retired.loc[i, 'PCA'] = cells.loc[CellID, 'PCA']
                retired.loc[i, 'YearRetired'] =y
                retired.loc[i, 'Occupied'] = 0
                retired.loc[i, 'AvailDisch'] = cells.loc[CellID, 'TotAvgSumDisch'] * .3
        #capoutput.loc[i, 'Changed'] = 1

def recycle(id, dCap, capoutput, effoutput, cells, retired, retirementDB, y, fuel, cooling):
    """
    recycle an old plant location
    """
    CellID=retired.loc[id,'CellID']
    create(CellID, dCap, capoutput, effoutput, cells, retirementDB, y, fuel, cooling)
    # We zero the retired record
    retired.loc[id, 'Index'] = 0
    retired.loc[id, 'CellID'] = 0
    retired.loc[id, 'PCA'] = 0
    retired.loc[id, 'YearRetired'] = 0
    retired.loc[id, 'Occupied'] = 0
    retired.loc[id, 'AvailDisch'] = 0


def create(c, capInst, capoutput, effoutput, cells, retirementDB, y, fuel, cooling, AltWater=0):
    """
    create a new plant at PCA cell c
    """
    # Crete the Plant Id for the new plant
    #if cells.loc[c,'CoordID'] == '-89.025_30.425':
    #    print('Here')
    Id,code=R2T_Globals.getNextIDs(capoutput)
    PCA=cells.loc[c,'PCA']
    # Add the basic paramaters for the new plant in the capoutput table
    capoutput.loc[Id, 'Index'] =Id
    capoutput.loc[Id, 'PlantCode'] =code
    capoutput.loc[Id, 'PCA']=PCA
    capoutput.loc[Id, 'Fuel']=fuel
    capoutput.loc[Id, 'Cooling']=cooling
    capoutput.loc[Id, 'State']=PCA_to_state[PCA]
    capoutput.loc[Id, 'Longitude']=cells.loc[c,'Longitude']
    capoutput.loc[Id, 'Latitude']=cells.loc[c,'Latitude']
    capoutput.loc[Id, 'CellID']=c
    capoutput.loc[Id, 'AltWater']=AltWater
    capoutput.loc[Id, 'RetireYear']=9999
    capoutput.loc[Id, 'YearRetired']=9999
    capoutput.loc[Id, 'Changed'] = 1

    # And in the effoutput table
    effoutput.loc[Id, 'Index'] =Id
    effoutput.loc[Id, 'PlantCode'] =code
    effoutput.loc[Id, 'PCA']=PCA
    effoutput.loc[Id, 'Fuel']=fuel
    effoutput.loc[Id, 'Cooling']=cooling
    effoutput.loc[Id, 'Longitude']=cells.loc[c,'Longitude']
    effoutput.loc[Id, 'Latitude']=cells.loc[c,'Latitude']

    # Now we can set the capacity for the newly created power plant
    newCapacity(capInst,Id,capoutput, effoutput,cells,y,AltWater)
    # And update the cell's occupancy
    cells.loc[c,'Occupied'] = cells.loc[c,'Occupied'] + 1
    if cooling in [1,4]:
        cells.loc[c,'Locked']=findNext(cells.loc[c,'ToCell'],cells.loc[c,'Longitude'],cells.loc[c,'Latitude'],R2T_Globals.Resolution)
    if cooling != 8: # <-- WHEN COOLING IS 8 WE JUST ADD AND REMOVE AS REQUIRED FROM A SINGLE "CATCH ALL" PLANT
                     # SO WE DON'T NEED AN ENTRY IN THE retirementYears TABLE
        # Now we add an entry in the Retirements Table with high retirement year
        # We get the next index value
        RetId=min(retirementDB.loc[retirementDB['Index']==0].index)
        # And create the new record
        retirementDB.loc[RetId,'Index']=RetId
        retirementDB.loc[RetId,'PlantCode']=code
        retirementDB.loc[RetId, 'Year'] = 9999
        retirementDB.loc[RetId, 'Capacity'] = capInst
        retirementDB.loc[RetId, 'PCA'] = PCA
        retirementDB.loc[RetId, 'Fuel'] = fuel
        retirementDB.loc[RetId, 'Cooling'] = cooling
        retirementDB.loc[RetId, 'Online'] = y
    return Id

def newCapacity(capacity,index,capoutput,effoutput,cells,y,AltWater=0):
    # Sets new capacity for a given power plant and updates
    # the corresponding water requirements and cell discharge

    year="NP_"+str(y)
    year_eff="Eff_"+str(y)

    cooling = capoutput.loc[index, 'Cooling']

    if capoutput.loc[index, 'YearRetired'] == 9999: # <-- If plant has NOT been retired then set the new discharge values
        # Get the Cell index of power plant and the cell occupancy
        netIndex=int(capoutput.loc[index,'CellID'])
        occupancy=cells.loc[netIndex,'Occupied']
        # Get current values for power plant
        cur_withd = capoutput.loc[index, 'Withdrawal']
        # Get current values for network cell
        cur_disch = cells.loc[netIndex, 'AvailDisch']
        cell_totaldisch = cells.loc[netIndex, 'TotAvgSumDisch']
        # Get key values for power plant
        fuel = capoutput.loc[index, 'Fuel']
        if AltWater==1:
            new_withd=0
            new_disch = cell_totaldisch * .3
        else:
            if capacity == 0:
                new_withd = 0
                if occupancy==1: # <-- LAST POWER PLANT IN THE CELL
                                 # NOTE: The occupancy IS UPDATED in the calling function AFTER this functon returns
                    new_disch = cell_totaldisch * .3
                else:
                    new_disch = cur_disch + cur_withd
            else:
                # Calculate new values for power plant
                new_withd =R2T_Globals.discharge(fuel,cooling,capacity)
                new_disch=cur_disch + cur_withd - new_withd
                # Check new values consistency
                if new_disch < 0:
                    new_disch = 0
                if new_disch > cell_totaldisch:
                    new_disch = cell_totaldisch * .3
        # Set the new values
        capoutput.loc[index, 'Withdrawal'] = new_withd
        # Available discharge needs to be broadcasted to all power plants in the cell
        capoutput.loc[capoutput['CellID']==netIndex, 'AvailDisch'] = new_disch
        cells.loc[netIndex, 'AvailDisch'] = new_disch
    curr_cap=capoutput.loc[index,year]
    curr_eff=effoutput.loc[index,year_eff]
    effoutput.loc[index,year_eff]=newEfficiency(curr_cap,capacity,curr_eff,cooling)
    capoutput.loc[index,year]=capacity
    #capoutput.loc[index, 'Changed'] = 1

def newEfficiency(curr_cap,new_cap,curr_eff,cooling):
    # if the capacity is 0 then we set the efficiency at 0 as well
    if (new_cap == 0): # or (cooling == 8):
        return 0
    # otherwise we take the waighted average of the current efficiency and the
    # efficiency factor from the lookup table. the weights are the current capacity installed
    # vs the new capacity installed
    # print(cooling)
    deltaCapacity=new_cap - curr_cap
    eff_factor=R2T_Globals.cool_eff_lookup.loc[R2T_Globals.cool_eff_lookup['Cooling'] == cooling,'Efficiency'].values[0]
    newEff=((deltaCapacity * eff_factor) + (curr_cap * curr_eff)) / new_cap
    # The efficiency should not go below 20, so if it does we reset it to the value in
    # the lookup table
    if newEff < 20:
        newEff = eff_factor
    return newEff

"""""
rules
"""""
def apply_rules(key, y, delta, retired, capoutput, effoutput, cells, retirementDB):

    """
    main rules function (retire / add [existing -> recycle -> create])

    Function arguments:
        key = tuple (PCA, fuel, cooling)
        y = current year
        delta = Capacity change for the specified PCA, fuel and cooling for year y
        retired = Table of retired power plants (used for reclying locations)
        capoutput = output table
        cells = table of network cells with discharge and PCA info (from HydroCells.xlsx file)
        retirementDB=Table of capacity due for retirement per power plant and year
    """
    if not delta: return  # If delta is 0 then do nothing

#    if key==(9, 1, 1) and y==2010:
#        print('Here')

    PCA = key[0]
    if PCA not in ActivePCA: return
    Fuel = key[1]
    Cooling = key[2]
    MaxPlantSize,MinGenSize,gallMWh,wpMW,effCoeff=[R2T_Globals.tech_specs[Fuel,Cooling][i] for i in range(len(R2T_Globals.tech_specs[Fuel,Cooling]))]
    Year='NP_'+str(y)

    # NEED TO HANDLE COOLING TYPE 8 FOR BOTH RETIREMENT AND ADDITION AND NO EFFECT ON EFFICIENCY
    if Cooling == 8:
        plants = capoutput.loc[((capoutput['PCA'] == PCA) &
                                (capoutput['Fuel'] == Fuel) &
                                (capoutput['Cooling'] == Cooling))]
        if not plants.empty:
            for id,plant in plants.iterrows():
                curCap=plants[Year].values[0]
                newCap=curCap+delta
                if newCap < 0:
                    newCap=0
                capoutput.loc[capoutput['Index']==id,Year]=newCap
                break # <--We just need the first one (there should only be one anyway)
        else:
#            cellCandidate = cells.loc[((cells['PCA'] == PCA) &
#                                       (cells['Occupied'] == 0) &
#                                       (cells['Coastal'] == 0) &
#                                       (cells['AvailDisch'] > 0))].sort_values('AvailDisch')
#            for c,cell in cellCandidate.iterrows():
            # We create a new plant at the centroid location if delta is positive
            if delta > 0:
                c = R2T_Globals.PCACentroids.loc[R2T_Globals.PCACentroids['PCA'] == PCA , 'CellID'].values[0]
                create(c, delta, capoutput, effoutput, cells, retirementDB, y, Fuel, 8)
#                break # <--We just need the first one (lowest discharge in PCA)
        return

    if delta < 0: # retire
        # We first check which plants have the right characteristics to match this
        # retirement need (same PCA, fuel, cooling and capacity more than 0 and
        # not already retired [e.g. retire year bigger than current year])
        plants=capoutput.loc[((capoutput['PCA'] == PCA) &
                               (capoutput['Fuel'] == Fuel) &
                               (capoutput['Cooling'] == Cooling) &
                               (capoutput[Year] > 0))].sort_values('RetireYear') # &
        #                       (capoutput['RetireYear'] > y))]
        # If the set returned is empty, then we cannot match the required capacity needed for retirment
        if not len(plants): # If there are no plants log the error and do nothing
            print('no plants to retire from. Key {}, year {}, capacity {}'.format(key,y,-delta))
            log('retire', 'no plants to retire from', key, y, -delta)
            return
        # otherwise we check if the total capacity is
        TotCap=0
        for index,plant in plants.iterrows():
            TotCap=TotCap + plant["NP_"+str(y)]
        if TotCap <= -delta: # We have to retire everything for this PCA, Fuel, Cooling
            retire(plants.index, retired, capoutput, effoutput, cells, y)
            delta=delta+TotCap
            if (-delta) > 0.0000000001:
                log('retire','not enough',key,y,(-delta - TotCap))
                print('Retiring everything, but not enough to retire from. Key {}, year {}, capacity {}'.format(key, y, (-delta - TotCap)))
            return
        amt=-delta
        retCandidates=retirementDB.loc[((retirementDB['PCA'] == PCA) &
                                        (retirementDB['Fuel'] == Cooling) &
                                        (retirementDB['Cooling'] == Cooling) &
                                        (retirementDB['Capacity'] > 0))].sort_values(['Year'])
        # We cycle through the retirement candidates (sorted by retirement year) and
        # retire their capacity until we match the required amount to retire
        for i,retiree in retCandidates.iterrows():
            plant=retiree['PlantCode']
            if plant in plants['PlantCode'].values:
                PlantIx=plants.loc[plants['PlantCode']==plant,'Index'].values[0]
                # We now get the capacity in the retiments database and the one in the power plants database
                capRet=retiree['Capacity']
                capPlant=capoutput.loc[PlantIx,Year]
                # we now retire the smaller of the two
                if capPlant < capRet:
                    amt = amt - capPlant
                    # Now we check the residual amount...
                    if amt >= 0:
                        # If it is more or equal to 0, then the we retire the power plant
                        retire(plants.loc[plants['Index']==PlantIx].index, retired, capoutput, effoutput, cells, y)
                        # and set the capacity in the retirement database record to 0
                        retirementDB.loc[i, 'Capacity'] = 0
                    else:
                        # If it is less than 0, then the we some capacity left (-amt) in the plant
                        # and in the retirement database
                        newCapacity(-amt, PlantIx, capoutput,effoutput, cells, y)
                        # WE SET THE CAPACITY IN THE RETIREMENT DATABASE TO THE SAME AS THE POWER PLANTS DATABASE
                        # WE MAY NEED TO REVISE THIS *********************************************************
                        retirementDB.loc[i, 'Capacity'] = -amt
                        # And set the amt to 0
                        amt=0
                else:
                    amt = amt - capRet
                    # Now we check the residual amount...
                    if amt >= 0:
                        # If it is more or equal to 0, then the we update the power plant capacity to
                        # the residual (e.g. capPlant-capRet)
                        if capPlant != capRet:
                            newCapacity((capPlant - capRet), PlantIx, capoutput, effoutput, cells, y)
                        else:
                            # This iwll not happen often, but if it does we need to
                            retire(plants.loc[plants['Index']==PlantIx].index, retired, capoutput, effoutput, cells, y)
                        # and set the capacity in the retirement database record to 0
                        retirementDB.loc[i, 'Capacity'] = 0
                    else:
                        # If it is less than 0, then the we some residual capacity left (-amt) in the in the
                        # retirement database, and we adjust the power plants database accordingly
                        newCapacity((capPlant - capRet - amt), PlantIx, capoutput, effoutput, cells, y)
                        retirementDB.loc[i, 'Capacity'] = -amt
                        amt=0
            if amt==0:
                break
        if amt > 0:
            # If we still have some amount, we re-query the retirement database for plants that meet the
            # requirements and check if they can provide additional retirement capacity
            plants = capoutput.loc[((capoutput['PCA'] == PCA) &
                                    (capoutput['Fuel'] == Fuel) &
                                    (capoutput['Cooling'] == Cooling) &
                                    (capoutput[Year] > 0))].sort_values('RetireYear')  # &
            if not plants.empty:
                for ix, plant in plants.iterrows():
                    capPlant = plant[Year]
                    amt = amt - capPlant
                    if amt >= 0:
                        retire(plants.loc[plants['Index']==ix].index, retired, capoutput, effoutput, cells, y)
                    else:
                        newCapacity(-amt, ix, capoutput, effoutput, cells, y)
                        amt = 0
                    if amt==0:
                        break
        # We have exhausted all the sources of capacity to retire, if we still have some amount
        # to retire we log the error
        if amt>.000001:
            print('Leftover not retired. Key {}, year {}, capacity {}'.format(key,y,amt))
            log('retire','insuf',key,y,amt)
    else: # add; as much as poss to existing power plants
        # We first check which plants have the right characteristics to match this
        # addition need (same PCA, fuel, cooling and are not already retired
        # [e.g. retire year bigger than current year]) and that are not LakeOcean cells (no additions to
        # LakeOcean power plants...
        #
        # We sort the list descending according to the AltWater flag and the Available Water next
        #
        # AltWater plants are those that do not relay on surface discharge for cooling and can have as
        # much capacity as the fuel and cooling allows for
        #
        # If we rule out AltWater plants, we start adding capacity to the Power Plants that are in
        # locations with the highest discharge
        plants=capoutput.loc[((capoutput['PCA'] == PCA) &
                               (capoutput['Fuel'] == Fuel) &
                               (capoutput['Cooling'] == Cooling) &
                               (capoutput['LakeOcean'] != 1) &
                               (capoutput['AvailDisch'] > 0) &
                               (capoutput['YearRetired'] > y))].sort_values(['AltWater','AvailDisch'],ascending=False)
        if not plants.empty:
            for ix, plant in plants.iterrows():
                capPlant = plant[Year]
                if plant['AltWater'] == 1:
                    avail4Water = inf
                    avail4Add = MaxPlantSize
                else:
                    avail4Add = MaxPlantSize - capPlant
                    if wpMW == 0: # <-- NO WATER REQUIREMENTS JUST ADD AS MUCH AS POSSIBLE
                        avail4Water = inf
                    else:
                        # avail4Water=( plant['AvailDisch'] * .3 ) / wpMW
                        avail4Water = plant['AvailDisch'] / wpMW
                if avail4Water > 0 and avail4Add > 0:
                    dCap=min(delta,avail4Add,avail4Water) # <-- POSSIBLY ADD MINIMUM GENERATOR SIZE CODE BEFORE THIS LINE
                    newCapacity((capPlant + dCap), ix, capoutput, effoutput, cells, y,plant['AltWater'])
                    delta=abs(delta-dCap)
                if delta==0:
                    break
        # If we still have some additional delta capacity, then we try to recycle retired PowerPlants locations
        if delta > 0: # recycle; key if 3 or 6, key[0] else (i.e. just PCA)
            # We select all the retired locations in the PCA
            recyCanditate=retired.loc[retired['PCA']==PCA].sort_values('AvailDisch')

            if not recyCanditate.empty:

                if Cooling in [1, 4]:
                    # Need to make sure that no candidate cell is in the Locked list
                    Locked = cells.loc[cells['CellID'].isin(recyCanditate['CellID']), 'CoordID'].isin(
                                                                            cells.loc[cells['Locked'] != ''])
                    Drops = Locked.loc[Locked].index.tolist()
                    if len(Drops) > 0:
                        DropsIndex = recyCanditate.loc[recyCanditate['CellID'].isin(Drops)].index
                        recyCanditate.drop(DropsIndex, inplace=True)

                for ix, recyclee in recyCanditate.iterrows():
                    if wpMW == 0:
                        avail4Water = inf
                    else:
                        # avail4Water = (recyclee['AvailDisch'] * .3) / wpMW
                        avail4Water = recyclee['AvailDisch'] / wpMW
                    aCap = min(delta, MaxPlantSize, avail4Water)
                    recycle(ix, aCap, capoutput, effoutput, cells, retired, retirementDB, y, Fuel, Cooling)
                    delta=abs(delta-aCap)
                    if delta==0:
                        break
        # If we still have some additional delta capacity, then we add a new PowerPlant
        if delta > 0: # create
            # We get all the candidate cells in the network, sorted descending by AvailDisch
            # We first check the non coastal cells and if we still don't have enough
            # we check the coastal ones
            for Coastal in [0, 1]:
                cellCandidate=cells.loc[((cells['PCA'] == PCA) &
                                         (cells['Occupied'] == 0) &
                                         (cells['Coastal'] == Coastal) &
                                         (~cells['CoordID'].isin(cells.loc[cells['Locked'] != '','Locked'])) &
                                         (cells['AvailDisch'] > 0))].sort_values('AvailDisch',ascending=False)
                if Cooling in [1,4]:
                    # from the list of possible cells we need to remove cells whose downstream is occupied
                    # otherwise we would break the rule that states that the cell downstream of a power plant
                    # with colling technology 1 or 4 should be empty
                    NextCellCoordID = cellCandidate.apply(nextCell, axis=1)
                    DownOccupied = cells.loc[cells['CoordID'].isin(NextCellCoordID) & cells['Occupied'] > 0, 'CoordID']
                    DelCells = NextCellCoordID.loc[NextCellCoordID.isin(DownOccupied)].index
                    if len(DelCells) > 0:
                        cellCandidate.drop(DelCells, inplace=True)

                #  Next we force plants to be on "actual rivers" and avoid plants being "built" on cells with
                # just enough, but very limited, discharge. We currently set the threshold to define "actual
                # river" as Available Discharge > 10 m3/sec --> so everything else is set to discharge = 0
                cellCandidate.loc[cellCandidate['AvailDisch'] < 10, 'AvailDisch']=0

                # We
                for ix, cell in cellCandidate.iterrows():
                    CellDischarge=cell['AvailDisch']
#                    if CellDischarge < 10:
#                        CellDischarge = 0
                    AltWater = 0
                    if wpMW == 0:
                        avail4Water = inf
                    else:
                        # avail4Water = (cell['AvailDisch'] * .3) / wpMW
                        avail4Water = CellDischarge / wpMW
                    if avail4Water <= 0.75 * delta:  # We have an AltWater plant that does not relay on surface water
                        AltWater = 1
                        avail4Water = inf
                    aCap = min(delta, MaxPlantSize, avail4Water)
                    id=create(ix, aCap, capoutput, effoutput, cells, retirementDB, y, Fuel, Cooling, AltWater)
#                    if AltWater: # Handled by the create routine
#                        capoutput.loc[id, 'AltWater']=AltWater
                    delta=abs(delta-aCap)
                    if delta == 0:
                        break
                if delta == 0:
                    break
        # If we still have some delta then we log the error
        if delta>.000001:
            print('Unable to allocate capacity. Key {}, year {}, capacity {}'.format(key,y,delta))
            log('create',key,y,delta)


"""""""""""""""""
main cap function
"""""""""""""""""
def capacity(plantsByID, capDeltas, retirementYears, cells, sumDB):
    """
    main cap function
    """
    # capoutput is dataframe that holds the final output of the whole process
    # capoutput is structured as a table with the Years in columns and the PowerPlant ID in the rows
    # capoutput has a unique index defined by the input Power Plant database

    OutputCols=['Index',
                'PlantCode',
                'RelKey',
                'PCA',
                'Fuel',
                'Cooling',
                'State',
                'Withdrawal',
                'Longitude',
                'Latitude',
                'CellID',
                'AvailDisch',
                'LakeOcean',
                'AltWater',
                'RetireYear',
                'Changed'
                ]+['NP_'+str(y) for y in R2T_Globals.YRS]

    EfficiCols=['Index',
                'PlantCode',
                'RelKey',
                'PCA',
                'Fuel',
                'Cooling',
                'Longitude',
                'Latitude'
                ]+['Eff_' + str(y) for y in R2T_Globals.YRS]

    RetiredCols=['Index',
                 'CellID',
                 'PCA',
                 'YearRetired',
                 'Occupied',
                 'AvailDisch'
                 ]


    # the table with the ReEDS output (capDeltas) is indexed with a key thas is a tuple of
    # (PCA, fuel, cooling). Looping on the capDeltas is the secondary loop within each year
    #
    # We need to threat keys that are only present in the ReEDS output in a different way for
    # the years included in the Power Plants database. To identify those keys we extract the
    # difference of the ReEDS output index and the DBsum index (e.g the aggregated index from
    # the Power Plant database)
    NotInPP_DB=capDeltas.index.difference(sumDB.index)
    # In order to use a smaller network and to subset the power plants database accordingly we first get the list
    # of CellIDs and PCAs included in the cells table
    global ActivePCA
    ActiveCells=cells['CellID'].unique()
    ActivePCA=cells['PCA'].unique()

    # We create an index that is twice as big as the index of the power plants database.
    # This should account for all the added plants and will drastically speed add operations.
    outIndex=plantsByID.loc[plantsByID['CellID']
        .isin(ActiveCells)].index\
        .append(pd.Int64Index
                             (list(range(max(plantsByID\
                                .index)+1,max(plantsByID\
                                              .index)+len(plantsByID\
                                                          .index)))))
    # And we create the output tables for both capacity and efficiency using the
    # index above and the pertinent list of columns
    capoutput=pd.DataFrame(index=outIndex,columns=OutputCols)
    effoutput=pd.DataFrame(index=outIndex,columns=EfficiCols)

    # And we save the MaxPlant ID to be used when calculating new ID
    # and Index entries when adding a power plant
    global MaxPlantID
    MaxPlantID = max(plantsByID.index)
    global EndYearInDatabase
    EndYearInDatabase=max(R2T_Globals.DBYears)

    # We now load the data from the Power Plants database in the output table
    # ONLY for the years included in the database
    for col in capoutput.columns:
        if col in plantsByID.columns:
            capoutput[col]=plantsByID[col]

    # And we do the same for the efficiency output table
    # ONLY for the years included in the database
    for col in effoutput.columns:
        if col in plantsByID.columns:
            effoutput[col]=plantsByID[col]

    capoutput.fillna(0, inplace=True)
    effoutput.fillna(0, inplace=True)

    capoutput['x']=capoutput.index

    tmpCapout = pd.merge(capoutput,
                   cells[['CellID', 'AvailDisch']].rename(
                       columns={'CellID': 'CellID', 'AvailDisch': 'Temp'}),
                       how='left', on=['CellID']).fillna(0)

    tmpCapout['AvailDisch'] = tmpCapout['Temp']

    capoutput=tmpCapout.drop('Temp',axis=1).set_index('x')



    # Now we create the table that will hold the info about the retired plants (same inndex as the capoutput table)
    retired=pd.DataFrame(index=outIndex,columns=RetiredCols)
#    retired=retired.set_index(['Index'],drop=False)

    # We need to add some space to the retirementYears dataframe to hold the info for the newly created
    # Power Plants as dataframes are only passed by reference when no structural modification is done
    # (e.g no adding rows or columns)

    retirementDB=pd.DataFrame(index=retirementYears.index.append(
        pd.Int64Index(list(
                range(
                    max(retirementYears.index)+1,
                    max(retirementYears.index)+
                    len(retirementYears.index)
                     )
                )
        )),columns=retirementYears.columns)

    for col in retirementDB.columns:
        if col in retirementYears.columns:
            retirementDB[col]=retirementYears[col]

    retirementDB.fillna(0,inplace=True)

    # We initialize a field that will keep the actual year of retirement of the power plant
    capoutput['YearRetired']=9999

    # Main loop - through each year in the experiment
    for y in R2T_Globals.YRS:
        print_if(y)
        # For the year between 2010 (Initial year) and 2015 (rounded up to the closes year in step 2, e.g. 2016)
        # we need to retire the power plants that no longer have any capacity in the database.
        # The value was initialized while reading the Power Plants database and stored in the field RetireYear
#        if y == 2014:
#            print(y)
        capoutput['Changed']=0
        if y <= EndYearInDatabase:
            retirees=capoutput.loc[capoutput['RetireYear']==y]
            if not retirees.empty:
                retire(retirees.index,retired,capoutput,effoutput,cells,y)
        # Now we apply the capacity allocation rules for year y for each combination of PCA, Fuel and Cooling
        for key in capDeltas.index:
            apply_rules(key, y, capDeltas.loc[key,'NP_' + str(y)], retired, capoutput, effoutput, cells, retirementDB)
        # Now we do a little bit of housekeeping. The basic idea is to copy the current year in the next so
        # that at the next step of the iteration the delta Capacity stored in capDeltas can be applied to
        # that year's capacity column
        #
        # However the basic principle only applies to the years that are not contained in the Power Plants database
        # so first we do the basic copy forward if the current year y is in not in the Power Plant database
        if EndYearInDatabase <= y < R2T_Globals.YEAR_E:
            capoutput['NP_'+str(y+2)]=capoutput['NP_'+str(y)]
            effoutput['Eff_' + str(y + 2)] = effoutput['Eff_' + str(y)]
        # For the years that were originally in the Power Plant database we only apply the basic copy forward
        # idea to those combination of PCA, Fuel and Cooling that were not original in the Power Plant Database
        # and to the power plants that where added during the current year
        elif y != R2T_Globals.YEAR_E:
            # We should be dealing with the added power plants
            #capoutput.loc[capoutput['Changed'] == 1,'NP_' + str(y + 2)] = capoutput['NP_'+str(y)]
            for lkey in NotInPP_DB:
                lPCA=lkey[0]
                lFuel=lkey[1]
                lCooling=lkey[2]
                TranferCap=capoutput.loc[((capoutput['PCA'] == lPCA) &
                               (capoutput['Fuel'] == lFuel) &
                               (capoutput['Cooling'] == lCooling)),'NP_' + str(y)]
                TranferEff=effoutput.loc[((effoutput['PCA'] == lPCA) &
                               (effoutput['Fuel'] == lFuel) &
                               (effoutput['Cooling'] == lCooling)),'Eff_' + str(y)]
                capoutput.loc[((capoutput['PCA'] == lPCA) &
                               (capoutput['Fuel'] == lFuel) &
                               (capoutput['Cooling'] == lCooling)),'NP_' + str(y + 2)] = TranferCap
                effoutput.loc[((effoutput['PCA'] == lPCA) &
                               (effoutput['Fuel'] == lFuel) &
                               (effoutput['Cooling'] == lCooling)), 'Eff_' + str(y + 2)] = TranferEff

    # Before we return the result we add the "RelKey' field (used during the generation allocation)
    #capoutput['RelKey']=capoutput['PCA']*100+capoutput['Fuel']*10+capoutput['Cooling']
    capoutput['RelKey'] = capoutput.apply(SetRelKey,axis=1) #(capoutput['PCA'], capoutput['Fuel'], capoutput['Cooling'])
    effoutput['RelKey'] = effoutput.apply(SetRelKey,axis=1) #(capoutput['PCA'], capoutput['Fuel'], capoutput['Cooling'])

    # Finaly given that we created extra rows to speed up the application of the ruleset, we
    # slice the results and return only the rows that have values
    return capoutput.loc[capoutput['PlantCode'] > 0], effoutput[effoutput['PlantCode'] > 0]


"""""""""""""""
save cap output
"""""""""""""""
# ADD AltWater,Lat,Lon,OceanLake,CellID
def saveCap(capoutput, dir, extra=''):

    OutFile=dir+"capoutput" + extra + ".csv"
    print_if('Saving file {}'.format(OutFile))
    capoutput.to_csv(OutFile,index=False)

def saveEff(effoutput, dir, extra=''):

    OutFile=dir+"effoutput" + extra + ".csv"
    print_if('Saving file {}'.format(OutFile))
    effoutput.to_csv(OutFile,index=False)
