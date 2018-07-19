"""
R2T_generation.py
"""

import R2T_Globals
from R2T_Globals import *


"""""""""""""""""
main gen function
"""""""""""""""""
def generation(gen_input, cap_plants, extra=''):


    # Initializa an empty Dataframe to hold the output. Initialitzing dataframes to their final size
    # helps speed up the whole process
    gen_plants = pd.DataFrame(index=cap_plants.index) #,columns=C_order) # .copy()

    # and we add data from a few columns from the capacity table...
    gen_plants = pd.merge(gen_plants, cap_plants[['PlantCode',
                                                  'RelKey',
                                                  'Fuel',
                                                  'Cooling',
                                                  'Longitude',
                                                  'Latitude',
                                                  'CellID',
                                                  'LakeOcean',
                                                  'AltWater']], left_index=True,
                          right_index=True)  # right_on='PlantID')

    # again we do a bit of adjustment if the Flag that defines the merge
    # of Fuel Type 6 Cooling 8 with Fuel Type 3 Cooling is set
    #    if merge6_8and3_8:
    #        cap_plants.loc[((cap_plants['Fuel'] == 6) & (cap_plants['Cooling'] == 8)), ['Fuel']] = 3

    # We reduce the size of the ReEDS generatio table to match only the Rel_Keys taht are actually present
    # in the Plants' Capacity database
    RelKeyInCap=cap_plants['RelKey'].unique()
    gen_input=gen_input.loc[gen_input.index.isin(RelKeyInCap)]

    # Main loops, we move through the Years and the Seasons and perform the
    # allocation of the seasonal generation to the power plants and then partition it
    # by month

    for y in R2T_Globals.YRS:

        # Create a variable to hold the name of the current capacity column
        # (Yxxxx, where xxxx is the current year)
        pp_year = 'NP_' + str(y)

        # To avoid dragging along useless data we extract the 'RelKey' and the Current year
        # info from the Power Plants capacity table
        plants_cap_year = cap_plants[['RelKey', pp_year]].copy()

        # and we calculate the total capacity per RelKey
        # (e.g. per PCA, Fuel Type and Cooling Tech)...
        pca_tot_cap_year = plants_cap_year.groupby(by=['RelKey'])[pp_year].sum().to_frame(name='TotCap').astype(float)

        # Now we are ready to loop through the seasons
        for s in R2T_Globals.Seasons:

            # Create a variable to hold the name of the current season column (season name + year)
            Step = s + str(y)



            # We extract to a Dataframe just the column with the current seasonal generation
            # and we exclude any record for which there is 0 generation for this specific season
            gen_season = gen_input.loc[gen_input[Step] != 0][[Step]]

            # We merge the above Dataframe with the Dataframe with the total capacity per RelKey
            gen_season = pd.merge(gen_season, pca_tot_cap_year, left_index=True, right_index=True)

            # Get the number of days in current season from the approariate function
            # which takes into account leap years
            DaysInSeason = R2T_Globals.days_in_season(s, y)

            # And calculate the MaxGeneration for each power plant. This is:
            #   installed capacity at power plant for the current year
            #   multiplied by 24 hours
            #   multiplied by a capacity factor (currently hardcoded to 1 - no effect)
            #   multiplied by the number of days in the current season
            plants_cap_year['MaxGeneration'] = plants_cap_year[pp_year] * 24 * R2T_Globals.capacity_factor * DaysInSeason

            # Now we can merge all the info into the main Dataframe that we will use to perform all the
            # calculations
            # We merge:
            #     the Dataframe with the power plants installed capacity
            #     the Dataframe with the current season generation requirement
            plants_cap_season = pd.merge(plants_cap_year, gen_season, how='left', left_on='RelKey',
                                         right_index=True).fillna(0)  # .set_index('PlantID').fillna(0)

            # now we can allocate to each power plant the fraction of total generation required
            # the generation is allocated by:
            #     plant's capacity / total capacity for the specific PCA, Fuel Type and Cooling Tech
            plants_cap_season[Step] = plants_cap_season[pp_year] / plants_cap_season['TotCap'] * plants_cap_season[Step]

            # However the generation cannot exceed the maximum generation capacity for the power plant
            # as calculated above...
            # We add a new column and initize it with 0s
            plants_cap_season['AboveMax'] = 0

            # When the generation exceeds the Max, then we store the excess in the new column
            plants_cap_season.loc[(plants_cap_season['MaxGeneration'] < plants_cap_season[Step]), 'AboveMax'] = \
            plants_cap_season[Step] - plants_cap_season['MaxGeneration']

            # And we check if the is at least one power plant that exceeded the Maximum Generation
            Residuals = plants_cap_season.loc[plants_cap_season['AboveMax'] != 0].empty

            # If at least one is found then we need to enter the following loop that will set the power plant
            # to the Maximum allowed generation and split the remaining unallocated generation among the other
            # power plants that have the same Fuel Type and Cooling tech inside the same PCA
            while not Residuals:

                # First we set the generation for the plants that exceeded their Maximum to the Maximum
                plants_cap_season.loc[(plants_cap_season['AboveMax'] != 0), Step] = plants_cap_season['MaxGeneration']

                # then we extract the portion of the total generation that needs to be reallocated
                gen_add = plants_cap_season.groupby(by=['RelKey'])['AboveMax'].sum().to_frame(name='AddGen').astype(
                    float)

                # and the amount of capacity that we need to subtract from the total to reallocate to the remaining
                # power plants
                cap_chg = plants_cap_season.loc[(plants_cap_season['AboveMax'] != 0)].groupby(by=['RelKey'])[
                    pp_year].sum().to_frame(name='CapChg').astype(float)

                # Now we merge the Dataframes with the additional data with the main Dataframe
                plants_cap_season = pd.merge(plants_cap_season, gen_add, how='left', left_on='RelKey',
                             right_index=True).fillna(0)  # .set_index('PlantID').fillna(0)
                plants_cap_season = pd.merge(plants_cap_season, cap_chg, how='left', left_on='RelKey',
                             right_index=True).fillna(0)  # .set_index('PlantID').fillna(0)

                # Now we can add the new generation to the remaining power plants
                plants_cap_season.loc[(plants_cap_season['MaxGeneration'] > plants_cap_season[Step]) & \
                    (plants_cap_season['AddGen'] > 0), Step] += \
                     plants_cap_season[pp_year] / (plants_cap_season['TotCap'] - plants_cap_season['CapChg']) * \
                     plants_cap_season['AddGen']

                # And we perform the test again. we first reset the AboveMax column to 0
                plants_cap_season['AboveMax'] = 0

                # ...and then we check if we still have power plants that exceed the Max
                # First calculate the difference between the MaxGeneration and the allocated generation
                plants_cap_season.loc[(plants_cap_season['MaxGeneration'] < plants_cap_season[Step]), 'AboveMax'] = \
                plants_cap_season[Step] - plants_cap_season['MaxGeneration']

                # Then we check if we have residuals and set the vairable
                Residuals = plants_cap_season.loc[plants_cap_season['AboveMax'] != 0].empty

                # Before we loop we erase the two columns tht were added by the two merge operations above
                plants_cap_season = plants_cap_season.drop('AddGen', 1)
                plants_cap_season = plants_cap_season.drop('CapChg', 1)

            # If the allocation is successful we can now split the seasonal generation into monthly
            # generations
            # We loop through the months in the current season
            for m in R2T_Globals.season_to_month[s]:

                # Create a variable to hold the name of the current month column (month abreviation + year)
                cur_month = m + str(y)

                # The split by month is done proportionally to the number of days in the month
                # divided by the total number of days in the season
                plants_cap_season[cur_month] = plants_cap_season[Step] * R2T_Globals.days_in_month(m, y) / DaysInSeason

                # We can now extract the new column from the main processing Dataframe and merge it with
                # the results Dataframe
                gen_plants = pd.merge(gen_plants, plants_cap_season[[cur_month]], left_index=True, right_index=True)
                # And we patch the NANs generated by the merge with 0s
                gen_plants[cur_month] = gen_plants[cur_month].fillna(0)

    print_if('Finished processing generation, doing some final clean up!')

    # Once we exit the loop we have the final output.
    # We just need to add a few columns from the capacity table, do some cleanup by reordering the
    # columns of the output and we are done
    # and we add a few columns from the capacity table...

    C_order = ['PlantCode',
               'RelKey',
               'Fuel',
               'Cooling',
               'Longitude',
               'Latitude',
               'CellID',
               'LakeOcean',
               'AltWater']
    for y in R2T_Globals.YRS:
        for m in R2T_Globals.Months:
            C_order.append(m + str(y))

    return gen_plants[C_order]

"""""""""""""""
save gen output
"""""""""""""""
def saveGen(genoutput, dir, extra=''):

    OutFile = dir+"genoutput" + extra + ".csv"
    print_if('Saving file {}'.format(OutFile))
    genoutput.to_csv(OutFile)
