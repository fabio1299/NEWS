#! /usr/bin/python


import numpy as np
import subprocess as sp
import glob
import calendar
import random
from R2T_Globals import *
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO


# The following is a small routine that can help with debugging

#def print_if(value):
#    if print_if_flag:
#        print(value)
#        sys.stdout.flush()

def PCAStatInput(discharge_base,runoff_base,start_needed,end_needed, output, verbose=False):

    if __name__ != "__main__":
        global print_if_flag
        print_if_flag = verbose

    print_if('Running shell script to calculate daily water balance by PCA')

    outfile=output+".txt"
    if verbose:
        Arg6='1'
    else:
        Arg6=''


    OutFile='/asrc/ecr/NEWS/configurations/Link/tmp/TEMP'+str(random.randint(10000,99999))

    Arg5=OutFile


    cmd=['/asrc/ecr/NEWS/configurations/Link/Pr_PCA_WaterStats.sh',
         discharge_base, runoff_base, str(start_needed),
         str(end_needed), Arg5, Arg6]

    sp.call(cmd)
    data = pd.read_csv(OutFile + 'mBudget.csv', sep='\t',
                             index_col=['PCA_REG'],
                             dtype={'PCA_REG' : 'int',
                                    'Year' : 'int',
                                    'Month' : 'int',
                                    'Area' : 'float',
                                    'Local' : 'float',
                                    'Entering' : 'float',
                                    'Re-entering' : 'float',
                                    'Leaving' : 'float',
                                    'Balance' : 'float',
                                    'TAWR' : 'float'
                                    })
    os.remove(OutFile + 'mBudget.csv')
    return data

# for scenario in ['RCP2p6_Final925', 'RCP4p5_Final925', 'RCP6p0_Final925', 'RCP8p5_Final925']:
# for scenario in ['RCP2p6_Final925', 'RCP8p5_Final925']:
def PCAStats(modelname, scenario, data,
             selec_yrs, b_number, a_number,
             year_of_interest_start, year_of_interest_end,
             Hist_year_start, Hist_year_end,
             output_file, verbose=False):

    if __name__ != "__main__":
        global print_if_flag
        print_if_flag = verbose

    print_if('Runnig seasonal aggregation of water balance by PCA')

    # Create a random number to add to the temp files names in order to
    # be able to spin concurrent runs of this function
    rand=str(random.randint(10000,99999))
    tmpDir='/asrc/ecr/NEWS/configurations/Link/tmp'
    data['Days_in_month'] = data.apply(lambda x: calendar.monthrange(int(x['Year']), int(x['Month']))[1], axis=1)
    data['Conv_Entering'] = data.apply(lambda x: x['Entering'] * 3600 * 24 * (10 ** -9) * x['Days_in_month'], axis=1)
    data['Conv_Re-entering'] = data.apply(lambda x: x['Re-entering'] * 3600 * 24 * (10 ** -9) * x['Days_in_month'],
                                          axis=1)
    data['Water_available'] = data.apply(lambda x: x['Conv_Entering'] + x['Local'] * (10 ** -9) - x['Conv_Re-entering'],
                                         axis=1)
    data["PCA"] = data.index

    a = data.groupby('Year')
    Seasonal = pd.DataFrame()
    Fraction_Seasonal = pd.DataFrame()
    Annual = pd.DataFrame()
    Fraction_Annual = pd.DataFrame()
    Historic = pd.DataFrame()
    Spring_Final = pd.DataFrame()
    Winter_Final = pd.DataFrame()
    Summer_Final = pd.DataFrame()
    Fall_Final = pd.DataFrame()
    Seasonal_Final = pd.DataFrame()
    Fraction_Annual_H = pd.DataFrame()
    Annual_2 = pd.DataFrame()
    Winter_Change = pd.DataFrame()
    Spring_Change = pd.DataFrame()
    Summer_Change = pd.DataFrame()
    Fall_Change = pd.DataFrame()
    Winter_H = pd.DataFrame()
    Spring_H = pd.DataFrame()
    Summer_H = pd.DataFrame()
    Fall_H = pd.DataFrame()

    for index, group in a:
        print_if('Working on year {}'.format(index))
        current2 = data[(data.Year == index)]
        aa = current2.groupby('PCA')
        app_wi_pca = []
        app_sp_pca = []
        app_su_pca = []
        app_fa_pca = []
        app_annual = []
        for pca, group in aa:
            current = current2[(current2.PCA == pca)]
            wi_pca = \
            current[(current.Month == 1) | (current.Month == 2) | (current.Month == 11) | (current.Month == 12)].sum()[
                'Water_available']
            sp_pca = current[(current.Month == 3) | (current.Month == 4) | (current.Month == 5)].sum()[
                'Water_available']
            su_pca = current[(current.Month == 6) | (current.Month == 7) | (current.Month == 8)].sum()[
                'Water_available']
            fa_pca = current[(current.Month == 9) | (current.Month == 10)].sum()['Water_available']
            annual_pca = current.sum()['Water_available']
            app_wi_pca.append(wi_pca)
            app_sp_pca.append(sp_pca)
            app_su_pca.append(su_pca)
            app_fa_pca.append(fa_pca)
            app_annual.append(annual_pca)
        Seasonal[('Winter_%s' % index)] = app_wi_pca
        Seasonal[('Spring_%s' % index)] = app_sp_pca
        Seasonal[('Summer_%s' % index)] = app_su_pca
        Seasonal[('Fall_%s' % index)] = app_fa_pca
        Annual[('%s' % index)] = app_annual
        Fraction_Seasonal[('Winter_%s' % index)] = Seasonal[('Winter_%s' % index)] / Annual[('%s' % index)]
        Fraction_Seasonal[('Spring_%s' % index)] = Seasonal[('Spring_%s' % index)] / Annual[('%s' % index)]
        Fraction_Seasonal[('Summer_%s' % index)] = Seasonal[('Summer_%s' % index)] / Annual[('%s' % index)]
        Fraction_Seasonal[('Fall_%s' % index)] = Seasonal[('Fall_%s' % index)] / Annual[('%s' % index)]

    # Longterm means
    Annual_mean = pd.DataFrame()
    Seasonal_mean = pd.DataFrame()
    Seasonal_mean_frac = pd.DataFrame()

    # Currently set to:
    # year_of_interest_start=2010
    # year_of_interest_end=2050
    # in routine MainLink.py

    for yr in range(year_of_interest_start - 1, year_of_interest_end + 1):
        if yr != (year_of_interest_start - 1):
            start_yr = yr - b_number
        else:
            start_yr = yr - a_number
        end_yr = yr + a_number
        sum_win_frac = np.zeros(len(Fraction_Seasonal[('Winter_%s' % yr)]), dtype=np.int)
        sum_spr_frac = np.zeros(len(Fraction_Seasonal[('Spring_%s' % yr)]), dtype=np.int)
        sum_sum_frac = np.zeros(len(Fraction_Seasonal[('Summer_%s' % yr)]), dtype=np.int)
        sum_fal_frac = np.zeros(len(Fraction_Seasonal[('Fall_%s' % yr)]), dtype=np.int)
        sum_annual = np.zeros(len(Annual[('%s' % yr)]), dtype=np.int)
        sum_win = np.zeros(len(Seasonal[('Winter_%s' % yr)]), dtype=np.int)
        sum_spr = np.zeros(len(Seasonal[('Spring_%s' % yr)]), dtype=np.int)
        sum_sum = np.zeros(len(Seasonal[('Summer_%s' % yr)]), dtype=np.int)
        sum_fal = np.zeros(len(Seasonal[('Fall_%s' % yr)]), dtype=np.int)
        for mean_yr in range(start_yr, end_yr + 1):
            sum_annual = sum_annual + Annual[('%s' % mean_yr)]
            sum_win = sum_win + Seasonal[('Winter_%s' % mean_yr)]
            sum_spr = sum_spr + Seasonal[('Spring_%s' % mean_yr)]
            sum_sum = sum_sum + Seasonal[('Summer_%s' % mean_yr)]
            sum_fal = sum_fal + Seasonal[('Fall_%s' % mean_yr)]
        mean_annual = sum_annual / selec_yrs
        mean_win = sum_win / selec_yrs
        mean_spr = sum_spr / selec_yrs
        mean_sum = sum_sum / selec_yrs
        mean_fal = sum_fal / selec_yrs
        Annual_mean[('%s' % yr)] = mean_annual
        Seasonal_mean[('Winter_%s' % yr)] = mean_win
        Seasonal_mean[('Spring_%s' % yr)] = mean_spr
        Seasonal_mean[('Summer_%s' % yr)] = mean_sum
        Seasonal_mean[('Fall_%s' % yr)] = mean_fal

    # UNCOMMENT BELOW FOR HISTORIC BEING STARTING VALUE
    # for yr in range(2000, 2005):
    # Currently set to:
    # Hist_year_start=1985
    # Hist_year_end=2005
    # in module MainLink.py

    for yr in range(Hist_year_start, Hist_year_end):
        Historic[('%s' % yr)] = Annual[('%s' % yr)]
        Winter_H[('%s' % yr)] = Seasonal[('Winter_%s' % yr)]
        Spring_H[('%s' % yr)] = Seasonal[('Spring_%s' % yr)]
        Summer_H[('%s' % yr)] = Seasonal[('Summer_%s' % yr)]
        Fall_H[('%s' % yr)] = Seasonal[('Fall_%s' % yr)]

    Historic_mean = Historic.mean(axis=1)
    Winter_H_mean = Winter_H.mean(axis=1)
    Spring_H_mean = Spring_H.mean(axis=1)
    Summer_H_mean = Summer_H.mean(axis=1)
    Fall_H_mean = Fall_H.mean(axis=1)
    Annual_2['2010'] = Historic_mean

    # ADD MEAN TO BELOW:
    Mean_Frac_Annual = pd.DataFrame()
    Mean_Frac_Winter = pd.DataFrame()
    Mean_Frac_Spring = pd.DataFrame()
    Mean_Frac_Summer = pd.DataFrame()
    Mean_Frac_Fall = pd.DataFrame()

    for yr in range(year_of_interest_start, year_of_interest_end + 1, 2):
        if (yr == year_of_interest_start):
            pre_yr = yr - 1
            Fraction_Annual_H[('%s' % yr)] = Annual_2[('%s' % yr)] / Annual_2[str(year_of_interest_start)]
            Winter_Change[('%s' % yr)] = Winter_H_mean / Winter_H_mean  # do for all seasons
            Spring_Change[('%s' % yr)] = Spring_H_mean / Spring_H_mean
            Summer_Change[('%s' % yr)] = Summer_H_mean / Summer_H_mean
            Fall_Change[('%s' % yr)] = Fall_H_mean / Fall_H_mean

            Mean_Frac_Annual[('%s' % yr)] = Annual_mean[('%s' % yr)] / Annual_mean[str(year_of_interest_start)]
            Mean_Frac_Winter[('%s' % yr)] = ((Seasonal_mean[('Winter_%s' % pre_yr)] + Seasonal_mean[
                ('Winter_%s' % yr)]) * 0.5) / ((Annual_mean[('%s' % pre_yr)] + Annual_mean[('%s' % yr)]) * 0.5)
            Mean_Frac_Spring[('%s' % yr)] = ((Seasonal_mean[('Spring_%s' % pre_yr)] + Seasonal_mean[
                ('Spring_%s' % yr)]) * 0.5) / ((Annual_mean[('%s' % pre_yr)] + Annual_mean[('%s' % yr)]) * 0.5)
            Mean_Frac_Summer[('%s' % yr)] = ((Seasonal_mean[('Summer_%s' % pre_yr)] + Seasonal_mean[
                ('Summer_%s' % yr)]) * 0.5) / ((Annual_mean[('%s' % pre_yr)] + Annual_mean[('%s' % yr)]) * 0.5)
            Mean_Frac_Fall[('%s' % yr)] = ((Seasonal_mean[('Fall_%s' % pre_yr)] + Seasonal_mean[
                ('Fall_%s' % yr)]) * 0.5) / ((Annual_mean[('%s' % pre_yr)] + Annual_mean[('%s' % yr)]) * 0.5)
        else:
            pre_yr = yr - 1
            Fraction_Annual_H[('%s' % yr)] = ((Annual_mean[('%s' % pre_yr)] + Annual_mean[('%s' % yr)]) * 0.5) / \
                                             Annual_2[str(year_of_interest_start)]
            Winter_Change[('%s' % yr)] = ((Seasonal_mean[('Winter_%s' % pre_yr)] + Seasonal_mean[
                ('Winter_%s' % yr)]) * 0.5) / Winter_H_mean  # do for all seasons
            Spring_Change[('%s' % yr)] = ((Seasonal_mean[('Spring_%s' % pre_yr)] + Seasonal_mean[
                ('Spring_%s' % yr)]) * 0.5) / Spring_H_mean
            Summer_Change[('%s' % yr)] = ((Seasonal_mean[('Summer_%s' % pre_yr)] + Seasonal_mean[
                ('Summer_%s' % yr)]) * 0.5) / Summer_H_mean
            Fall_Change[('%s' % yr)] = ((Seasonal_mean[('Fall_%s' % pre_yr)] + Seasonal_mean[
                ('Fall_%s' % yr)]) * 0.5) / Fall_H_mean

            Mean_Frac_Winter[('%s' % yr)] = ((Seasonal_mean[('Winter_%s' % pre_yr)] + Seasonal_mean[
                ('Winter_%s' % yr)]) * 0.5) / ((Annual_mean[('%s' % pre_yr)] + Annual_mean[('%s' % yr)]) * 0.5)
            Mean_Frac_Spring[('%s' % yr)] = ((Seasonal_mean[('Spring_%s' % pre_yr)] + Seasonal_mean[
                ('Spring_%s' % yr)]) * 0.5) / ((Annual_mean[('%s' % pre_yr)] + Annual_mean[('%s' % yr)]) * 0.5)
            Mean_Frac_Summer[('%s' % yr)] = ((Seasonal_mean[('Summer_%s' % pre_yr)] + Seasonal_mean[
                ('Summer_%s' % yr)]) * 0.5) / ((Annual_mean[('%s' % pre_yr)] + Annual_mean[('%s' % yr)]) * 0.5)
            Mean_Frac_Fall[('%s' % yr)] = ((Seasonal_mean[('Fall_%s' % pre_yr)] + Seasonal_mean[
                ('Fall_%s' % yr)]) * 0.5) / ((Annual_mean[('%s' % pre_yr)] + Annual_mean[('%s' % yr)]) * 0.5)

    PCA_index = Mean_Frac_Annual.index + 1

    Mean_Frac_Annual = Mean_Frac_Annual.set_index(PCA_index)
    Mean_Frac_Winter = Mean_Frac_Winter.set_index(PCA_index)
    Mean_Frac_Spring = Mean_Frac_Spring.set_index(PCA_index)
    Mean_Frac_Summer = Mean_Frac_Summer.set_index(PCA_index)
    Mean_Frac_Fall = Mean_Frac_Fall.set_index(PCA_index)

    Fraction_Annual_H = Fraction_Annual_H.set_index(PCA_index)
    Winter_Change = Winter_Change.set_index(PCA_index)
    Spring_Change = Spring_Change.set_index(PCA_index)
    Summer_Change = Summer_Change.set_index(PCA_index)
    Fall_Change = Fall_Change.set_index(PCA_index)

    Mean_Frac_Winter.to_csv(
        ('%s/TMP%s_Mean_Frac_Winter_%s_%s.csv' % (tmpDir, rand, modelname, scenario)),
        index_label='PCA')
    Mean_Frac_Spring.to_csv(
        ('%s/TMP%s_Mean_Frac_Spring_%s_%s.csv' % (tmpDir, rand, modelname, scenario)),
        index_label='PCA')
    Mean_Frac_Summer.to_csv(
        ('%s/TMP%s_Mean_Frac_Summer_%s_%s.csv' % (tmpDir, rand, modelname, scenario)),
        index_label='PCA')
    Mean_Frac_Fall.to_csv(
        ('%s/TMP%s_Mean_Frac_Fall_%s_%s.csv' % (tmpDir, rand, modelname, scenario)),
        index_label='PCA')

    Fraction_Annual_H.to_csv(
        ('%s/TMP%s_Annual_Change_%s_%s.csv' % (tmpDir, rand, modelname, scenario)),
        index_label='PCA')
    Winter_Change.to_csv(
        ('%s/TMP%s_Winter_Change_%s_%s.csv' % (tmpDir, rand, modelname, scenario)),
        index_label='PCA')
    Spring_Change.to_csv(
        ('%s/TMP%s_Spring_Change_%s_%s.csv' % (tmpDir, rand, modelname, scenario)),
        index_label='PCA')
    Summer_Change.to_csv(
        ('%s/TMP%s_Summer_Change_%s_%s.csv' % (tmpDir, rand, modelname, scenario)),
        index_label='PCA')
    Fall_Change.to_csv(
        ('%s/TMP%s_Fall_Change_%s_%s.csv' % (tmpDir, rand, modelname, scenario)),
        index_label='PCA')

    print_if('all done')

    Final_Data = pd.DataFrame(columns=['watclass'])
    value = []
    BA = []
    Season = []
    year = []
    print_if('Finalizing mean fraction for season:')
    for season in ['Summer', 'Winter', 'Fall', 'Spring']:
        print(season)
        file1 = ('%s/TMP%s_Mean_Frac_%s_%s_%s.csv' % (tmpDir, rand, season, modelname, scenario))
        data1 = pd.read_csv(file1)
        for gBA in (data1.PCA.unique()):
            for yr in range(year_of_interest_start, year_of_interest_end + 1, 2):
                data2 = data1[(data1.PCA == gBA)]
                v1 = data2[('%s' % yr)].max()
                value.append(v1)
                BA.append('p'+str(gBA))
                Season.append(season)
                year.append(yr)
    Final_Data['BA'] = BA
    Final_Data['Season'] = Season
    Final_Data['Year'] = year
    Final_Data['Value'] = value
    Final_Data['watclass'] = 'watclass2'

    #Final_Data.to_csv("/asrc/ecr/ariel/NEWS/tmp/Final/%s_%s_MeanFrac.csv" % (modelname, scenario))
    #Final_Data.to_csv(output_file + '_MeanFrac.csv',index=False)
    Final_Data.to_csv(output_file + 'UnappWaterSeaAnnDistr.csv',index=False)

    Final_Data = pd.DataFrame(columns=['watclass'])
    value = []
    BA = []
    Season = []
    year = []
    print_if('Finalizing change for season:')
    for season in ['Summer', 'Winter', 'Fall', 'Spring', 'Annual']:
        print_if(season)
        file1 = ('%s/TMP%s_%s_Change_%s_%s.csv' % (tmpDir, rand, season, modelname, scenario))
        data1 = pd.read_csv(file1)
        for gBA in (data1.PCA.unique()):
            for yr in range(year_of_interest_start, year_of_interest_end + 1, 2):
                data2 = data1[(data1.PCA == gBA)]
                v1 = data2[('%s' % yr)].max()
                value.append(v1)
                BA.append('p'+str(gBA))
                Season.append(season)
                year.append(yr)
    Final_Data['BA'] = BA
    Final_Data['Season'] = Season
    Final_Data['Year'] = year
    Final_Data['Value'] = value
    Final_Data['watclass'] = 'watclass2'

    #Final_Data.to_csv("/asrc/ecr/ariel/NEWS/tmp/Final/%s_%s_Change.csv" % (modelname, scenario))
    #Final_Data.to_csv(output_file + '_Change.csv',index=False)
    # As per ReEDS standards:
    # We save the individual seasons in the file UnappWaterMult.csv
    Final_Data.loc[Final_Data['Season'] != 'Annual'].to_csv(output_file + 'UnappWaterMult.csv',index=False)
    # And the annual in the file UnappWaterMultAnn.csv 9removing the column season)
    cols=['watclass','BA','Year','Value']
    Final_Data.loc[Final_Data['Season'] == 'Annual'].to_csv(output_file + 'UnappWaterMultAnn.csv',index=False, columns=cols)

    PathToDelate=('%s/TMP%s_*' % (tmpDir, rand))
    for x in glob.glob(PathToDelate):
        os.remove(x)

if __name__ == "__main__":
    # Averaging window
    # before number should be greater (5,4 = 10yr), (10,9 = 20 yr), (15,14 = 30yr)
    w_and_p_num = 20 # window size
    b_number = int((w_and_p_num / 2)) # years before current
    a_number = int((w_and_p_num / 2 - 1)) # year after current

    year_of_interest_start=2010
    year_of_interest_end=2050

    #modelname = "NorESM1-M"
    #scenario = "RCP2p6_Final925"
    modelname = "gfdl-esm2m"
    scenario = "RCP8p5_cap_v000"
    # IPSL-CM5A-LR, HadGEM2-ES, GFDL-ESM2M, MIROC-ESM-CHEM, NorESM1-M


    discharge_base="/asrc/ecr/ariel/NEWS/Runs/RGISresults_v2/NorESM1-M_RCP8p5_Final925/USA/Discharge/Pristine/Static/Monthly/USA_Discharge_Pristine_Static_mTS"
    runoff_base="/asrc/ecr/ariel/NEWS/Runs/RGISresults_v2/NorESM1-M_RCP8p5_Final925/USA/Runoff/Pristine/Static/Monthly/USA_Runoff_Pristine_Static_mTS"
    output_file='/asrc/ecr/NEWS/tmp/gfdl-esm2m_rcp8p5_cap_v000'
    #Set the start and end year based on the years of interest and the averaging window
    # in order
    start_needed = year_of_interest_start - b_number #int((w_and_p_num / 2))
    end_needed = year_of_interest_end + a_number # int((w_and_p_num / 2 - 1))

    #cmd=['/asrc/ecr/NEWS/configurations/Link/Pr_PCA_WaterStats.sh',discharge_base,runoff_base,start_needed,end_needed,True]

    #proc = sp.Popen(cmd, stdout=sp.PIPE)
    #data1 = StringIO(bytearray(proc.stdout.read()).decode("utf-8"))
    #data = pd.read_csv(data1, sep='\t',
    #                         index_col=['PCA_REG'],
    #                         dtype={'PCA_REG' : 'int',
    #                                'Year' : 'int',
    #                                'Month' : 'int',
    #                                'Area' : 'float',
    #                                'Local' : 'float',
    #                                'Entering' : 'float',
    #                                'Re-entering' : 'float',
    #                                'Leaving' : 'float',
    #                                'Balance' : 'float',
    #                                'TAWR' : 'float'
    #                                })

    #input_file = (("/asrc/ecr/NEWS/MultiScenario/Scripts/%s_%s/TEMPmBudget_%s_%s.csv" % (
    #    modelname, scenario, modelname, scenario)))
    input_file = "/asrc/ecr/NEWS/MultiScenario/Environment/WaterAvailability/PIPPOmBudget_gfdl-esm2m_rcp8p5_cap_v000.csv"
    data = pd.read_csv(input_file, sep="\t", header=0, index_col="PCA_REG")
    PCAStats(modelname, scenario, data, w_and_p_num, b_number, a_number, output_file)
