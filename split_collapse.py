"""
Module description:
First pass row collapsing functions and procedure. 
Make into python script to run on cluster.

Details: 
This script aim to collapse raw finalised datafiles into Job indexed
datafiles. This is a first pass collapsing only, hence the collapsing 
functions (function that maps columns of values to a single value) are 
some what arbitrary and uninformed. 
Note that a single datafile is processed in MODULUS number of parallel
processes where MODULUS is defined in data_spliting.py.
Running on clusters, this script is ran as follow:
python split_data.py name data_directory_name output_directory_name array_id

which is submitted using sbatch 
sbatch --array=0-255 slurm_with_command_above.

array_id : The slurm environment variable SLURM_ARRAY_JOB_ID which is passed
to the script to inform which splited data to process.


"""

import numpy as np
import pandas as pd
import os
import csv
import itertools
import datetime
import sys


####################### 
# Manual stratification of columns into groups that can be processed 
# together.
######################

ALL_COLS = ['AllocCPUS', 'AllocGRES', 'AllocNodes', 'AllocTRES', 'Account', 
            'AssocID', 'AveCPU', 'AveCPUFreq', 'AveDiskRead', 'AveDiskWrite', 
            'AvePages', 'AveRSS', 'AveVMSize', 'BlockID', 'Cluster', 'Comment', 
            'ConsumedEnergy', 'ConsumedEnergyRaw', 'CPUTime', 'CPUTimeRAW', 'DerivedExitCode', 
            'Elapsed', 'Eligible', 'End', 'ExitCode', 'GID', 
            'Group', 'JobID', 'JobIDRaw', 'JobName', 'Layout', 
            'MaxDiskRead', 'MaxDiskReadNode', 'MaxDiskReadTask', 'MaxDiskWrite', 'MaxDiskWriteNode', 
            'MaxDiskWriteTask', 'MaxPages', 'MaxPagesNode', 'MaxPagesTask', 'MaxRSS', 
            'MaxRSSNode', 'MaxRSSTask', 'MaxVMSize', 'MaxVMSizeNode', 'MaxVMSizeTask', 
            'MinCPU', 'MinCPUNode', 'MinCPUTask', 'NCPUS', 'NNodes', 
            'NodeList', 'NTasks', 'Priority', 'Partition', 'QOS', 
            'QOSRAW', 'ReqCPUFreq', 'ReqCPUFreqMin', 'ReqCPUFreqMax', 'ReqCPUFreqGov', 
            'ReqCPUS', 'ReqGRES', 'ReqMem', 'ReqNodes', 'ReqTRES', 
            'Reservation', 'ReservationId', 'Reserved', 'ResvCPU', 'ResvCPURAW', 
            'Start', 'State', 'Submit', 'Suspended', 'SystemCPU', 
            'Timelimit', 'TotalCPU', 'UID', 'User', 'UserCPU', 
            'WCKey', 'WCKeyID']

EXTRA_COL = ['NJobSteps', 'Job']

# _uniq_to_job
UNIQ_TO_JOB_COL = ["Account", "AssocID", "Cluster", "GID", "Group",
                   "BlockID", "DerivedExitCode", "ReqMem", "ReqTRES", 
                   "Reservation", "ReservationId", "Priority", "Partition", 
                   "QOS", "QOSRAW", "Timelimit", "UID", 'User']
UNINFORMATIVE_COL = ['AllocGRES', 'Comment', 'ConsumedEnergy',
                     'ConsumedEnergyRaw', 'ReqCPUFreq','ReqCPUFreqMin',
                     'ReqCPUFreqMax', 'ReqCPUFreqGov', 'ReqGRES',
                     'Suspended', 'WCKey', 'WCKeyID']
# _flatten
FLATTEN_COL = ['Eligible', 'End', 'JobName', 'Layout', 
               'Start', 'State', 'Submit', 'MaxDiskReadNode',
               'MaxDiskReadTask', 'MaxDiskWriteNode', 'MaxDiskWriteTask', 'MaxPagesNode',
               'MaxPagesTask', 'MaxRSSNode', 'MaxRSSTask', 'MaxVMSizeNode',
               'MaxVMSizeTask', 'MinCPUNode', 'MinCPUTask', 'NodeList', 'ExitCode',
               'NJobSteps', 'Job']
# _ave
AVG_COL = ['AveCPUFreq', 'AveDiskRead', 'AveDiskWrite', 'AvePages', 'AveRSS', 'AveVMSize']

#_numeric_ave
NUM_AVG_COL = ["AveCPU","Reserved", "ResvCPU"]
#_max
MAX_COL = ['MaxDiskRead', 'MaxDiskWrite','MaxPages','MaxRSS', 'MaxVMSize']
# _sum
SUM_COL = ['AllocCPUS', 'AllocNodes', 'NTasks', 
           'ResvCPURAW', 'NCPUS', 'CPUTimeRAW', 
           'ReqNodes', 'ReqCPUS', 'NNodes', "Elapsed", "CPUTime", "SystemCPU", "UserCPU", "TotalCPU"]

# convert to seconds at the begining and then process 
DURATION_COL = ["Elapsed", "CPUTime", "AveCPU", "MinCPU", 
                   "Reserved", "ResvCPU", "SystemCPU", 
                   "UserCPU", "TotalCPU"]

TIME_COLS_LONG = ["Submit", "Start", "End", "Eligible"]
TIME_COLS_SHORT = ["Elapsed", "CPUTime", "AveCPU", "MinCPU", 
                   "Reserved", "ResvCPU", "SystemCPU", 
                   "UserCPU", "TotalCPU"]

# use manually written functions
MANUAL_COL = ['JobID', 'JobIDRaw', 'AllocTRES', 'MinCPU']

TIME_COLS = TIME_COLS_LONG + TIME_COLS_SHORT


#####################
# Global variables
#####################

DATADIR = "/Users/VLSCI/Desktop/lifesci_usage_project/notebooks/finalised_data/"
DATAFILES = ['final_avoca_data_python', 'final_barcoo_data_python', 
             'final_bruce_data_original', 'final_merri_data_python',
             'final_snowy_data_python', 'final_tambo_data_original']
DELIMITER = '|'
TIME_FMT_LONG = "%Y-%m-%dT%H:%M:%S"
TIME_FMT_SHORT_WITH_DAY = "%j-%H:%M:%S"
TIME_FMT_SHORT_WITHOUT_DAY = "%H:%M:%S"
BEGINING_OF_TIME = datetime.datetime.min

UNIT_CONVERSION = {'M':10**6, 'K':10**3}



COLLECTED_COLS = (UNIQ_TO_JOB_COL + UNINFORMATIVE_COL + FLATTEN_COL
                  + AVG_COL + NUM_AVG_COL + MAX_COL + SUM_COL + MANUAL_COL)


DIAGNOSTIC_OUTPUT = """Histogram of job group size:\n%s\n
aggregated dataframe shape: %s\n
Histogram of job group (duplicated only): %s\n\n"""

        

def aggregate(df):
    """
    1. convert all DURATION_COL in df to seconds
    2. add in the "Job" data field.
    3. add in the "NJobSteps" data field.
    4. Group by "Job" and then aggregate. 
    """
    sys.stdout.write("Converting durations\n")
    df_convert_duration(df)
    sys.stdout.write("Add job groups and jobsteps\n")
    add_job_group(df)
    add_njobsteps(df)
    job_group = df.groupby(by="Job")
    sys.stdout.write("Aggregating... \n")
    df_agg = job_group.aggregate(col_func_dict)
    return df_agg, job_group


def add_job_group(df, col="JobIDRaw"):
    """
    Add in the "Job" data field by taking the first dot separated
    value in JobIDRaw.
    Warning: in place operation
    """
    df.loc[:, 'Job'] = [str(jobidraw).split('.')[0] 
                        for jobidraw in df.loc[:, col]]
    return df

def add_njobsteps(df, col="JobIDRaw", job_col=None):
    """
    Add in the NJobSteps columns by making a histogram
    of the JobIDRaw count for each job. 
    Warning: in place operation
    """
    jobidraw_col = df.loc[:, col]
    if not job_col:
        jobid_list = list(map(lambda x:str(x).split('.')[0], jobidraw_col))
    else:
        jobid_list = df.loc[:, job_col]
    jobid_hist = make_hist(jobid_list)
    num_rows = df.shape[0]
    njobsteps = np.zeros(num_rows)
    for i in range(num_rows):
        njobsteps[i] = jobid_hist[jobid_list[i]]
    df.loc[:, "NJobSteps"] = njobsteps
    return 

def df_convert_duration(df):
    """
    Convert all columns in DURATION_COL that are recorded
    in time stamps strings to seconds
    """
    for col in DURATION_COL:
        for row in df.index:
            df.loc[row, col] = convert_duration(df.loc[row, col])
#        df.loc[:, col] = map(convert_duration, df.loc[:, col])
    return

def convert_duration(time_string):
    """
    Convert time strings into seconds. 
    Handling time stamps format:
    1. "%Y-%m-%dT%H:%M:%S"                                            
    2. "day-%H:%M:%S"                                        
    3. "%H:%M:%S"
    4. "%M:%S"
    """
    isnan = False
    try:
        isnan = np.isnan(time_string)
    except TypeError:
        pass
    if not isnan:
        if 'T' in time_string:
            t = datetime.datetime.strptime(time_string, TIME_FMT_LONG)
            delta = t - BEGINING_OF_TIME
            seconds = delta.total_seconds()
        else:
            if '-' in time_string:
                day, hms = time_string.split('-')
            else:
                day = 0
                hms = time_string
            seconds = int(day) * 24 * 3600 + hms_to_seconds(hms)
        return seconds
    else:
        return time_string

def hms_to_seconds(hms):
    """
    Convert HH:MM:SS or MM:SS into seconds
    """
    time_info = list(map(float, hms.split(':')))
    multipliers = [1.0, 60.0, 3600.0]
    seconds = 0
    for val1, val2 in zip(time_info[::-1], multipliers):
        seconds += val1 * val2
    return seconds
    
#def _parse_time_string(time_string):
#    if 'T' in string:
#        return datetime.datetime.strptime(time_string, TIME_FMT_LONG)
#
#        
#def _parse_time_string(time_string):
#    """
#    Parse the 3 types of time string present in data. 
#    Types Format Example
#    LONG %Y-%m-%dT%H:%M:%S 2013-09-05T22:59:16
#    SHORT_WITH_DAY %d-%H:%M:%S 2-00:30:20
#    SHORT_WITHOUT_DAY %H:%M:%S 00:03:20
#    """
#    if 'T' in time_string:
#        return datetime.datetime.strptime(time_string, TIME_FMT_LONG)
#    elif '-' in time_string:
#        return datetime.datetime.strptime(time_string, TIME_FMT_SHORT_WITH_DAY)
#    else:
#        return datetime.datetime.strptime(time_string, TIME_FMT_SHORT_WITHOUT_DAY)

def _flatten(col):
    """
    Series (val1, val2, val3) -> list('val1', 'val2', 'val3')
    """
    return ', '.join(map(str, col.dropna()))

def _ave(col):
    """
    Take average of non-nan values handling
    Mega and Kilo prefix conversion. 
    Handling string inputs such as 
    '0.01M'
    '1K'
    '3.03'
    """
    col_values = col.dropna()
    if len(col_values) == 0:
        return 0
    total = 0.0
    length = 0.0
    for val in col_values:
        length += 1
        val = str(val)
        if val[-1].isalpha():
            total += float(val[:-1]) * UNIT_CONVERSION[val[-1]]
        else:
            total += float(val)
    return total / length

def _num_ave(col):
    """
    Take average of non-nan numerial values
    """
    return np.mean(col.dropna())

def _max(col):
    """
    Take maximum of non-nan values handling
    Mega and kilo conversion.
    """
    col_values = col.dropna()
    if len(col_values) == 0:
        return 0
    max_val = 0
    for val in col_values:
        val = str(val)
        if val[-1].isalpha():
            numeric_val = float(val[:-1]) * UNIT_CONVERSION[val[-1]]
        else:
            numeric_val = float(val)
        if numeric_val > max_val:
            max_val = numeric_val
    return max_val


def _sum(col):
    return np.sum(col.dropna())

def JobID(col):
    col = col.dropna()
    return str(col.iloc[0]).split('.')[0].split('_')[0]

def JobIDRaw(col):
    col = col.dropna()
    return str(col.iloc[0]).split('.')[0]
    
def AllocTRES(col):
    return _flatten(col)

def MinCPU(col):
    return np.min(col.dropna())
    
def uniq_to_job(col):
    col_values = col.dropna()
    if len(col_values) == 0:
        return None
    else:
        return col_values.iloc[0]
        
def make_hist(source):
    """
    Given an iterable, return a histogram, i.e.
    a dictionary of {uniq_items : counts}
    """
    hist = {}
    for e in source:
        if e not in hist:
            hist[e] = 0
        hist[e] += 1
    return hist

def duplicates_hist(lst):                                                      
    """                                                                        
    Return a histogram of lst but                                              
    only of duplicated items in lst.                                           
    """                                                                        
    seen = set()                                                               
    duplicates = {}                                                            
    for item in lst:                                                           
        if item in seen:                                                       
            if item in duplicates:                                             
                duplicates[item] += 1                                          
            else:                                                              
                duplicates[item] = 2 # already seen once                       
        else:                                                                  
            seen.add(item)                                                     
    return duplicates                    




#################
# Make a dictionary of column_name -> collpase function map
#################
col_func_dict = {}
for col in ALL_COLS + EXTRA_COL:
    if col in UNIQ_TO_JOB_COL:
        col_func_dict[col] = uniq_to_job
    elif col in UNINFORMATIVE_COL + FLATTEN_COL:
        col_func_dict[col] = _flatten
    elif col in AVG_COL:
        col_func_dict[col] = _ave
    elif col in NUM_AVG_COL:
        col_func_dict[col] = _num_ave
    elif col in SUM_COL:
        col_func_dict[col] = _sum
    elif col in MAX_COL:
        col_func_dict[col] = _max
    elif col == 'JobID':
        col_func_dict[col] = JobID
    elif col == 'JobIDRaw':
        col_func_dict[col] = JobIDRaw
    elif col == 'AllocTRES':
        col_func_dict[col] = AllocTRES
    elif col == 'MinCPU':
        col_func_dict[col] = MinCPU
    else:
        print("Warning :%s"%col)


def main():
    name, datadir, outdir, array_id = sys.argv[1:5]
    infilename = os.path.join(datadir, "split_%s_%s" %(name, array_id))
    outfilename = os.path.join(outdir, "%s" % array_id)
    process_file(infilename, outfilename)
    return 0
    
    
def process_file(infilename, outfilename):
    """
    1. Read in dataframe
    2. run aggregate. see documentation for aggregate()
    3. sort columns alphabetically
    4. output to outfile
    5. print diagnostic information
    """
    sys.stdout.write("Processing: %s\n"%infilename)
    df = pd.read_csv(infilename, 
            delimiter=DELIMITER, 
            names=ALL_COLS, 
            na_values=["Unknown", np.nan, "INVALID", ''])
    df_agg, job_group = aggregate(df)
    df_agg.sort_index(axis=1, inplace=True)
    sys.stdout.write("Writing data to %s\n"%outfilename)
    df_agg.to_csv(outfilename, sep=DELIMITER, index=False, header=False)
    diagnostic_info = [make_hist(job_group.size()), 
                       df_agg.shape,
                       duplicates_hist(df_agg.loc[:, "Job"])]
    diagnostic_output = DIAGNOSTIC_OUTPUT % tuple(map(str, 
                                                      diagnostic_info))
    sys.stdout.write(diagnostic_output)



if __name__ == "__main__":
    main()
