## Usage Analytics
# Get primary measures for first pass stats

def get_filenames():
    # data sets
    '''
    final_avoca_data_python
    final_barcoo_data_python
    final_bruce_data_original
    final_merri_data_python
    final_snowy_data_python
    final_tambo_data_original
    '''

    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("infile")
    parser.add_argument("outfile")
    args = parser.parse_args()

    infile = args.infile  #'../finalised_data/final_merri_data_python'
    outfile = args.outfile  #'merri_jobs.csv'
    return infile, outfile


import pandas as pd
import numpy as np
import datetime

#######################
# Manual stratification of columns into groups that can be processed
# together.
# See https://slurm.schedmd.com/sacct.html for column details
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

## Cols to use
## See https://docs.google.com/document/d/1fINxu8ddsaaN32fC8a_SpvQVzewQiMK5wPV-Qx7PeqI
USE_COLS = ['Account', 'AveDiskRead', 'AveDiskWrite', 'MaxDiskRead', 'MaxDiskWrite', 'Cluster', 'CPUTimeRAW',
            'Elapsed', 'Eligible', 'End', 'Group', 'JobID', 'JobIDRaw', 'NCPUS', 'NNodes',
            'NTasks', 'ReqMem', 'Start', 'State', 'Submit', 'SystemCPU',
            'Timelimit', 'TotalCPU', 'User', 'UserCPU',
            ]

## cols to save, including derived fields
OUT_COLS = ['Cluster', 'ParentJobID', 'Account', 'Group', 'User',
            'NCPUS', 'NNodes', 'NTasks', 'Timelimit',
            'Start', 'Submit', 'Elapsed', 'Eligible', 'End',
            'SystemCPU', 'TotalCPU', 'UserCPU', 'CPUTimeRAW', 'SU',
            'TotalReqMemMB', 'ContiguousReqMemMB', 'ReqMemType',
            'TotalDiskReadMB', 'TotalDiskWriteMB', 'MaxDiskReadMB/s', 'MaxDiskWriteMB/s',
            'State', 'JobState', 'JobSteps']


## converters

def nan_clean(val):
    ''' returns 0 if value is nan or raises a Type error
    '''
    isnan = False
    try:
        isnan = np.isnan(val)
    except TypeError:
        pass
    if not isnan and val:
        return val
    return 0

# NOTE: assumes val can be cast as float!
def nan_to_int(val):
    return int(round(float(nan_clean(val))))

UNIT_CONVERSION = {'G':1024.0, 'M':1.0, 'K':1.0/1024.0}
def to_Mbytes(val):
    val = str(nan_clean(val))
    if val[-1].isalpha():
        return float(val[:-1]) * UNIT_CONVERSION[val[-1]]
    return float(val)  ## assume no units means MByte???

TIME_FMT_LONG = "%Y-%m-%dT%H:%M:%S"
TIME_FMT_SHORT_WITH_DAY = "%j-%H:%M:%S"
TIME_FMT_SHORT_WITHOUT_DAY = "%H:%M:%S"
BEGINING_OF_TIME = datetime.datetime.min
def convert_duration(time_string):
    """
    Convert time strings into seconds.
    Handling time stamps format:
    1. "%Y-%m-%dT%H:%M:%S"
    2. "day-%H:%M:%S"
    3. "%H:%M:%S"
    4. "%M:%S"
    """
    time_string = str(nan_clean(time_string))
    if 'T' in time_string:
        try:
            t = datetime.datetime.strptime(time_string, TIME_FMT_LONG)
        except ValueError:
            t = BEGINING_OF_TIME
        delta = t - BEGINING_OF_TIME
        seconds = delta.total_seconds()
    else:
        if '-' in time_string:
            day, hms = time_string.split('-')
        else:
            day = 0
            hms = time_string
        seconds = int(day) * 24 * 3600 + hms_to_seconds(hms)
    return int(seconds)

def hms_to_seconds(hms):
    """
    Convert HH:MM:SS or MM:SS into seconds
    """
    try:
        time_info = list(map(float, hms.split(':')))
    except ValueError:
        time_info = [0,0]
    multipliers = [1, 60, 3600]
    seconds = 0
    for val1, val2 in zip(time_info[::-1], multipliers):
        seconds += val1 * val2
    return int(seconds)

# To get total memory, only want requested mem and cores or nodes from top step
def split_req_mem(val):
    is_node = None
    try:
        num = val[:-1]
        is_node = val[-1]
    except (KeyError, TypeError):
        pass
    num = to_Mbytes(num)
    if is_node == 'c':
        return num, 'core'
    elif is_node == 'n':
        return num, 'node'
    else:
        return 0, ''


# ## Read and convert select columns from CSV file
# ### NOTE conversion takes a long time -- not sure if it's better to read then convert, or convert during read

def get_raw_data(infile=None):
    #%%time
    DELIMITER = '|'
    df = pd.read_csv(infile,
                delimiter=DELIMITER,
                header=0,
                usecols=USE_COLS,
                converters={# match Karaage case
                            'Account': str.upper,
                            'Group': str.upper,
                            # Byte units
                            'AveDiskRead': to_Mbytes,
                            'AveDiskWrite': to_Mbytes,
                            'MaxDiskRead': to_Mbytes,
                            'MaxDiskWrite': to_Mbytes,
                            # date-time and duration
                            'Elapsed': convert_duration,
                            'Eligible': convert_duration,
                            'End': convert_duration,
                            'Start': convert_duration,
                            'Submit': convert_duration,
                            'SystemCPU': convert_duration,
                            'Timelimit': convert_duration,
                            'TotalCPU': convert_duration,
                            'UserCPU': convert_duration,
                            # integers
                            'NCPUS': nan_to_int,
                            'NNodes': nan_to_int,
                            'NTasks': nan_to_int,
                           },
                dtype={# hint for data import
                       'ReqMem': 'object',
                       'JobID': 'object',
                       'JobIDRaw': 'object' },
                na_values=["Unknown", np.nan, "INVALID", ''],
                keep_default_na=False)
    return df


# ## Create new columns INPLACE
def add_step_fields(df):
    #df['ParentJobID'] = (df['JobID'].str.split('.').str[0]).astype('uint32')  # get the string (number) before the '.'
    ## NOTE this will treat parts of job arrays as separate ParentJobIDs
    # will include ParentJobIDs like 349349_183
    df['ParentJobID'] = df['JobID'].str.split('.').str[0]  # get the string before the '.'

    ## NOTE: NTasks can be zero, but, by definition;
    # AveDiskRead
    # Average number of bytes read by all tasks in job.
    # AveDiskWrite
    # Average number of bytes written by all tasks in job.
    #
    # so guessing can't have AveDiskRead if NTasks == 0 ???
    df['TotalDiskReadMB'] = df[['AveDiskRead', 'NTasks']].prod(axis=1)

    df['TotalDiskWriteMB'] = df[['AveDiskWrite', 'NTasks']].prod(axis=1)

    # Estimate max data rate per task
    # NOTE this is MaxDataRead / Elapsed time of task
    # so IS NOT the I/O rate
    # ** not sure if it is a meaningfull measure
    df['MaxDiskReadMB/s'] = np.where(df['Elapsed'] > 0, df['TotalDiskReadMB'].div(df['Elapsed'], axis='index'), 0)

    df['MaxDiskWriteMB/s'] = np.where(df['Elapsed'] > 0, df['TotalDiskWriteMB'].div(df['Elapsed'], axis='index'), 0)


# What times to collect, elapsed, UserCPU, SystemCPU (TotalCPU = User + System)
# Diff between CPUTime and TotalCPU indicates IO time?

## Service Unit conversion from CPU hour
MACHINE_SU = {'bruce': 1.0,
              'tambo': 0.25,
              'merri': 1.0,
              'barcoo': 1.25,
              'avoca': 0.25,
              'snowy': 1.25}

# ### Group by Cluster and JobID, or just JobID assuming aggregation is applied per cluster?
def do_aggregation(df):
    grouped = df.groupby(['Cluster', 'ParentJobID'])

    gf = grouped.first()

    gf['JobSteps'] = grouped['ParentJobID'].count()

    ## aggregate
    '''
    ARGH took me all day to trace weird wrong ordering in output to this
    gf[['NTasks', 'TotalDiskReadMB', 'TotalDiskWriteMB', 'MaxDiskReadMB/s', 'MaxDiskWriteMB/s']] = grouped.agg(

    Works in 1.19.2 but not 1.18.1
    Solution assign in order returned by grouping
    '''
    agg_ordered = grouped.agg(
        {'NTasks': np.max,
         'TotalDiskReadMB': np.sum,
         'TotalDiskWriteMB': np.sum,
         'MaxDiskReadMB/s': np.max,
         'MaxDiskWriteMB/s': np.max })
    gf[agg_ordered.columns] = agg_ordered

    # convert mem to MB and classify as 'core' or 'node' (or '')
    # NOTE: ReqMem represents the largest contiguous memory requested
    gf['ContiguousReqMemMB'], gf['ReqMemType'] = zip(*gf['ReqMem'].apply(split_req_mem))

    # Total requested memory in MByte based on core or node, as per definition;
    # ReqMem
    # Minimum required memory for the job, in MB.
    # A 'c' at the end of number represents Memory Per CPU,
    # a 'n' represents Memory Per Node.
    # Note: This value is only from the job allocation, not the step.
    gf['TotalReqMemMB'] = np.where(gf['ReqMemType']=='core',
                                   gf[['ContiguousReqMemMB', 'NCPUS']].prod(axis=1),
                                   gf[['ContiguousReqMemMB', 'NNodes']].prod(axis=1))

    #gf.loc['bruce', 1733656]

    # State includes extra information, so unify
    gf['JobState'] = gf['State'].str.split(' ').str[0]

    # add group lables back as columns
    gf = gf.reset_index(level=['Cluster', 'ParentJobID'])

    # now add SU
    gf['SU'] = gf['CPUTimeRAW'] * gf['Cluster'].map(MACHINE_SU) / 3600

    return gf


def save_aggregated(outfile, gf):
    return gf[OUT_COLS].to_csv(outfile, index=False)   #TODO: should test return ?


def main(infile=None, outfile=None):
    df = get_raw_data(infile)
    add_step_fields(df)
    gf = do_aggregation(df)
    save_aggregated(outfile, gf)


if __name__ == "__main__":
    infile, outfile = get_filenames()
    main(infile, outfile)
