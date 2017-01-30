# Analytics
This project aims to identify resource usage patterns at VLSCI.
By reviewing historical use of VLSCI resources the intent is to identify types of research and resource usage that are well supported by VLSCI, those that could be better supported elsewhere and those that need better support.  This information will be used to inform resource planning and management.

The current data sources are:

- Compute logs from Karaage and SLURM databases
- Storage logs GPFS and HSM storage daily audits, in the Karaage database
- Project data collected from project applications, LSCC collaborations and previous data collection activities

## Data sources
### Compute resources
The compute resources consist of x86 machines (in chronological order); Bruce (SGI), Merri (IBM), Barcoo (IBM), and Snowy (Lenovo), and the IBM BlueGene machines; Tambo and Avoca.

Job logs are stored in Karaage and SLURM databases.  Karaage job data is collected daily and summarises jobs that completed on the day.  The SLURM database has more comprehensive job information, but was implemented on each cluster at different times (as summarised in the following table)

Machine | First Karaage <br/> Job Start Date | First SLURM <br/>Job Start Date
--------|---------:|--------------:
Avoca | 2012-06-21 | 2012-06-18
Barcoo | 2013-08-01 | 2013-07-26
Bruce | 2010-03-11 | 2013-09-05
Merri | 2010-09-10 | 2013-08-08
Snowy | 2015-10-05 | 2015-08-26
Tambo | 2010-07-29 | 2011-06-16

The starting point for Compute resource data analysis is to use the SLURM data.  This means that the earliest jobs on the older machines are not considered.  For planning purposes, the more recent jobs are assumed to be a better indicator or current needs.

### Storage resources
The GPFS and HSM storage systems provide commands for querying the meta-data for the file systems.  To collect historical usage data a daily audit log is run, and the results stored in the Karaage database.

The daily audits were implimented for each file system starting at the following dates.

File system | First audit date
------------|----------------:|
HSM | 2014-04-23
Scratch | 2013-12-11
VLSCI | 2013-12-11

