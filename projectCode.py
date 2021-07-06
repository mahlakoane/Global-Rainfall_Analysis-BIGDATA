import numpy as np
from netCDF4 import Dataset
from mpi4py import MPI
import statistics
import time
import pymp
import sys


def mergeSort(nlist):
    
    if len(nlist)>1:
        mid = len(nlist)//2
        lefthalf = nlist[:mid]
        righthalf = nlist[mid:]

        mergeSort(lefthalf)
        mergeSort(righthalf)
        i=j=k=0       
        while i < len(lefthalf) and j < len(righthalf):
            if lefthalf[i] < righthalf[j]:
                nlist[k]=lefthalf[i]
                i=i+1
            else:
                nlist[k]=righthalf[j]
                j=j+1
            k=k+1

        while i < len(lefthalf):
            nlist[k]=lefthalf[i]
            i=i+1
            k=k+1

        while j < len(righthalf):
            nlist[k]=righthalf[j]
            j=j+1
            k=k+1


comm = MPI.COMM_WORLD
rank = comm.Get_rank() # equivalsent of thread_id
size = comm.Get_size() # equivalent of num_threads

manager = 0
Q2N = 0
Q2S = 0
Q1S = 0
Q1N = 0
Q3S = 0
Q3N = 0
max_N = 0
max_S =0
min_N = 0
min_S = 0

NGlobal_max = 0
NGlobal_min = 0
SGlobal_max = 0
SGlobal_min = 0

Q1NGlobal = []
Q1SGlobal = []
Q3NGlobal = []
Q3SGlobal = []

Northern = []
Southern = []
Northern_Hemi = []
Southern_Hemi = []
SouthernHemi_Precip=[]
NorthernHemi_Precip=[]
numthreads  = int(sys.argv[1])
start_year = int(sys.argv[2])
end_year = int(sys.argv[3])
Dataset_version = str(sys.argv[4])
VersionAndStep = str(sys.argv[5])
years = end_year - start_year


seconds_start = MPI.Wtime()
# Node Specific Instructions
for year in range(rank,years,size): # round robin distribution of years to nodes.
    year_num = start_year+year
    print(year_num)
    fn = f'/data/full_data_daily_{Dataset_version}/full_data_daily_{VersionAndStep}_{year_num}.nc'    # the year can be changed at will.
    ds = Dataset(fn)
        
    time = ds.variables['time'][:]
    latitude = ds.variables['lat'][:]
    longitude = ds.variables['lon'][:]
    precipitation = np.array(ds.variables['precip'])

    half = int(len(latitude)/2)
    full = int(len(latitude))


    southern_lat_indices = []
    time_indices = []
    longitude_indices = []

    for index,lat in enumerate(latitude):
        if (lat < 0):
         Southern_Hemi.append(lat)
         southern_lat_indices.append(index)

    for index,tm in enumerate(time):
        time_indices.append(index)

    for index,lon in enumerate(longitude):
        longitude_indices.append(index)
 
    seconds1 = MPI.Wtime()
    with pymp.Parallel(numthreads) as p:
        for tm in range(p.thread_num,len(time),p.num_threads):  
            for lat in southern_lat_indices: # stops at half-1,89.
                for lon in longitude_indices:
                    if precipitation[tm,lat,lon] == -9999.0 or precipitation[tm,lat,lon] == 0 :
                        continue
                    else:
                        SouthernHemi_Precip.append(precipitation[tm,lat,lon])
                    if precipitation[tm,lat+half,lon] == -9999.0 or precipitation[tm,lat+half,lon] == 0 :
                        continue
                    else:
                        NorthernHemi_Precip.append(precipitation[tm,lat+half,lon])
    seconds2 = MPI.Wtime()

    totalTime = seconds2 - seconds1
    print("Threading time :", totalTime)
        
    seconds1 = MPI.Wtime()
    mergeSort(SouthernHemi_Precip) 
    mergeSort(NorthernHemi_Precip)
    #The Median

    current_medianN = np.percentile(NorthernHemi_Precip,50)
    current_medianS = np.percentile(SouthernHemi_Precip,50)
    Q2S += current_medianS
    Q2N += current_medianN
    
    
    

    #The Lower Quartile
    currentQ1N = np.percentile(NorthernHemi_Precip,25)
    currentQ1S = np.percentile(SouthernHemi_Precip,25)
    if currentQ1S<current_medianS:
        Q1S += currentQ1S
    if currentQ1N<current_medianN:
        Q1N += currentQ1N

    #The Upper Quartile
    currentQ3N = np.percentile(NorthernHemi_Precip,75)
    currentQ3S = np.percentile(SouthernHemi_Precip,75)
    if currentQ3S > current_medianS:
        Q3S += currentQ3S
    if currentQ3N > current_medianN:
        Q3N += currentQ3N

    #min and max for  Northern_Hemi
    if max_N < max(NorthernHemi_Precip):
        max_N = max(NorthernHemi_Precip)
    if min_N > min(NorthernHemi_Precip):
        min_N = min(NorthernHemi_Precip)

    #min and max for  Southern_Hemi
    if max_S < max(SouthernHemi_Precip):
        max_S = max(SouthernHemi_Precip)
    if min_S > min(SouthernHemi_Precip):
        min_S = min(SouthernHemi_Precip)
    


    
#Global communication - Directives for each node
Southern = comm.gather(Q2S,manager)
Northern = comm.gather(Q2N,manager)
NGlobal_max = comm.gather(max_N,manager)
NGlobal_min = comm.gather(min_N,manager)
SGlobal_max = comm.gather(max_S,manager)
SGlobal_min = comm.gather(min_S,manager)
Q1NGlobal = comm.gather(Q1N,manager)
Q1SGlobal = comm.gather(Q1S,manager)
Q3NGlobal = comm.gather(Q3N,manager)
Q3SGlobal = comm.gather(Q3S,manager)

    
    
#Master
if rank == manager:  #all results are worked out and presented here.
    Southern = np.array(Southern)
    Northern = np.array(Northern)
    Q1NGlobal = np.array(Q1NGlobal)
    Q1SGlobal = np.array(Q1SGlobal)
    Q3NGlobal = np.array(Q3NGlobal)
    Q3SGlobal = np.array(Q3SGlobal)

    print("Southern hemisphere max :",max(SGlobal_max))
    print("Southern hemisphere min :",min(SGlobal_min))
    print("Northern hemisphere max :",max(NGlobal_max))
    print("Northern hemisphere min : ",min(NGlobal_min))
  
    print("Southern Hemisphere median : ",(sum(Southern)/years))
    print("Northern Hemisphere median : ",(sum(Northern)/years))
    
    half_median = years*0.5
    print("Northern Hemisphere Q1 : ",(sum(Q1NGlobal)/half_median))
    print("Northern Hemisphere Q3 : ",(sum(Q3NGlobal)/half_median))
    print("Southern Hemisphere Q1 : ",(sum(Q1SGlobal)/half_median))
    print("Southern Hemisphere Q3 : ",(sum(Q3SGlobal)/half_median))


seconds_end = MPI.Wtime()
totalTime = seconds_end - seconds_start
print("Total time is ", totalTime)
