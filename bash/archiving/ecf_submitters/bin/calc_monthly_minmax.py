# Quick and dirty script to read the data for all days
# and select the min and max

import sys
import re
import eccodes as ecc
import numpy as np
import calendar

if len(sys.argv) < 4:
    print("Please provide input,output file and parameter code")
    sys.exit(1)
else:
    infile = sys.argv[1]
    outfile = sys.argv[2]
    param_code = int(sys.argv[3])
    origin = sys.argv[4]
    yyyymm = sys.argv[5]

domain={"no-ar-ce": {"Nx": 789,"Ny" : 989}, "no-ar-cw":{"Nx": 1069,"Ny" : 1269},
        "no-ar-pa": {"Nx": 2869, "Ny": 2869}}


print(f"input: {infile}")
print(f"output: {outfile}")
print(f"parameter code: {param_code}")
print(f"Domain: {origin}")
print(f"yearmonth: {yyyymm}")

#max_params=[201,260646,260647]
max_params=[201] #only one in CARRA2
ikey = "param"
gfile = open(infile)

#if msg['param'] == self.indicatorOfParameter and msg['level'] == self.level and msg['levelType'] == self.levelType and msg["timeRangeIndicator"] == self.timeRangeIndicator:
values={}
latdim=domain[origin]["Ny"]
londim=domain[origin]["Nx"]
print(f"lat and lon dimensions {latdim}, {londim}")
#get the number of dayys
year=int(yyyymm[0:4])
month=int(yyyymm[4:6])
ndays=calendar.monthrange(year, month)[1]

# packing all values here

values = np.zeros([ndays, latdim*londim], dtype=np.float32)
i=0

#with ecc.GribFile(infile) as gfile:
while True:
    msg = ecc.codes_grib_new_from_file(gfile)
    if msg is None:
        break
    #print(msg['param'])
    
    key = ecc.codes_get_long(msg, ikey)
    date = ecc.codes_get_long(msg, "date")
    if (key == param_code):
        print(f"Found key and input for {date}" )
        values[i,:] = ecc.codes_get_values(msg)
        i+=1
gfile.close()

if param_code in max_params:
    print(f"Calculating the maximum monthly value for {param_code}")
    month_value = np.max(values, axis=0)
else:
    print(f"Calculating the minimum monthly value for {param_code}")
    month_value = np.min(values, axis=0)

#clone one message, replace values later and keep everything else the same
gfile = open(infile)
while True:
    msg_clone = ecc.codes_grib_new_from_file(gfile)
    #print(msg_clone)
    if msg_clone is None: break
    key = ecc.codes_get_long(msg_clone, ikey)
    if (key == param_code):
        print(f"Found key for cloning the output" )
        msg2 = ecc.codes_clone(msg_clone)
        break
gfile.close()


with open(outfile,'wb') as test:
    ecc.codes_set_values(msg2, month_value)
    ecc.codes_write(msg2, test)


