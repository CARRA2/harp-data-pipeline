#!/usr/bin/env python
# Set total precipitation to zero if the minimum is not zero in the monthly means
# This is due to an issue with the grib packing, as described here:
# https://confluence.ecmwf.int/display/UDOC/Why+are+there+sometimes+small+negative+precipitation+accumulations+-+ecCodes+GRIB+FAQ
# We set the values to zero by hand to avoid confusion 
# This is only done for total precipitation

import os
import sys
import re
import eccodes as ecc
import numpy as np
import calendar

if len(sys.argv) < 4:
    print("Please provide input,output file,origin, yearmonth and number of fields")
    sys.exit(1)
else:
    infile = sys.argv[1]
    outfile = sys.argv[2]
    origin = sys.argv[3]
    yyyymm = sys.argv[4]
    #nf = sys.argv[5]

domain={"no-ar-ce": {"Nx": 789,"Ny" : 989}, "no-ar-cw":{"Nx": 1069,"Ny" : 1269},
       "no-ar-pa": {"Nx": 2869, "Ny": 2869}}
param_code = 228228 #total precipitation

gfile = open(infile)
nf=ecc.codes_count_in_file(gfile)
gfile.close()
print(f"input: {infile}")
print(f"output: {outfile}")
print(f"parameter code: {param_code}")
print(f"Domain: {origin}")
print(f"yearmonth: {yyyymm}")

if os.stat(infile).st_size==0:
    print(f"{infile} is empty!")
    sys.exit(1)


ikey = "param"
gfile = open(infile)
values={}
latdim=domain[origin]["Ny"]
londim=domain[origin]["Nx"]
print(f"lat and lon dimensions {latdim}, {londim}")
#get the number of dayys
year=int(yyyymm[0:4])
month=int(yyyymm[4:6])
ndays=calendar.monthrange(year, month)[1]

# packing all values here
values = np.zeros([latdim*londim], dtype=np.float32)
other_values=np.zeros([nf-1,latdim*londim], dtype=np.float32)
i=0
found_var=False
while True:
    msg = ecc.codes_grib_new_from_file(gfile)
    if msg is None:
        break
    #print(msg['param'])
    key = ecc.codes_get_long(msg, ikey)
    date = ecc.codes_get_long(msg, "date")
    if (key == param_code):
        print(f"Found key and input for {date}" )
        values[:] = ecc.codes_get_values(msg)
        found_var=True
    else:
        other_values[i,:] = ecc.codes_get_values(msg)
        i+=1
gfile.close()
if not found_var:
    print(f"{param_code} not found in {infile}!")
    sys.exit(1)
# find the places where the values are negative. This is just for testing
#find_values = np.argwhere(values < 0)
#print("before")
#print(find_values)
#print(values[0,259])

print(f"Setting all negative values to zero for {param_code}")
set_values = np.where(values >= 0, values , 0.)
find_values=np.argwhere(set_values < 0)

#print("after")
#print(find_values)
#print(set_values[0,259])

#clone one message, replace values later and keep everything else the same
gfile = open(infile)
print(f"opening {infile} again to clone")
other_msg=[] #np.zeros([nf-1], dtype=np.int64)
i=0
while True:
    msg_clone = ecc.codes_grib_new_from_file(gfile)
    #print(msg_clone)
    if msg_clone is None: break
    key = ecc.codes_get_long(msg_clone, ikey)
    if (key == param_code):
        print(f"Found key for cloning the output" )
        msg2 = ecc.codes_clone(msg_clone)
        #break
    else:
        get_msg = ecc.codes_clone(msg_clone)
        other_msg.append(get_msg)
        i+=1
gfile.close()

#write the output
with open(outfile,'wb') as f:
    ecc.codes_set_values(msg2, set_values)
    ecc.codes_write(msg2, f)
    for i in range(nf-1):
        ecc.codes_set_values(other_msg[i],other_values[i,:])
        ecc.codes_write(other_msg[i], f)

