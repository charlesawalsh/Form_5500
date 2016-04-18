#==========================================
'''
Programmer: Shiqi Zheng
Date: 9/19/14
Last modified: 9/25/14

Description: Program for collecting and pruning data from 
             form 5500 and schedules. 
'''
#==========================================

#Library
import zipfile
import pandas
import os
import time
import operator
import csv
import sys
import linecache

#binary_search
def binary_search(seq, ID):
    min = 0
    max = len(seq) - 1
    while True:
        if max < min:
            return -1
        m = (min + max) // 2
        if seq[m] < ID:
            min = m + 1
        elif seq[m] > ID:
            max = m - 1
        else:
            return m

#=================Working directory======================
folder=[2009,2010,2011]
#folder=[2012]
size=len(folder)
#print size
for l in range(size):
    #print i
    #break
    os.chdir('C:\\Users\\Shiqiz\\Desktop\\data\\'+str(folder[l]))
    print("Processing year %i" %(folder[l]))
#==================Write form 5500 to a different .csv file and sort Schedule I and H=================================    
    #file locations
    schI_path=os.getcwd()+("\\F_SCH_I_%i_latest.csv"%(folder[l]))
    schH_path=os.getcwd()+("\\F_SCH_H_%i_latest.csv"%(folder[l]))
    form_path=os.getcwd()+("\\f_5500_%i_latest.csv"%(folder[l]))
    '''
    #file locations
    schI_path=os.getcwd()+("\\F_SCH_I_%i_latest.csv"%(folder[l]))
    schH_path=os.getcwd()+("\\F_SCH_H_%i_latest.csv"%(folder[l]))
    form_path=os.getcwd()+("\\f_5500_%i_latest.csv"%(folder[l]))
    '''
    #new file locations
    sortedH=os.getcwd()+"\\sortedH.csv" #open a writing file
    sortedI=os.getcwd()+"\\sortedI.csv"
    form5500=os.getcwd()+"\\form5500.csv"

    #open csv writer
    File=open(sortedH,'wb')
    writer=csv.writer(File,delimiter=',')
    SchI=open(sortedI,'wb')
    writer2=csv.writer(SchI,delimiter=',')
    form=open(form5500,'wb')
    writer3=csv.writer(form,delimiter=',')

    data5500 = csv.reader(open(form_path),delimiter=',') #read in file

    for line in data5500:
        # print line
        #Change variable names
        line[1]="PLAN_YEAR_BEGIN_DATE"
        line[16]="PLAN_NUM"
        line[43]="SPONS_EIN"
        line[71]="PARTCP_ACCOUNT_BAL_CNT"
        form.write(','.join(line)+'\n')
        #print line[71]
        break

    for row in data5500: #write file
        writer3.writerow(row)

        #sort schedule H
    dataH = csv.reader(open(schH_path),delimiter=',')
    writer.writerow(dataH.next()) #write header
    sortedlist = sorted(dataH, key=operator.itemgetter(0)) #sort by first column
    for row in sortedlist: #write content
        writer.writerow(row)

        #sort schedule I
    dataI = csv.reader(open(schI_path),delimiter=',')
    writer2.writerow(dataI.next())
    sortedlistI = sorted(dataI, key=operator.itemgetter(0))
    for row in sortedlistI:
        writer2.writerow(row)

    #close files
    File.close()
    SchI.close()
    form.close()


#==================Prune schedules=================================

    scheduleH=os.getcwd()+"\\sortedH.csv"
    scheduleI=os.getcwd()+"\\sortedI.csv"
    H=os.getcwd()+"\\H.csv"
    I=os.getcwd()+"\\I.csv"


    schH=open(scheduleH,'rb')
    readerH=csv.reader(schH)
    schI=open(scheduleI,'rb')
    readerI=csv.reader(schI)
    #reader2.next()
    Hfile=open(H,'wb')
    writerH=csv.writer(Hfile)
    Ifile=open(I,'wb')
    writerI=csv.writer(Ifile)


    for i, row in enumerate(readerH):
        if (i==0):
            writerH.writerow([row[0],row[29],row[34],row[35],row[60],row[65],
                            row[66],row[67],row[68],row[69],row[95], 
                            row[100],row[49]]+["PARTCP_LOANS_IND"]+["TOT_PLAN_TRANSFERS_AMT"])                
        else:
                
            if (row[49]!=""):
                    x=1
            else:
                    x=2
                        
            writerH.writerow([row[0],row[29],row[34],row[35],row[60],row[65],
                            row[66],row[67],row[68],row[69],row[95], 
                            row[100],row[49]]+[x]+[0.01])                  
            #print "written"

    Hfile.close()

    for row in readerI:  
       
            writerI.writerow([row[0],row[5],row[6],row[7],row[8],row[9],
                            row[10],row[11],row[12],row[13],row[15], 
                            row[17],row[34],row[33],row[24]])
            #print "written"

    Ifile.close()



    schH.close()
    schI.close()


#==================Match form 5500 to schedule by ACK_ID =================================

    #Files
    form_path=os.getcwd()+"\\form5500.csv"
    schH_path=os.getcwd()+"\\H.csv"
    schI_path=os.getcwd()+"\\I.csv"

    #Open files
    form5500=open(form_path,'r')
    schI=open(schI_path, 'r')
    schI.readline()
    schH=open(schH_path, 'r')

    #d=os.path.split(form_path)[0] #get the directory location


    merged_5500=open(os.getcwd()+"\\merged.csv", 'w') #open a writing file
    merged_5500.write(form5500.readline()[:-1]+','+schH.readline()) # Write the first row

    #read files
    data=form5500.readlines()
    data1=schI.readlines()
    data2=schH.readlines()

    #print data[1]
    #data_I=schI.readlines()
    ListI=[]
    ListH=[]

    #print line     
    for p, line in enumerate(data1):
        ListI+=[line.split(',')[0]]
        #print ListI
        #break
    for j, line in enumerate(data2):
        ListH+=[line.split(',')[0]]
        #print ListH
        #break
    #print(len(ListI),len(ListH))

    for k, line in enumerate(data): #loop through the entire file
        ID5500=line.split(',')[0] #get ID
        index=binary_search(ListI, ID5500)
        if(index!=-1):
            #print linecache.getline(schI_path,index+1)
            data[k]=data[k][:-1]
            merged_5500.write(data[k]+','+linecache.getline(schI_path,index+2))
            #print("found")
        else:
            index=binary_search(ListH, ID5500)
            if(index!=-1):
                #linecache.getline(schH_path,index+1)
                data[k]=data[k][:-1]
                merged_5500.write(data[k]+','+linecache.getline(schH_path,index+2))
                #print("found")
            #else:
                #print("%s has no match" %(ID5500))
     

    form5500.close()
    schI.close()
    schH.close()
    merged_5500.close()

#==================Merge form 5500 and short form=================================

    merged=os.getcwd()+"\\merged.csv"
    sf=os.getcwd()+("\\f_5500_sf_%i_latest.csv" %(folder[l]))
    merged_sf5500=os.getcwd()+"\\merged_sf5500.csv"



    form5500=open(merged,'rb')
    reader0=csv.reader(form5500)
    shortform=open(sf,'rb')
    reader1=csv.reader(shortform)
    reader1.next()

    writefile=open(merged_sf5500,'wb')
    writer0=csv.writer(writefile)



    '''
    #For 2012 only 
    for row in reader0:
        
                writer0.writerow([row[0],row[1],row[5],row[7],row[16],row[43],row[59],
                        row[62],row[63],row[70],row[76],row[77],row[80],
                        row[81],row[125],row[126], row[127],row[128],row[129],row[130],
                        row[131],row[132],row[133],row[134],row[135],row[136],row[137],row[138]])  
     '''                            
                      
    for row in reader0:
            
                    writer0.writerow([row[0],row[1],row[5],row[7],row[16],row[43],row[59],
                            row[62],row[63],row[70],row[76],row[77],row[80],
                            row[81],row[106], row[107],row[108],row[109],row[110],
                            row[111],row[112],row[113],row[114],row[115],row[116],row[117],row[118],row[119]])          
    #print "written"


    for row in reader1:  
       
                writer0.writerow([row[0],row[1],row[4],row[6],row[14],row[29],row[45],
                          row[48],row[49],row[50],row[51],row[52],row[73],row[74],
                          row[55],row[56],row[57],row[58],row[59],
                          row[60],row[61],row[62],row[63],row[64],row[66],
                          row[88],row[87],row[72]])
    #print "Done!"




    form5500.close()
    shortform.close()
    writefile.close()


#==================Prune data=================================
    #os.chdir('os.getcwd()')
    File_path=os.getcwd()+"\\merged_sf5500.csv"
    File=open(File_path, 'rb')
    pruned=open('pruned.csv','wb')
    writer=csv.writer(pruned,delimiter=',')
    reader=csv.reader(File)
    writer.writerow(reader.next())

    #print ('2' in '2E2G2J3D' and '3' in '2E2G2J3D')

    for row in reader:
        #print row
        if(max(row[9],row[10])=='1' and row[11] <='1'):
            #writer.writerow(row)
            #print row
            if ( ('1' in row[12] or '4' in row[12])==False and ('1' in row[13] or '4' in row[13])==False):
                # print (row[23],row[24])
                writer.writerow(row)
                #print(('1' in row[23] or '4' in row[23]),('1' in row[24] or '4' in row[24]) )

    

    pruned.close()
    File.close()
    print ("%s is done!"%(folder[l]))

print("Complete...")
