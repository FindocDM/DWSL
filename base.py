
import pandas as pd
from glob import glob
from pandas_ta import ema
import datetime as dt
from dateutil.parser import parse
import PySimpleGUI as sg # for progress bar
from time import perf_counter
from threading import Thread
import multiprocessing as mp
from math import ceil

##starting time
start = perf_counter()

#reading all files of csv
def read_all_files_inside_directory()-> [str]:
    """This function reading all the csv files from specific folder .For eg: it reading Stocks """
    files=glob(r'D:\DWSL\InputOptFiles\*.csv')
    return files
files=read_all_files_inside_directory()
# print(len(files))

def round_number_interval_of_25000(number:int)->int:
    """Round number interval of 25000"""
    if number%25000!=0:
        return ((number//25000)+1)*25000
    return number

def round_number_interval_of_1(number:int)->int:
    """"This function return i/p=>1.4 ,o/p= 2 and i/p =1.6,o/p=2 and i/p=1 ,o/p=1 """
    return 0 if number<0 else ceil(number)


 

def get_value_of_weekly_input(weekday_name:str,opt_type:str)->pd.DataFrame:
    """Reading Weekly Input File"""
    df=pd.read_csv(r'D:\DWSL\WeeklyInputFile\WeeklyInput.csv')
    df=df[df['WeekDay']==weekday_name]

    if opt_type=="CEW1":
        df=df[['ce_en','ce_ex','ce_5_start','ce_start','ce_difference','ce_activation']]
        print
    elif opt_type=="PEW1":
        df=df[['pe_en','pe_ex','pe_5_start','pe_start','pe_difference','pe_activation']]
    
    else:
        df=pd.DataFrame()

    return df
# get_value_of_weekly_input("Monday", "CEW1")
   


def get_filename(file:[str])->[]:
    """returning filename"""
    splitted_file = file.split('\\')
    separate_from_csv=splitted_file[-1].split('.')
    return separate_from_csv[0]

all_formatted_file=list(map(get_filename,files))
# print(all_formatted_file)

#read csv_file
def read_csv_file(filename: str,columns_list:list=[]) -> pd.DataFrame:
    """"Reading csv files using pandas"""
    # df=pd.read_csv(filename,parse_dates=['Date'],usecols=columns_list)

    df=pd.read_csv(filename,index_col=False,parse_dates=['Date'],dayfirst=True)
    
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    # print(type(df['Date'][0]))
    
    #filtering data from year 
    # df=df[df['Date'].dt.year >2009]
    # df=df[int(df['Date'])>=2011]
    
    return df



def perform_operation(index:int,file:str)->None:
    # print("RunningCode")
    filename=all_formatted_file[index]
    # print(filename)
    strike=filename[9:14]
    # print(strike)
    strike_type=1 if int(strike)%500 ==0 else 0
    opt_type=filename[14:]
    #reading csv
    df=read_csv_file(file)

    # getting week_day_name 
    df['WeekDay'] = df['Date'].dt.strftime('%A')

   

    #adding extra column in dataframe
    df['Strike']=strike
    df['Strike Type']=strike_type
    df['Opt_Type']=opt_type

    # #df index
    df_index=df.index
    df_start_index=df_index.start
    df_end_index=df_index.stop

    #previous column
    volume=[]
    counter=0
    j=0
    for i in range(df_start_index,df_end_index-1):
        
        if df['Date'].dt.date.loc[i] ==  df['Date'].dt.date[i+1]:
            if counter==0 and j==0:
                j=i
            else:
                pass
            volume.append(df['Volume'].loc[j])
            counter+=1
            # print(">>Process")
        else:
            if counter>df_start_index:
                volume.append( df['Volume'].loc[j])
                j=i+1
                # counter=0
            
            # volume.append(df['Volume'].loc[i+1])
            # break
                
        # print(f'{counter/len(df)*100}%>>>>>>>>')
    
    volume.append(volume[-1])
    # print(volume,len(volume),len(df))
    df['Previous']=volume
    df['Change']=df['Volume']-df['Previous']

    #calculating en_rate_Condition
    en_rate_list=[]
    ex_rate_list=[]
    start_value_list=[]
    difference_list=[]
    activation_list=[]
   
    for i in df.index:
        get_weekday_value=get_value_of_weekly_input(df['WeekDay'].loc[i],df['Opt_Type'].loc[i])
        get_value_columns=list(get_weekday_value)
        get_value_columns_to_set=set(get_value_columns)
       
       
        if {'ce_en','ce_ex','ce_5_start','ce_start','ce_difference','ce_activation'}.issubset( get_value_columns_to_set):
            # print("yes",print(get_value_columns))
            difference_list.append(get_weekday_value['ce_difference'].tolist()[0])
            activation_list.append(get_weekday_value['ce_activation'].tolist()[0])
            en_rate_value=1 if all(get_weekday_value['ce_en']<=df['Close'].loc[i]) else 0
            
            ex_rate_value=1 if all(get_weekday_value['ce_ex']>=df['Close'].loc[i]) else 0

           
            
            
        elif {'pe_en','pe_ex','pe_5_start','pe_start','pe_difference','pe_activation'}.issubset( get_value_columns_to_set):
            difference_list.append(get_weekday_value['pe_difference'].tolist()[0])
            activation_list.append(get_weekday_value['pe_activation'].tolist()[0])
            en_rate_value=1 if all(get_weekday_value['pe_en']<=df['Close'].loc[i]) else 0
        
            ex_rate_value=1 if all(get_weekday_value['pe_ex']>=df['Close'].loc[i]) else 0
            

        
        en_rate_list.append(en_rate_value)
        ex_rate_list.append(ex_rate_value)

        # working on start column
        
        volume_value=int(df['Previous'][i])
        if "CE" in opt_type:
            
            if df['Strike Type'][i]==1:
                # print(df['Strike Type'][i]==1)
                ce_5_start_value=int(get_weekday_value['ce_5_start'])
                start_value=ce_5_start_value if ce_5_start_value>=volume_value else round_number_interval_of_25000(volume_value)
                start_value_list.append(start_value)
                

            elif df['Strike Type'][i]==0:
                ce_start_value=int(get_weekday_value['ce_start'])
                start_value=ce_start_value if ce_start_value>=volume_value else round_number_interval_of_25000(volume_value)
                start_value_list.append(start_value)
                
        
        elif "PE" in opt_type:
            
            if df['Strike Type'][i]==1:
                # print(filename,"PE")
                pe_5_start_value=int(get_weekday_value['pe_5_start'])
                start_value=pe_5_start_value if pe_5_start_value>=volume_value else round_number_interval_of_25000(volume_value)
                start_value_list.append(start_value)
                print(len(start_value_list))

            elif df['Strike Type'][i]==0:
                pe_start_value=int(get_weekday_value['pe_start'])
                start_value=pe_start_value if pe_start_value>=volume_value else round_number_interval_of_25000(volume_value)
                start_value_list.append(start_value)
                print(len(start_value_list))

    df['en_rate_Condition']=en_rate_list
    df['ex_rate_Condition']=ex_rate_list
    df['Start']=start_value_list
    df['difference']=difference_list
    df['activation']=activation_list
    df['prefered_qty']=(df['Volume']-df['Start'])/df['difference']
    df['prefered_qty']=df['prefered_qty'].apply(round_number_interval_of_1)
    df['buybuy']=df['Start']-df['activation']+(df['prefered_qty']*df['difference'])
    actual_value_list=[]
    # count=0
    for i in df.index:
        
        if i==df_start_index:
            if df.loc[i,'ex_rate_Condition']==0:
                # print(i)
                actual_value_list.append(0)
            else:
                actual_value_list.append(1)
    
    
        elif df.loc[i-1,'prefered_qty'] ==df.loc[i,'prefered_qty']:
            if len(actual_value_list)>0:
                actual_value_list.append(actual_value_list[-1]) # appending last actual value
        
        elif df.loc[i,'prefered_qty'] -df.loc[i-1,'prefered_qty'] >0:
            print(i)
            print(df.loc[i,'prefered_qty'] ,df.loc[i-1,'prefered_qty'])
            
            if df.loc[i,'en_rate_Condition']==1:#and actual_value_list[-1]>=df.loc[i,'prefered_qty']
                actual_value_list.append(df.loc[i,'prefered_qty']) # appending last actual value
                print(df.loc[i,'prefered_qty'])
                # break
            else:
                if len(actual_value_list)>0:
                    actual_value_list.append(actual_value_list[-1]) # appending last actual value
        
        elif df.loc[i,'prefered_qty'] -df.loc[i-1,'prefered_qty'] < 0:
            if df.loc[i,'buybuy']<=df.loc[i,'Volume'] :#and actual_value_list[-1]>=df.loc[i,'prefered_qty']
                actual_value_list.append(df.loc[i,'prefered_qty']) # appending last actual value
            else:
                if len(actual_value_list)>0:
                    actual_value_list.append(actual_value_list[-1]) # appending last actual value
        
       
    df['actual']=actual_value_list
    #filtering df having ex_rate_condition 1
    df_ex_rate_condition_one=df[df['ex_rate_Condition']==1]
    for i in df_ex_rate_condition_one.index:
        df.loc[i,"actual"]=0

    #more filtering
    for i in df.index:
        if i==df_start_index:
            pass
        else:
            print(i,"index")
            
            if df.loc[i,'Volume']>df.loc[i-1,'Volume'] and df.loc[i,'en_rate_Condition'] == 1:
                # print(df.loc[i,'actual'],df.loc[i,'prefered_qty'])
                df.loc[i,'actual']=df.loc[i,'prefered_qty']
                # print(df.loc[i,'actual'],df.loc[i,'prefered_qty'])
            else:
                if df.loc[i,'Volume']<df.loc[i-1,'Volume'] and df.loc[i,'Volume']<df.loc[i,'buybuy']:
                    print(df.loc[i,'actual'],df.loc[i,'prefered_qty'])
                    df.loc[i,'actual']=df.loc[i,'prefered_qty']
                    print(df.loc[i,'actual'],df.loc[i,'prefered_qty'])
                else:
                    df.loc[i,'actual']=df.loc[i-1,'actual']
                    

        # break
    # print(df_ex_rate_condition_one)

    #data having time 15:15:59 making zero in dataframe
    df_having_15_15_59 = df[df['Time']>='15:15:59']
    for i in df_having_15_15_59.index:
        df.loc[i,"actual"]=0
    

    #SUV and BV
    sv_value_list=[]
    bv_value_list=[]
    #putting zero in both list as there will be no difference of first
    sv_value_list.append(0)
    bv_value_list.append(0)
    for i in df.index:
        # print(i)
        if i==df_start_index:
            pass
        else:
            # print(i,"index")
            difference_value_actual=df.loc[i,'actual']-df.loc[i-1,'actual']
            if   difference_value_actual==0:
                # print(df.loc[i,'actual'],df.loc[i,'prefered_qty'])
                # sv_value=bv_value=df.loc[i-1,'actual']
                sv_value=sv_value_list[-1]
                bv_value=bv_value_list[-1]
               
                # print(df.loc[i,'actual'],df.loc[i,'prefered_qty'])
            elif  difference_value_actual>0:
                    
                    if difference_value_actual<0:
                        sv_value=sv_value_list[-1]
                        return
                    sv_value=sv_value_list[-1]+difference_value_actual*df.loc[i,'Close']
            
                
                

            elif difference_value_actual<0:
                if difference_value_actual>0:
                    bv_value=bv_value_list[-1]
                    return
                bv_value=bv_value_list[-1]+abs(difference_value_actual)*df.loc[i,'Close']
               
            
            sv_value_list.append(sv_value)
            bv_value_list.append(bv_value)
    
    df['SV']=sv_value_list
    df['BV']=bv_value_list

    #Making SV and BV 0 
    for i in df_having_15_15_59.index:
        df.loc[i,"SV"]=0
        df.loc[i,"BV"]=0

    df['SV-BV']=df['SV']-df['BV']
    df['Value']=df['actual']*df['Close']
    df['M2M']=(df['SV-BV']-df['Value'])*25
    # df['M2M*25']=df['M2M']*25
    
    # df.reset_index(inplace=True) => this line create extra index in csv file
    df.to_csv(r'D:\\DWSL\Process\\'+filename+'.csv',index=False)
   



  
# threading. #add 5 to each number  
if __name__ == '__main__':
    
    #dividing list into  16 chunks
    #How many elements each
    # list should have
    # n = 16
 
    # using list comprehension
    # final = [files[i * n:(i + 1) * n] for i in range((len(files) + n - 1) // n )]
    # print (len(final))
    # count=0
    # for files in final:
    # sg.one_line_progress_meter('My meter', count+1, len(final), 'DWSL' )
    thread_list=[]
    for index,file in enumerate(files):
        # thrd=mp.Process(target=perform_operation,args=(index,file,))
        thrd=Thread(target=perform_operation,args=(index,file,))
        
        thrd.start()
        thread_list.append(thrd)
        # perform_operation(index, file)
        # break

    
    for thrd in thread_list:
        thrd.join()
    
    # count+=1
        
    # print(len(thread_list))
    end= perf_counter()
    print("Total time taken in seconds: ",end-start)
        # break
           