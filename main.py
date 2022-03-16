#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


import glob
import re
import os
# search .csv files
# in the current working directory

for py in glob.glob("SAP\*.csv"):
    list_of_files = glob.glob('SAP\*.csv') # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
print(latest_file)
     
df = pd.read_csv(latest_file)


# In[3]:


df


# In[4]:


df['COMPONENT QTY.'].apply(type).value_counts()


# In[5]:


df['COMPONENT VALUE'].apply(type).value_counts()


# In[6]:


df['PRO QUANITY'] = df['PRO QUANITY'].str.replace(',','').str.replace('$','').astype('float')
df['COMPONENT QTY.'] = df['COMPONENT QTY.'].str.replace(',','').str.replace('$','').astype('float')
df['COMPONENT VALUE'] = df['COMPONENT VALUE'].str.replace(',','').str.replace('$','').astype('float')


# In[7]:


df


# In[8]:


df.drop(['MOVEMENT TYPE' , 'REMARK'] , axis=1 , inplace=True)


# In[9]:


df


# In[10]:


df.fillna(value = 0 , inplace = True)


# In[11]:


df


# In[12]:


df['COMPONENT'] = df['COMPONENT'].astype('str')


# In[13]:


df


# In[14]:


def componentconvert(x):
       if len(x)==1:
           return x
       else:
           if x[:2]=='00':
               return x[2:]
           else:
               return x


# In[15]:


df.COMPONENT = df.COMPONENT.apply(componentconvert)


# In[16]:


df.info()


# In[17]:


NATCPOL = df[490:507]


# In[18]:


NATCPOL


# In[19]:


CPF_Symtet_Tech94 = df[118:124]


# In[20]:


CPF_Symtet_Tech94


# In[21]:


CPF_TECH_OWN = df[105:113]
CPF_Tech_own1 = df[116:117]
frames = [CPF_TECH_OWN,CPF_Tech_own1]
CPF_TECH_OWN94 = pd.concat(frames)


# In[22]:


CPF_TECH_OWN94


# In[23]:


NATCPOL_ESTER = df[321:328]
NATCPOL_ESTER


# In[ ]:





# In[24]:


ACTP_ESTER = df[3:18]
ACTP_ESTER


# In[25]:


RRCMA = df[347:351]
RRCMA


# In[26]:


DCPFROMDCA = df[433:443]
DCPFROMDCA


# In[27]:


MECL = df[470:473]
MECL


# In[28]:


import mysql.connector
from mysql.connector import Error
try:
    connection = mysql.connector.connect(host='127.0.0.1',
                                         database='product_data',
                                         user='root',
                                         password='amogh28')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor(buffered=True)
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)


# In[29]:


sql_select_Query = "select * from standard_data"
cursor = connection.cursor()
cursor.execute(sql_select_Query)
    # get all records
records = cursor.fetchall()
print("Total number of rows in table: ", cursor.rowcount)


# In[30]:


NATCPOL


# In[31]:


costsheet_for_natcpol_cpf = pd.DataFrame()
costsheet_for_natcpol_cpf['ID'] = NATCPOL['COMPONENT']
costsheet_for_natcpol_cpf['Description'] = NATCPOL['COMPONENT DESCR.']


# In[32]:


costsheet_for_natcpol_cpf


# In[33]:


x = NATCPOL['COMPONENT']


# In[34]:


for column in NATCPOL:
   # Select column contents by column name using [] operator
   columnSeriesObj = NATCPOL['COMPONENT']
   
print(columnSeriesObj.values)


# In[35]:


import numpy as np
data=[]
try:
    connection = mysql.connector.connect(host='127.0.0.1',
                                         database='product_data',
                                         user='root',
                                         password='amogh28')
    cursor2 = connection.cursor(prepared=True)
    
    cursor2.execute("SELECT Component_ID FROM standard_data WHERE Product_Desc='NATCPOL (FOR CPF)'")
    result_set = [item[0] for item in cursor2.fetchall()]
    #print(result_set)
    

    ansf = []
    ansf2 = []
 
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='NATCPOL (FOR CPF)'"
        cursor2.execute(queryrun,(val,))
        ans = cursor2.fetchall()
        print(ans)
        print(len(ans))
        if len(ans)==0:
            ansf.append(0)
        else:
            ansf.append(ans[0][0])
        ansf =[round(num,4) for num in ansf]
        
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='NATCPOL (FOR CPF)'"
        cursor2.execute(queryrun,(val,))
        ans2 = cursor2.fetchall()
        print(ans2)
        if len(ans2)==0:
            ansf2.append(0)
        else:
            ansf2.append(ans2[0][0])
        ansf2 =[round(num,4) for num in ansf2]
        
    
        
  
        
    connection.commit()
except mysql.connector.Error as error:
    print("parameterized query failed {}".format(error))


# In[36]:


ansf3 = []
ansf4 = []
others = [1 , 2 , 3 , 4 , 5 , 6, 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14]
for val in others:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='NATCPOL (FOR CPF)'"
        cursor2.execute(queryrun,(val,))
        ans3 = cursor2.fetchall()
        #print(ans3)
        #print(len(ans3))
        if len(ans3)==0:
            ansf3.append(0)
        else:
            ansf3.append(ans3[0][0])
        ansf3 =[round(num,4) for num in ansf3]
for val in others:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='NATCPOL (FOR CPF)'"
        cursor2.execute(queryrun,(val,))
        ans4 = cursor2.fetchall()
        #print(ans4)
        if len(ans4)==0:
            ansf4.append(0)
        else:
            ansf4.append(ans4[0][0])
        ansf4 =[round(num,4) for num in ansf4]
        
print(ansf3)
print(ansf4)


# In[37]:


costsheet_for_natcpol_cpf['STD_Consumption'] = ansf
costsheet_for_natcpol_cpf['month_Consumption'] = NATCPOL['COMPONENT QTY.'] / NATCPOL['PRO QUANITY']
costsheet_for_natcpol_cpf['STD_Rate'] = ansf2
costsheet_for_natcpol_cpf['month_Rate'] = NATCPOL['COMPONENT VALUE'] / NATCPOL['COMPONENT QTY.']
costsheet_for_natcpol_cpf['STD_cost'] = costsheet_for_natcpol_cpf['STD_Consumption'] * costsheet_for_natcpol_cpf['STD_Rate']
costsheet_for_natcpol_cpf['Month_cost']= costsheet_for_natcpol_cpf['month_Consumption']*costsheet_for_natcpol_cpf['month_Rate']
costsheet_for_natcpol_cpf['Price CM'] = (costsheet_for_natcpol_cpf['STD_Rate'] - costsheet_for_natcpol_cpf['month_Rate'])*costsheet_for_natcpol_cpf['month_Consumption']
costsheet_for_natcpol_cpf['Usage CM'] = (costsheet_for_natcpol_cpf['STD_Consumption'] - costsheet_for_natcpol_cpf['month_Consumption'])*costsheet_for_natcpol_cpf['STD_Rate']
costsheet_for_natcpol_cpf['Total CM'] = costsheet_for_natcpol_cpf['Usage CM'] + costsheet_for_natcpol_cpf['Price CM']
costsheet_for_natcpol_cpf


# In[38]:


import pandas as pd
string = str(latest_file)
file = 'natcpol_cpf'+ '-' + string[4:12]
writer = pd.ExcelWriter('RawCostsheet/' + file + '.' + 'xlsx')  
 
costsheet_for_natcpol_cpf.to_excel(writer, sheet_name='costsheet_for_natcpol_cpf',index=False, na_rep='NaN')

# Auto-adjust columns' width
for column in costsheet_for_natcpol_cpf:
    column_width = max(costsheet_for_natcpol_cpf[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_natcpol_cpf.columns.get_loc(column)
    writer.sheets['costsheet_for_natcpol_cpf'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_natcpol_cpf']
#worksheet.write(0, 0, 'NATCPOL for CPF', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[39]:


costsheet_for_natcpol_cpf['STD_Consumption'] = ansf
costsheet_for_natcpol_cpf['month_Consumption'] = NATCPOL['COMPONENT QTY.'] / NATCPOL['PRO QUANITY']
costsheet_for_natcpol_cpf['STD_Rate'] = ansf2
costsheet_for_natcpol_cpf['month_Rate'] = NATCPOL['COMPONENT VALUE'] / NATCPOL['COMPONENT QTY.']
costsheet_for_natcpol_cpf['STD_cost'] = costsheet_for_natcpol_cpf['STD_Consumption'] * costsheet_for_natcpol_cpf['STD_Rate']
costsheet_for_natcpol_cpf['Month_cost']= costsheet_for_natcpol_cpf['month_Consumption']*costsheet_for_natcpol_cpf['month_Rate']
costsheet_for_natcpol_cpf['Price CM'] = (costsheet_for_natcpol_cpf['STD_Rate'] - costsheet_for_natcpol_cpf['month_Rate'])*costsheet_for_natcpol_cpf['month_Consumption']
costsheet_for_natcpol_cpf['Usage CM'] = (costsheet_for_natcpol_cpf['STD_Consumption'] - costsheet_for_natcpol_cpf['month_Consumption'])*costsheet_for_natcpol_cpf['STD_Rate']
costsheet_for_natcpol_cpf['Total CM'] = costsheet_for_natcpol_cpf['Usage CM'] + costsheet_for_natcpol_cpf['Price CM']

costsheet_for_natcpol_cpf.loc['Total RM']= costsheet_for_natcpol_cpf.sum(numeric_only=True, axis=0)
costsheet_for_natcpol_cpf


# In[40]:


import pandas as pd

dict = {'ID':[1,2,3,4,5,6,7,8,9,10,11,12,13,14],
        'Description':['Steam-DIrect',
'Steam-IndIrect',
'Power-DIrect',
'Power-IndIrect',
'Water-DIrect','Water-IndIrect'
,
'Utilities total'
,
'Stores-Direct'
,
'Stores-IndIrect'
,
'Repairs-Direct'
,
'Environment cost'
,
'Repairs-Indirect'
,
'Repairs Total'
,
'Total Variable Cost'
] }
  
df2 = pd.DataFrame(dict)
df2['STD_Consumption'] = ansf3
df2['month_Consumption'] = np.zeros(14)
df2['STD_Rate'] = ansf4
df2['month_Rate'] = np.zeros(14)
df2['STD_cost'] = df2['STD_Consumption']*df2['STD_Rate']
df2['Month_cost'] = np.zeros(14)
df2['Price CM'] = np.zeros(14)
df2['Usage CM'] = np.zeros(14)
df2['Total CM'] = np.zeros(14)

costsheet_for_natcpol_cpf = pd.concat([costsheet_for_natcpol_cpf, df2], ignore_index = True)
costsheet_for_natcpol_cpf.reset_index()


# In[41]:


import pandas as pd
string = str(latest_file)
file = 'natcpol_cpf'+ '-' + string[4:12]
writer = pd.ExcelWriter('COSTSHEET/' + file + '.' + 'xlsx')  
 
costsheet_for_natcpol_cpf.to_excel(writer, sheet_name='costsheet_for_natcpol_cpf',index=False, na_rep='NaN',startrow=2)

# Auto-adjust columns' width
for column in costsheet_for_natcpol_cpf:
    column_width = max(costsheet_for_natcpol_cpf[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_natcpol_cpf.columns.get_loc(column)
    writer.sheets['costsheet_for_natcpol_cpf'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_natcpol_cpf']
worksheet.write(0, 0, 'NATCPOL for CPF', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()
print(type(worksheet))


# In[42]:


costsheet_for_CPF_Symtet_Tech94 = pd.DataFrame()
costsheet_for_CPF_Symtet_Tech94['ID'] = CPF_Symtet_Tech94['COMPONENT']
costsheet_for_CPF_Symtet_Tech94['Description'] = CPF_Symtet_Tech94['COMPONENT DESCR.']

x = CPF_Symtet_Tech94['COMPONENT']
for column in CPF_Symtet_Tech94:
   # Select column contents by column name using [] operator
   columnSeriesObj = CPF_Symtet_Tech94['COMPONENT']
   

data=[]
try:
    connection = mysql.connector.connect(host='127.0.0.1',
                                         database='product_data',
                                         user='root',
                                         password='amogh28')
    cursor2 = connection.cursor(prepared=True)
    
    cursor2.execute("SELECT Component_ID FROM standard_data WHERE Product_Desc='CHLOROPYRIPHOS  94% (FROM SYMTET)'")
    result_set = [item[0] for item in cursor2.fetchall()]
    #print(result_set)
    

    ansf = []
    ansf2 = []
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='CHLOROPYRIPHOS  94% (FROM SYMTET)'"
        cursor2.execute(queryrun,(val,))
        ans = cursor2.fetchall()
        print(ans)
        print(len(ans))
        if len(ans)==0:
            ansf.append(0)
        else:
            ansf.append(ans[0][0])
        ansf =[round(num,4) for num in ansf]
        
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='CHLOROPYRIPHOS  94% (FROM SYMTET)'"
        cursor2.execute(queryrun,(val,))
        ans2 = cursor2.fetchall()
        print(ans2)
        if len(ans2)==0:
            ansf2.append(0)
        else:
            ansf2.append(ans2[0][0])
        ansf2 =[round(num,4) for num in ansf2]
        
        
  
        
    connection.commit()
except mysql.connector.Error as error:
    print("parameterized query failed {}".format(error))


# In[43]:


ansf3 = []
ansf4 = []
others = [1 , 2 , 3 , 4 , 5 , 6, 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14]
for val in others:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='CHLOROPYRIPHOS  94% (FROM SYMTET)'"
        cursor2.execute(queryrun,(val,))
        ans3 = cursor2.fetchall()
        #print(ans3)
        #print(len(ans3))
        if len(ans3)==0:
            ansf3.append(0)
        else:
            ansf3.append(ans3[0][0])
        ansf3 =[round(num,4) for num in ansf3]
for val in others:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='CHLOROPYRIPHOS  94% (FROM SYMTET)'"
        cursor2.execute(queryrun,(val,))
        ans4 = cursor2.fetchall()
        #print(ans4)
        if len(ans4)==0:
            ansf4.append(0)
        else:
            ansf4.append(ans4[0][0])
        ansf4 =[round(num,4) for num in ansf4]
        
print(ansf3)
print(ansf4)


# In[44]:


costsheet_for_CPF_Symtet_Tech94['STD_Consumption'] = ansf
costsheet_for_CPF_Symtet_Tech94['month_Consumption'] = CPF_Symtet_Tech94['COMPONENT QTY.'] / CPF_Symtet_Tech94['PRO QUANITY']
costsheet_for_CPF_Symtet_Tech94['STD_Rate'] = ansf2
costsheet_for_CPF_Symtet_Tech94['month_Rate'] = CPF_Symtet_Tech94['COMPONENT VALUE'] / CPF_Symtet_Tech94['COMPONENT QTY.']
costsheet_for_CPF_Symtet_Tech94['STD_cost'] = costsheet_for_CPF_Symtet_Tech94['STD_Consumption'] * costsheet_for_CPF_Symtet_Tech94['STD_Rate']
costsheet_for_CPF_Symtet_Tech94['Month_cost'] = costsheet_for_CPF_Symtet_Tech94['month_Rate'] * costsheet_for_CPF_Symtet_Tech94['month_Consumption']
costsheet_for_CPF_Symtet_Tech94['Price CM'] = (costsheet_for_CPF_Symtet_Tech94['STD_Rate'] - costsheet_for_CPF_Symtet_Tech94['month_Rate'])*costsheet_for_CPF_Symtet_Tech94['month_Consumption']
costsheet_for_CPF_Symtet_Tech94['Usage CM'] = (costsheet_for_CPF_Symtet_Tech94['STD_Consumption'] - costsheet_for_CPF_Symtet_Tech94['month_Consumption'])*costsheet_for_CPF_Symtet_Tech94['STD_Rate']
costsheet_for_CPF_Symtet_Tech94['Total CM'] = costsheet_for_CPF_Symtet_Tech94['Usage CM'] + costsheet_for_CPF_Symtet_Tech94['Price CM']


# In[45]:


import pandas as pd
string = str(latest_file)
file = 'CPF_Symtet_Tech94'+ '-' + string[4:12]
writer = pd.ExcelWriter('RawCostsheet/' + file + '.' + 'xlsx')  

costsheet_for_CPF_Symtet_Tech94.to_excel(writer, sheet_name='costsheet_for_CPF_Symtet_Tech94', index=False, na_rep='NaN')

# Auto-adjust columns' width
for column in costsheet_for_CPF_Symtet_Tech94:
    column_width = max(costsheet_for_CPF_Symtet_Tech94[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_CPF_Symtet_Tech94.columns.get_loc(column)
    writer.sheets['costsheet_for_CPF_Symtet_Tech94'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_CPF_Symtet_Tech94']
#worksheet.write(0, 0, 'CPF TECH 94% (SYMTET)', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[46]:


costsheet_for_CPF_Symtet_Tech94['STD_Consumption'] = ansf
costsheet_for_CPF_Symtet_Tech94['month_Consumption'] = CPF_Symtet_Tech94['COMPONENT QTY.'] / CPF_Symtet_Tech94['PRO QUANITY']
costsheet_for_CPF_Symtet_Tech94['STD_Rate'] = ansf2
costsheet_for_CPF_Symtet_Tech94['month_Rate'] = CPF_Symtet_Tech94['COMPONENT VALUE'] / CPF_Symtet_Tech94['COMPONENT QTY.']
costsheet_for_CPF_Symtet_Tech94['STD_cost'] = costsheet_for_CPF_Symtet_Tech94['STD_Consumption'] * costsheet_for_CPF_Symtet_Tech94['STD_Rate']
costsheet_for_CPF_Symtet_Tech94['Month_cost'] = costsheet_for_CPF_Symtet_Tech94['month_Rate'] * costsheet_for_CPF_Symtet_Tech94['month_Consumption']
costsheet_for_CPF_Symtet_Tech94['Price CM'] = (costsheet_for_CPF_Symtet_Tech94['STD_Rate'] - costsheet_for_CPF_Symtet_Tech94['month_Rate'])*costsheet_for_CPF_Symtet_Tech94['month_Consumption']
costsheet_for_CPF_Symtet_Tech94['Usage CM'] = (costsheet_for_CPF_Symtet_Tech94['STD_Consumption'] - costsheet_for_CPF_Symtet_Tech94['month_Consumption'])*costsheet_for_CPF_Symtet_Tech94['STD_Rate']
costsheet_for_CPF_Symtet_Tech94['Total CM'] = costsheet_for_CPF_Symtet_Tech94['Usage CM'] + costsheet_for_CPF_Symtet_Tech94['Price CM']
costsheet_for_CPF_Symtet_Tech94.loc['Total RM']= costsheet_for_CPF_Symtet_Tech94.sum(numeric_only=True, axis=0)
costsheet_for_CPF_Symtet_Tech94


# In[47]:


import pandas as pd

dict = {'ID':[1,2,3,4,5,6,7,8,9,10,11,12,13,14],
        'Description':['Steam-DIrect',
'Steam-IndIrect',
'Power-DIrect',
'Power-IndIrect',
'Water-DIrect','Water-IndIrect'
,
'Utilities total'
,
'Stores-Direct'
,
'Stores-IndIrect'
,
'Repairs-Direct'
,
'Environment cost'
,
'Repairs-Indirect'
,
'Repairs Total'
,
'Total Variable Cost'
] }
  
df2 = pd.DataFrame(dict)
df2['STD_Consumption'] = ansf3
df2['month_Consumption'] = np.zeros(14)
df2['STD_Rate'] = ansf4
df2['month_Rate'] = np.zeros(14)
df2['STD_cost'] = df2['STD_Consumption']*df2['STD_Rate']
df2['Month_cost'] = np.zeros(14)
df2['Price CM'] = np.zeros(14)
df2['Usage CM'] = np.zeros(14)
df2['Total CM'] = np.zeros(14)

costsheet_for_CPF_Symtet_Tech94 = pd.concat([costsheet_for_CPF_Symtet_Tech94, df2], ignore_index = True)
costsheet_for_CPF_Symtet_Tech94.reset_index()


# In[48]:


import pandas as pd
string = str(latest_file)
file = 'CPF_Symtet_Tech94'+ '-' + string[4:12]
writer = pd.ExcelWriter('COSTSHEET/' + file + '.' + 'xlsx')  

costsheet_for_CPF_Symtet_Tech94.to_excel(writer, sheet_name='costsheet_for_CPF_Symtet_Tech94', index=False, na_rep='NaN',startrow=2)

# Auto-adjust columns' width
for column in costsheet_for_CPF_Symtet_Tech94:
    column_width = max(costsheet_for_CPF_Symtet_Tech94[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_CPF_Symtet_Tech94.columns.get_loc(column)
    writer.sheets['costsheet_for_CPF_Symtet_Tech94'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_CPF_Symtet_Tech94']
worksheet.write(0, 0, 'CPF TECH 94% (SYMTET)', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()
print(type(worksheet))


# In[49]:


costsheet_for_CPF_TECH_OWN94 = pd.DataFrame()
costsheet_for_CPF_TECH_OWN94['ID'] = CPF_TECH_OWN94['COMPONENT']
costsheet_for_CPF_TECH_OWN94['Description'] = CPF_TECH_OWN94['COMPONENT DESCR.']

x = CPF_TECH_OWN94['COMPONENT']
for column in CPF_TECH_OWN94:
   # Select column contents by column name using [] operator
   columnSeriesObj = CPF_TECH_OWN94['COMPONENT']
   

data=[]
try:
    connection = mysql.connector.connect(host='127.0.0.1',
                                         database='product_data',
                                         user='root',
                                         password='amogh28')
    cursor2 = connection.cursor(prepared=True)
    
    cursor2.execute("SELECT Component_ID FROM standard_data WHERE Product_Desc='CHLOROPYRIPHOS (CRUDE) 94%'")
    result_set = [item[0] for item in cursor2.fetchall()]
    #print(result_set)
    

    ansf = []
    ansf2 = []
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='CHLOROPYRIPHOS (CRUDE) 94%'"
        cursor2.execute(queryrun,(val,))
        ans = cursor2.fetchall()
        print(ans)
        print(len(ans))
        if len(ans)==0:
            ansf.append(0)
        else:
            ansf.append(ans[0][0])
        ansf =[round(num,4) for num in ansf]
        
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='CHLOROPYRIPHOS (CRUDE) 94%'"
        cursor2.execute(queryrun,(val,))
        ans2 = cursor2.fetchall()
        print(ans2)
        if len(ans2)==0:
            ansf2.append(0)
        else:
            ansf2.append(ans2[0][0])
        ansf2 =[round(num,4) for num in ansf2]
        
        
  
        
    connection.commit()
except mysql.connector.Error as error:
    print("parameterized query failed {}".format(error))


# In[50]:


ansf3 = []
ansf4 = []
others = [1 , 2 , 3 , 4 , 5 , 6, 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14]
for val in others:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='CHLOROPYRIPHOS (CRUDE) 94%'"
        cursor2.execute(queryrun,(val,))
        ans3 = cursor2.fetchall()
        #print(ans3)
        #print(len(ans3))
        if len(ans3)==0:
            ansf3.append(0)
        else:
            ansf3.append(ans3[0][0])
        ansf3 =[round(num,4) for num in ansf3]
for val in others:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='CHLOROPYRIPHOS (CRUDE) 94%'"
        cursor2.execute(queryrun,(val,))
        ans4 = cursor2.fetchall()
        #print(ans4)
        if len(ans4)==0:
            ansf4.append(0)
        else:
            ansf4.append(ans4[0][0])
        ansf4 =[round(num,4) for num in ansf4]
        
print(ansf3)
print(ansf4)


# In[51]:


costsheet_for_CPF_TECH_OWN94['STD_Consumption'] = ansf
costsheet_for_CPF_TECH_OWN94['month_Consumption'] = CPF_TECH_OWN94['COMPONENT QTY.'] / CPF_TECH_OWN94['PRO QUANITY']
costsheet_for_CPF_TECH_OWN94['STD_Rate'] = ansf2
costsheet_for_CPF_TECH_OWN94['month_Rate'] = CPF_TECH_OWN94['COMPONENT VALUE'] / CPF_TECH_OWN94['COMPONENT QTY.']
costsheet_for_CPF_TECH_OWN94['STD_cost'] = costsheet_for_CPF_TECH_OWN94['STD_Consumption'] * costsheet_for_CPF_TECH_OWN94['STD_Rate']
costsheet_for_CPF_TECH_OWN94['Month_cost'] = costsheet_for_CPF_TECH_OWN94['month_Consumption'] * costsheet_for_CPF_TECH_OWN94['month_Rate']
costsheet_for_CPF_TECH_OWN94['Price CM'] = (costsheet_for_CPF_TECH_OWN94['STD_Rate'] - costsheet_for_CPF_TECH_OWN94['month_Rate'])*costsheet_for_CPF_TECH_OWN94['month_Consumption']
costsheet_for_CPF_TECH_OWN94['Usage CM'] = (costsheet_for_CPF_TECH_OWN94['STD_Consumption'] - costsheet_for_CPF_TECH_OWN94['month_Consumption'])*costsheet_for_CPF_TECH_OWN94['STD_Rate']
costsheet_for_CPF_TECH_OWN94['Total CM'] = costsheet_for_CPF_TECH_OWN94['Usage CM'] + costsheet_for_CPF_TECH_OWN94['Price CM']


# In[52]:


import pandas as pd
string = str(latest_file)
file = 'CPF_TECH_OWN94'+ '-' + string[4:12]
writer = pd.ExcelWriter('RawCostsheet/' + file + '.' + 'xlsx')  
 
costsheet_for_CPF_TECH_OWN94.to_excel(writer, sheet_name='costsheet_for_CPF_TECH_OWN94', index=False, na_rep='NaN')

# Auto-adjust columns' width
for column in costsheet_for_CPF_TECH_OWN94:
    column_width = max(costsheet_for_CPF_TECH_OWN94[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_CPF_TECH_OWN94.columns.get_loc(column)
    writer.sheets['costsheet_for_CPF_TECH_OWN94'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_CPF_TECH_OWN94']
#worksheet.write(0, 0, 'CPF TECH 94% (OWN NATCPOL)', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[53]:


costsheet_for_CPF_TECH_OWN94['STD_Consumption'] = ansf
costsheet_for_CPF_TECH_OWN94['month_Consumption'] = CPF_TECH_OWN94['COMPONENT QTY.'] / CPF_TECH_OWN94['PRO QUANITY']
costsheet_for_CPF_TECH_OWN94['STD_Rate'] = ansf2
costsheet_for_CPF_TECH_OWN94['month_Rate'] = CPF_TECH_OWN94['COMPONENT VALUE'] / CPF_TECH_OWN94['COMPONENT QTY.']
costsheet_for_CPF_TECH_OWN94['STD_cost'] = costsheet_for_CPF_TECH_OWN94['STD_Consumption'] * costsheet_for_CPF_TECH_OWN94['STD_Rate']
costsheet_for_CPF_TECH_OWN94['Month_cost'] = costsheet_for_CPF_TECH_OWN94['month_Consumption'] * costsheet_for_CPF_TECH_OWN94['month_Rate']
costsheet_for_CPF_TECH_OWN94['Price CM'] = (costsheet_for_CPF_TECH_OWN94['STD_Rate'] - costsheet_for_CPF_TECH_OWN94['month_Rate'])*costsheet_for_CPF_TECH_OWN94['month_Consumption']
costsheet_for_CPF_TECH_OWN94['Usage CM'] = (costsheet_for_CPF_TECH_OWN94['STD_Consumption'] - costsheet_for_CPF_TECH_OWN94['month_Consumption'])*costsheet_for_CPF_TECH_OWN94['STD_Rate']
costsheet_for_CPF_TECH_OWN94['Total CM'] = costsheet_for_CPF_TECH_OWN94['Usage CM'] + costsheet_for_CPF_TECH_OWN94['Price CM']
costsheet_for_CPF_TECH_OWN94.loc['Total RM']= costsheet_for_CPF_TECH_OWN94.sum(numeric_only=True, axis=0)
costsheet_for_CPF_TECH_OWN94


# In[54]:


import pandas as pd

dict = {'ID':[1,2,3,4,5,6,7,8,9,10,11,12,13,14],
        'Description':['Steam-DIrect',
'Steam-IndIrect',
'Power-DIrect',
'Power-IndIrect',
'Water-DIrect','Water-IndIrect'
,
'Utilities total'
,
'Stores-Direct'
,
'Stores-IndIrect'
,
'Repairs-Direct'
,
'Environment cost'
,
'Repairs-Indirect'
,
'Repairs Total'
,
'Total Variable Cost'
] }
  
df2 = pd.DataFrame(dict)
df2['STD_Consumption'] = ansf3
df2['month_Consumption'] = np.zeros(14)
df2['STD_Rate'] = ansf4
df2['month_Rate'] = np.zeros(14)
df2['STD_cost'] = df2['STD_Consumption']*df2['STD_Rate']
df2['Month_cost'] = np.zeros(14)
df2['Price CM'] = np.zeros(14)
df2['Usage CM'] = np.zeros(14)
df2['Total CM'] = np.zeros(14)

costsheet_for_CPF_TECH_OWN94 = pd.concat([costsheet_for_CPF_TECH_OWN94, df2], ignore_index = True)
costsheet_for_CPF_TECH_OWN94.reset_index()


# In[55]:


import pandas as pd
string = str(latest_file)
file = 'CPF_TECH_OWN94'+ '-' + string[4:12]
writer = pd.ExcelWriter('COSTSHEET/' + file + '.' + 'xlsx')  
 
costsheet_for_CPF_TECH_OWN94.to_excel(writer, sheet_name='costsheet_for_CPF_TECH_OWN94', index=False, na_rep='NaN',startrow=2)

# Auto-adjust columns' width
for column in costsheet_for_CPF_TECH_OWN94:
    column_width = max(costsheet_for_CPF_TECH_OWN94[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_CPF_TECH_OWN94.columns.get_loc(column)
    writer.sheets['costsheet_for_CPF_TECH_OWN94'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_CPF_TECH_OWN94']
worksheet.write(0, 0, 'CPF TECH 94% (OWN NATCPOL)', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[56]:


costsheet_for_NATCPOL_ESTER = pd.DataFrame()
costsheet_for_NATCPOL_ESTER['ID'] = NATCPOL_ESTER['COMPONENT']
costsheet_for_NATCPOL_ESTER['Description'] = NATCPOL_ESTER['COMPONENT DESCR.']

x = NATCPOL_ESTER['COMPONENT']
for column in NATCPOL_ESTER:
   # Select column contents by column name using [] operator
   columnSeriesObj = NATCPOL_ESTER['COMPONENT']
   

data=[]
try:
    connection = mysql.connector.connect(host='127.0.0.1',
                                         database='product_data',
                                         user='root',
                                         password='amogh28')
    cursor2 = connection.cursor(prepared=True)
    
    cursor2.execute("SELECT Component_ID FROM standard_data WHERE Product_Desc='NATCPOL (FOR ACTP ESTER)'")
    result_set = [item[0] for item in cursor2.fetchall()]
    #print(result_set)
    

    ansf = []
    ansf2 = []
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='NATCPOL (FOR ACTP ESTER)'"
        cursor2.execute(queryrun,(val,))
        ans = cursor2.fetchall()
        print(ans)
        print(len(ans))
        if len(ans)==0:
            ansf.append(0)
        else:
            ansf.append(ans[0][0])
        ansf =[round(num,4) for num in ansf]
        
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='NATCPOL (FOR ACTP ESTER)'"
        cursor2.execute(queryrun,(val,))
        ans2 = cursor2.fetchall()
        print(ans2)
        if len(ans2)==0:
            ansf2.append(0)
        else:
            ansf2.append(ans2[0][0])
        ansf2 =[round(num,4) for num in ansf2]
        
        
  
        
    connection.commit()
except mysql.connector.Error as error:
    print("parameterized query failed {}".format(error))


# In[57]:


ansf3 = []
ansf4 = []
others = [1 , 2 , 3 , 4 , 5 , 6, 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14]
for val in others:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='NATCPOL (FOR ACTP ESTER)'"
        cursor2.execute(queryrun,(val,))
        ans3 = cursor2.fetchall()
        #print(ans3)
        #print(len(ans3))
        if len(ans3)==0:
            ansf3.append(0)
        else:
            ansf3.append(ans3[0][0])
        ansf3 =[round(num,4) for num in ansf3]
for val in others:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='NATCPOL (FOR ACTP ESTER)'"
        cursor2.execute(queryrun,(val,))
        ans4 = cursor2.fetchall()
        #print(ans4)
        if len(ans4)==0:
            ansf4.append(0)
        else:
            ansf4.append(ans4[0][0])
        ansf4 =[round(num,4) for num in ansf4]
        
print(ansf3)
print(ansf4)


# In[58]:


costsheet_for_NATCPOL_ESTER['STD_Consumption'] = ansf
costsheet_for_NATCPOL_ESTER['month_Consumption'] = NATCPOL_ESTER['COMPONENT QTY.'] / NATCPOL_ESTER['PRO QUANITY']
costsheet_for_NATCPOL_ESTER['STD_Rate'] = ansf2
costsheet_for_NATCPOL_ESTER['month_Rate'] = NATCPOL_ESTER['COMPONENT VALUE'] / NATCPOL_ESTER['COMPONENT QTY.']
costsheet_for_NATCPOL_ESTER['STD_cost'] = costsheet_for_NATCPOL_ESTER['STD_Consumption'] * costsheet_for_NATCPOL_ESTER['STD_Rate']
costsheet_for_NATCPOL_ESTER['Month_cost'] = costsheet_for_NATCPOL_ESTER['month_Consumption'] * costsheet_for_NATCPOL_ESTER['month_Rate']
costsheet_for_NATCPOL_ESTER['Price CM'] = (costsheet_for_NATCPOL_ESTER['STD_Rate'] - costsheet_for_NATCPOL_ESTER['month_Rate'])*costsheet_for_NATCPOL_ESTER['month_Consumption']
costsheet_for_NATCPOL_ESTER['Usage CM'] = (costsheet_for_NATCPOL_ESTER['STD_Consumption'] - costsheet_for_NATCPOL_ESTER['month_Consumption'])*costsheet_for_NATCPOL_ESTER['STD_Rate']
costsheet_for_NATCPOL_ESTER['Total CM'] = costsheet_for_NATCPOL_ESTER['Usage CM'] + costsheet_for_NATCPOL_ESTER['Price CM']


# In[59]:


import pandas as pd
string = str(latest_file)
file = 'NATCPOL_ESTER'+ '-' + string[4:12]
writer = pd.ExcelWriter('RawCostsheet/' + file + '.' + 'xlsx')  

costsheet_for_NATCPOL_ESTER.to_excel(writer, sheet_name='costsheet_for_NATCPOL_ESTER', index=False, na_rep='NaN')

# Auto-adjust columns' width
for column in costsheet_for_NATCPOL_ESTER:
    column_width = max(costsheet_for_NATCPOL_ESTER[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_NATCPOL_ESTER.columns.get_loc(column)
    writer.sheets['costsheet_for_NATCPOL_ESTER'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_NATCPOL_ESTER']
#worksheet.write(0, 0, 'NaTCPOL for ACTP ESTER', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[60]:


costsheet_for_NATCPOL_ESTER['STD_Consumption'] = ansf
costsheet_for_NATCPOL_ESTER['month_Consumption'] = NATCPOL_ESTER['COMPONENT QTY.'] / NATCPOL_ESTER['PRO QUANITY']
costsheet_for_NATCPOL_ESTER['STD_Rate'] = ansf2
costsheet_for_NATCPOL_ESTER['month_Rate'] = NATCPOL_ESTER['COMPONENT VALUE'] / NATCPOL_ESTER['COMPONENT QTY.']
costsheet_for_NATCPOL_ESTER['STD_cost'] = costsheet_for_NATCPOL_ESTER['STD_Consumption'] * costsheet_for_NATCPOL_ESTER['STD_Rate']
costsheet_for_NATCPOL_ESTER['Month_cost'] = costsheet_for_NATCPOL_ESTER['month_Consumption'] * costsheet_for_NATCPOL_ESTER['month_Rate']
costsheet_for_NATCPOL_ESTER['Price CM'] = (costsheet_for_NATCPOL_ESTER['STD_Rate'] - costsheet_for_NATCPOL_ESTER['month_Rate'])*costsheet_for_NATCPOL_ESTER['month_Consumption']
costsheet_for_NATCPOL_ESTER['Usage CM'] = (costsheet_for_NATCPOL_ESTER['STD_Consumption'] - costsheet_for_NATCPOL_ESTER['month_Consumption'])*costsheet_for_NATCPOL_ESTER['STD_Rate']
costsheet_for_NATCPOL_ESTER['Total CM'] = costsheet_for_NATCPOL_ESTER['Usage CM'] + costsheet_for_NATCPOL_ESTER['Price CM']
costsheet_for_NATCPOL_ESTER.loc['Total RM']= costsheet_for_NATCPOL_ESTER.sum(numeric_only=True, axis=0)
costsheet_for_NATCPOL_ESTER


# In[61]:


import pandas as pd

dict = {'ID':[1,2,3,4,5,6,7,8,9,10,11,12,13,14],
        'Description':['Steam-DIrect',
'Steam-IndIrect',
'Power-DIrect',
'Power-IndIrect',
'Water-DIrect','Water-IndIrect'
,
'Utilities total'
,
'Stores-Direct'
,
'Stores-IndIrect'
,
'Repairs-Direct'
,
'Environment cost'
,
'Repairs-Indirect'
,
'Repairs Total'
,
'Total Variable Cost'
] }
  
df2 = pd.DataFrame(dict)
df2['STD_Consumption'] = ansf3
df2['month_Consumption'] = np.zeros(14)
df2['STD_Rate'] = ansf4
df2['month_Rate'] = np.zeros(14)
df2['STD_cost'] = df2['STD_Consumption']*df2['STD_Rate']
df2['Month_cost'] = np.zeros(14)
df2['Price CM'] = np.zeros(14)
df2['Usage CM'] = np.zeros(14)
df2['Total CM'] = np.zeros(14)

costsheet_for_NATCPOL_ESTER = pd.concat([costsheet_for_NATCPOL_ESTER, df2], ignore_index = True)
costsheet_for_NATCPOL_ESTER.reset_index()


# In[62]:


import pandas as pd
string = str(latest_file)
file = 'NATCPOL_ESTER'+ '-' + string[4:12]
writer = pd.ExcelWriter('COSTSHEET/' + file + '.' + 'xlsx')  

costsheet_for_NATCPOL_ESTER.to_excel(writer, sheet_name='costsheet_for_NATCPOL_ESTER', index=False, na_rep='NaN',startrow=2)

# Auto-adjust columns' width
for column in costsheet_for_NATCPOL_ESTER:
    column_width = max(costsheet_for_NATCPOL_ESTER[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_NATCPOL_ESTER.columns.get_loc(column)
    writer.sheets['costsheet_for_NATCPOL_ESTER'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_NATCPOL_ESTER']
worksheet.write(0, 0, 'NaTCPOL for ACTP ESTER', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[63]:


costsheet_for_ACTP_ESTER = pd.DataFrame()
costsheet_for_ACTP_ESTER['ID'] = ACTP_ESTER['COMPONENT']
costsheet_for_ACTP_ESTER['Description'] = ACTP_ESTER['COMPONENT DESCR.']

x = ACTP_ESTER['COMPONENT']
for column in ACTP_ESTER:
   # Select column contents by column name using [] operator
   columnSeriesObj = ACTP_ESTER['COMPONENT']
   

data=[]
try:
    connection = mysql.connector.connect(host='127.0.0.1',
                                         database='product_data',
                                         user='root',
                                         password='amogh28')
    cursor2 = connection.cursor(prepared=True)
    
    cursor2.execute("SELECT Component_ID FROM standard_data WHERE Product_Desc='ACTP ESTER'")
    result_set = [item[0] for item in cursor2.fetchall()]
    #print(result_set)
    

    ansf = []
    ansf2 = []
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='ACTP ESTER'"
        cursor2.execute(queryrun,(val,))
        ans = cursor2.fetchall()
        print(ans)
        print(len(ans))
        if len(ans)==0:
            ansf.append(0)
        else:
            ansf.append(ans[0][0])
        ansf =[round(num,4) for num in ansf]
        
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='ACTP ESTER'"
        cursor2.execute(queryrun,(val,))
        ans2 = cursor2.fetchall()
        print(ans2)
        if len(ans2)==0:
            ansf2.append(0)
        else:
            ansf2.append(ans2[0][0])
        ansf2 =[round(num,4) for num in ansf2]
        
        
  
        
    connection.commit()
except mysql.connector.Error as error:
    print("parameterized query failed {}".format(error))


# In[64]:


ansf3 = []
ansf4 = []
others = [1 , 2 , 3 , 4 , 5 , 6, 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14]
for val in others:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='ACTP ESTER'"
        cursor2.execute(queryrun,(val,))
        ans3 = cursor2.fetchall()
        #print(ans3)
        #print(len(ans3))
        if len(ans3)==0:
            ansf3.append(0)
        else:
            ansf3.append(ans3[0][0])
        ansf3 =[round(num,4) for num in ansf3]
for val in others:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='ACTP ESTER'"
        cursor2.execute(queryrun,(val,))
        ans4 = cursor2.fetchall()
        #print(ans4)
        if len(ans4)==0:
            ansf4.append(0)
        else:
            ansf4.append(ans4[0][0])
        ansf4 =[round(num,4) for num in ansf4]
        
print(ansf3)
print(ansf4)


# In[65]:


costsheet_for_ACTP_ESTER['STD_Consumption'] = ansf
costsheet_for_ACTP_ESTER['month_Consumption'] = ACTP_ESTER['COMPONENT QTY.'] / ACTP_ESTER['PRO QUANITY']
costsheet_for_ACTP_ESTER['STD_Rate'] = ansf2
costsheet_for_ACTP_ESTER['month_Rate'] = ACTP_ESTER['COMPONENT VALUE'] / ACTP_ESTER['COMPONENT QTY.']
costsheet_for_ACTP_ESTER['STD_cost'] = costsheet_for_ACTP_ESTER['STD_Consumption'] * costsheet_for_ACTP_ESTER['STD_Rate']
costsheet_for_ACTP_ESTER['Month_cost'] = costsheet_for_ACTP_ESTER['month_Consumption'] * costsheet_for_ACTP_ESTER['month_Rate']
costsheet_for_ACTP_ESTER['Price CM'] = (costsheet_for_ACTP_ESTER['STD_Rate'] - costsheet_for_ACTP_ESTER['month_Rate'])*costsheet_for_ACTP_ESTER['month_Consumption']
costsheet_for_ACTP_ESTER['Usage CM'] = (costsheet_for_ACTP_ESTER['STD_Consumption'] - costsheet_for_ACTP_ESTER['month_Consumption'])*costsheet_for_ACTP_ESTER['STD_Rate']
costsheet_for_ACTP_ESTER['Total CM'] = costsheet_for_ACTP_ESTER['Usage CM'] + costsheet_for_ACTP_ESTER['Price CM']


# In[66]:


import pandas as pd
string = str(latest_file)
file = 'ACTP_ESTER'+ '-' + string[4:12]
writer = pd.ExcelWriter('RawCostsheet/' + file + '.' + 'xlsx')  
 
costsheet_for_ACTP_ESTER.to_excel(writer, sheet_name='costsheet_for_ACTP_ESTER', index=False, na_rep='NaN')

# Auto-adjust columns' width
for column in costsheet_for_ACTP_ESTER:
    column_width = max(costsheet_for_ACTP_ESTER[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_ACTP_ESTER.columns.get_loc(column)
    writer.sheets['costsheet_for_ACTP_ESTER'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_ACTP_ESTER']
#worksheet.write(0, 0, 'ACTP Ester', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[67]:


costsheet_for_ACTP_ESTER['STD_Consumption'] = ansf
costsheet_for_ACTP_ESTER['month_Consumption'] = ACTP_ESTER['COMPONENT QTY.'] / ACTP_ESTER['PRO QUANITY']
costsheet_for_ACTP_ESTER['STD_Rate'] = ansf2
costsheet_for_ACTP_ESTER['month_Rate'] = ACTP_ESTER['COMPONENT VALUE'] / ACTP_ESTER['COMPONENT QTY.']
costsheet_for_ACTP_ESTER['STD_cost'] = costsheet_for_ACTP_ESTER['STD_Consumption'] * costsheet_for_ACTP_ESTER['STD_Rate']
costsheet_for_ACTP_ESTER['Month_cost'] = costsheet_for_ACTP_ESTER['month_Consumption'] * costsheet_for_ACTP_ESTER['month_Rate']
costsheet_for_ACTP_ESTER['Price CM'] = (costsheet_for_ACTP_ESTER['STD_Rate'] - costsheet_for_ACTP_ESTER['month_Rate'])*costsheet_for_ACTP_ESTER['month_Consumption']
costsheet_for_ACTP_ESTER['Usage CM'] = (costsheet_for_ACTP_ESTER['STD_Consumption'] - costsheet_for_ACTP_ESTER['month_Consumption'])*costsheet_for_ACTP_ESTER['STD_Rate']
costsheet_for_ACTP_ESTER['Total CM'] = costsheet_for_ACTP_ESTER['Usage CM'] + costsheet_for_ACTP_ESTER['Price CM']
costsheet_for_ACTP_ESTER.loc['Total RM']= costsheet_for_ACTP_ESTER.sum(numeric_only=True, axis=0)
costsheet_for_ACTP_ESTER


# In[68]:


import pandas as pd

dict = {'ID':[1,2,3,4,5,6,7,8,9,10,11,12,13,14],
        'Description':['Steam-DIrect',
'Steam-IndIrect',
'Power-DIrect',
'Power-IndIrect',
'Water-DIrect','Water-IndIrect'
,
'Utilities total'
,
'Stores-Direct'
,
'Stores-IndIrect'
,
'Repairs-Direct'
,
'Environment cost'
,
'Repairs-Indirect'
,
'Repairs Total'
,
'Total Variable Cost'
] }
  
df2 = pd.DataFrame(dict)
df2['STD_Consumption'] = ansf3
df2['month_Consumption'] = np.zeros(14)
df2['STD_Rate'] = ansf4
df2['month_Rate'] = np.zeros(14)
df2['STD_cost'] = df2['STD_Consumption']*df2['STD_Rate']
df2['Month_cost'] = np.zeros(14)
df2['Price CM'] = np.zeros(14)
df2['Usage CM'] = np.zeros(14)
df2['Total CM'] = np.zeros(14)

costsheet_for_ACTP_ESTER = pd.concat([costsheet_for_ACTP_ESTER, df2], ignore_index = True)
costsheet_for_ACTP_ESTER.reset_index()


# In[69]:


import pandas as pd
string = str(latest_file)
file = 'ACTP_ESTER'+ '-' + string[4:12]
writer = pd.ExcelWriter('COSTSHEET/' + file + '.' + 'xlsx')  
 
costsheet_for_ACTP_ESTER.to_excel(writer, sheet_name='costsheet_for_ACTP_ESTER', index=False, na_rep='NaN',startrow=2)

# Auto-adjust columns' width
for column in costsheet_for_ACTP_ESTER:
    column_width = max(costsheet_for_ACTP_ESTER[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_ACTP_ESTER.columns.get_loc(column)
    writer.sheets['costsheet_for_ACTP_ESTER'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_ACTP_ESTER']
worksheet.write(0, 0, 'ACTP Ester', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[70]:


costsheet_for_RRCMA = pd.DataFrame()
costsheet_for_RRCMA['ID'] = RRCMA['COMPONENT']
costsheet_for_RRCMA['Description'] = RRCMA['COMPONENT DESCR.']
x = RRCMA['COMPONENT']
for column in RRCMA:
   # Select column contents by column name using [] operator
   columnSeriesObj = RRCMA['COMPONENT']
   

data=[]
try:
    connection = mysql.connector.connect(host='127.0.0.1',
                                         database='product_data',
                                         user='root',
                                         password='amogh28')
    cursor2 = connection.cursor(prepared=True)
    
    cursor2.execute("SELECT Component_ID FROM standard_data WHERE Product_Desc='RR CMA'")
    result_set = [item[0] for item in cursor2.fetchall()]
    #print(result_set)
    

    ansf = []
    ansf2 = []
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='RR CMA'"
        cursor2.execute(queryrun,(val,))
        ans = cursor2.fetchall()
        print(ans)
        print(len(ans))
        if len(ans)==0:
            ansf.append(0)
        else:
            ansf.append(ans[0][0])
        ansf =[round(num,4) for num in ansf]
        
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='RR CMA'"
        cursor2.execute(queryrun,(val,))
        ans2 = cursor2.fetchall()
        print(ans2)
        if len(ans2)==0:
            ansf2.append(0)
        else:
            ansf2.append(ans2[0][0])
        ansf2 =[round(num,4) for num in ansf2]
        
        
  
        
    connection.commit()
except mysql.connector.Error as error:
    print("parameterized query failed {}".format(error))


# In[71]:


ansf3 = []
ansf4 = []
others = [1 , 2 , 3 , 4 , 5 , 6, 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14]
for val in others:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='RR CMA'"
        cursor2.execute(queryrun,(val,))
        ans3 = cursor2.fetchall()
        #print(ans3)
        #print(len(ans3))
        if len(ans3)==0:
            ansf3.append(0)
        else:
            ansf3.append(ans3[0][0])
        ansf3 =[round(num,4) for num in ansf3]
for val in others:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='RR CMA'"
        cursor2.execute(queryrun,(val,))
        ans4 = cursor2.fetchall()
        #print(ans4)
        if len(ans4)==0:
            ansf4.append(0)
        else:
            ansf4.append(ans4[0][0])
        ansf4 =[round(num,4) for num in ansf4]
        
print(ansf3)
print(ansf4)


# In[72]:


costsheet_for_RRCMA['STD_Consumption'] = ansf
costsheet_for_RRCMA['month_Consumption'] = RRCMA['COMPONENT QTY.'] / RRCMA['PRO QUANITY']
costsheet_for_RRCMA['STD_Rate'] = ansf2
costsheet_for_RRCMA['month_Rate'] = RRCMA['COMPONENT VALUE'] / RRCMA['COMPONENT QTY.']
costsheet_for_RRCMA['STD_cost'] = costsheet_for_RRCMA['STD_Consumption'] * costsheet_for_RRCMA['STD_Rate']
costsheet_for_RRCMA['Month_cost'] = costsheet_for_RRCMA['month_Consumption'] * costsheet_for_RRCMA['month_Rate']
costsheet_for_RRCMA['Price CM'] = (costsheet_for_RRCMA['STD_Rate'] - costsheet_for_RRCMA['month_Rate'])*costsheet_for_RRCMA['month_Consumption']
costsheet_for_RRCMA['Usage CM'] = (costsheet_for_RRCMA['STD_Consumption'] - costsheet_for_RRCMA['month_Consumption'])*costsheet_for_RRCMA['STD_Rate']
costsheet_for_RRCMA['Total CM'] = costsheet_for_RRCMA['Usage CM'] + costsheet_for_RRCMA['Price CM']


# In[73]:


import pandas as pd
string = str(latest_file)
file = 'RRCMA'+ '-' + string[4:12]
writer = pd.ExcelWriter('RawCostsheet/' + file + '.' + 'xlsx')  

costsheet_for_RRCMA.to_excel(writer, sheet_name='costsheet_for_RRCMA', index=False, na_rep='NaN')

# Auto-adjust columns' width
for column in costsheet_for_RRCMA:
    column_width = max(costsheet_for_RRCMA[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_RRCMA.columns.get_loc(column)
    writer.sheets['costsheet_for_RRCMA'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_RRCMA']
#worksheet.write(0, 0, 'RR CMA', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[74]:


costsheet_for_RRCMA['STD_Consumption'] = ansf
costsheet_for_RRCMA['month_Consumption'] = RRCMA['COMPONENT QTY.'] / RRCMA['PRO QUANITY']
costsheet_for_RRCMA['STD_Rate'] = ansf2
costsheet_for_RRCMA['month_Rate'] = RRCMA['COMPONENT VALUE'] / RRCMA['COMPONENT QTY.']
costsheet_for_RRCMA['STD_cost'] = costsheet_for_RRCMA['STD_Consumption'] * costsheet_for_RRCMA['STD_Rate']
costsheet_for_RRCMA['Month_cost'] = costsheet_for_RRCMA['month_Consumption'] * costsheet_for_RRCMA['month_Rate']
costsheet_for_RRCMA['Price CM'] = (costsheet_for_RRCMA['STD_Rate'] - costsheet_for_RRCMA['month_Rate'])*costsheet_for_RRCMA['month_Consumption']
costsheet_for_RRCMA['Usage CM'] = (costsheet_for_RRCMA['STD_Consumption'] - costsheet_for_RRCMA['month_Consumption'])*costsheet_for_RRCMA['STD_Rate']
costsheet_for_RRCMA['Total CM'] = costsheet_for_RRCMA['Usage CM'] + costsheet_for_RRCMA['Price CM']
costsheet_for_RRCMA.loc['Total RM']= costsheet_for_RRCMA.sum(numeric_only=True, axis=0)
costsheet_for_RRCMA


# In[75]:


import pandas as pd

dict = {'ID':[1,2,3,4,5,6,7,8,9,10,11,12,13,14],
        'Description':['Steam-DIrect',
'Steam-IndIrect',
'Power-DIrect',
'Power-IndIrect',
'Water-DIrect','Water-IndIrect'
,
'Utilities total'
,
'Stores-Direct'
,
'Stores-IndIrect'
,
'Repairs-Direct'
,
'Environment cost'
,
'Repairs-Indirect'
,
'Repairs Total'
,
'Total Variable Cost'
] }
  
df2 = pd.DataFrame(dict)
df2['STD_Consumption'] = ansf3
df2['month_Consumption'] = np.zeros(14)
df2['STD_Rate'] = ansf4
df2['month_Rate'] = np.zeros(14)
df2['STD_cost'] = df2['STD_Consumption']*df2['STD_Rate']
df2['Month_cost'] = np.zeros(14)
df2['Price CM'] = np.zeros(14)
df2['Usage CM'] = np.zeros(14)
df2['Total CM'] = np.zeros(14)

costsheet_for_RRCMA = pd.concat([costsheet_for_RRCMA, df2], ignore_index = True)
costsheet_for_RRCMA.reset_index()


# In[76]:


import pandas as pd
string = str(latest_file)
file = 'RRCMA'+ '-' + string[4:12]
writer = pd.ExcelWriter('COSTSHEET/' + file + '.' + 'xlsx')  

costsheet_for_RRCMA.to_excel(writer, sheet_name='costsheet_for_RRCMA', index=False, na_rep='NaN',startrow=2)

# Auto-adjust columns' width
for column in costsheet_for_RRCMA:
    column_width = max(costsheet_for_RRCMA[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_RRCMA.columns.get_loc(column)
    writer.sheets['costsheet_for_RRCMA'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_RRCMA']
worksheet.write(0, 0, 'RR CMA', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[77]:


costsheet_for_DCPFROMDCA = pd.DataFrame()
costsheet_for_DCPFROMDCA['ID'] = DCPFROMDCA['COMPONENT']
costsheet_for_DCPFROMDCA['Description'] = DCPFROMDCA['COMPONENT DESCR.']
x = DCPFROMDCA['COMPONENT']
for column in DCPFROMDCA:
   # Select column contents by column name using [] operator
   columnSeriesObj = DCPFROMDCA['COMPONENT']
   

data=[]
try:
    connection = mysql.connector.connect(host='127.0.0.1',
                                         database='product_data',
                                         user='root',
                                         password='amogh28')
    cursor2 = connection.cursor(prepared=True)
    
    cursor2.execute("SELECT Component_ID FROM standard_data WHERE Product_Desc='DELTAMETHRIN'")
    result_set = [item[0] for item in cursor2.fetchall()]
    #print(result_set)
    

    ansf = []
    ansf2 = []
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='DELTAMETHRIN'"
        cursor2.execute(queryrun,(val,))
        ans = cursor2.fetchall()
        print(ans)
        print(len(ans))
        if len(ans)==0:
            ansf.append(0)
        else:
            ansf.append(ans[0][0])
        ansf =[round(num,4) for num in ansf]
        
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='DELTAMETHRIN'"
        cursor2.execute(queryrun,(val,))
        ans2 = cursor2.fetchall()
        print(ans2)
        if len(ans2)==0:
            ansf2.append(0)
        else:
            ansf2.append(ans2[0][0])
        ansf2 =[round(num,4) for num in ansf2]
        
        
  
        
    connection.commit()
except mysql.connector.Error as error:
    print("parameterized query failed {}".format(error))


# In[78]:


ansf3 = []
ansf4 = []
others = [1 , 2 , 3 , 4 , 5 , 6, 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14]
for val in others:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='DELTAMETHRIN'"
        cursor2.execute(queryrun,(val,))
        ans3 = cursor2.fetchall()
        #print(ans3)
        #print(len(ans3))
        if len(ans3)==0:
            ansf3.append(0)
        else:
            ansf3.append(ans3[0][0])
        ansf3 =[round(num,4) for num in ansf3]
for val in others:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='DELTAMETHRIN'"
        cursor2.execute(queryrun,(val,))
        ans4 = cursor2.fetchall()
        #print(ans4)
        if len(ans4)==0:
            ansf4.append(0)
        else:
            ansf4.append(ans4[0][0])
        ansf4 =[round(num,4) for num in ansf4]
        
print(ansf3)
print(ansf4)


# In[79]:


costsheet_for_DCPFROMDCA['STD_Consumption'] = ansf
costsheet_for_DCPFROMDCA['month_Consumption'] = DCPFROMDCA['COMPONENT QTY.'] / DCPFROMDCA['PRO QUANITY']
costsheet_for_DCPFROMDCA['STD_Rate'] = ansf2
costsheet_for_DCPFROMDCA['month_Rate'] = DCPFROMDCA['COMPONENT VALUE'] /DCPFROMDCA['COMPONENT QTY.']
costsheet_for_DCPFROMDCA['STD_cost'] = costsheet_for_DCPFROMDCA['STD_Consumption'] * costsheet_for_DCPFROMDCA['STD_Rate']
costsheet_for_DCPFROMDCA['Month_cost'] = costsheet_for_DCPFROMDCA['month_Consumption'] * costsheet_for_DCPFROMDCA['month_Rate']
costsheet_for_DCPFROMDCA['Price CM'] = (costsheet_for_DCPFROMDCA['STD_Rate'] - costsheet_for_DCPFROMDCA['month_Rate'])*costsheet_for_DCPFROMDCA['month_Consumption']
costsheet_for_DCPFROMDCA['Usage CM'] = (costsheet_for_DCPFROMDCA['STD_Consumption'] - costsheet_for_DCPFROMDCA['month_Consumption'])*costsheet_for_DCPFROMDCA['STD_Rate']
costsheet_for_DCPFROMDCA['Total CM'] = costsheet_for_DCPFROMDCA['Usage CM'] + costsheet_for_DCPFROMDCA['Price CM']


# In[80]:


import pandas as pd
string = str(latest_file)
file = 'DCPFROMDCA'+ '-' + string[4:12]
writer = pd.ExcelWriter('RawCostsheet/' + file + '.' + 'xlsx')  

costsheet_for_DCPFROMDCA.to_excel(writer, sheet_name='costsheet_for_DCPFROMDCA',index=False, na_rep='NaN')

# Auto-adjust columns' width
for column in costsheet_for_DCPFROMDCA:
    column_width = max(costsheet_for_DCPFROMDCA[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_DCPFROMDCA.columns.get_loc(column)
    writer.sheets['costsheet_for_DCPFROMDCA'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_DCPFROMDCA']
#worksheet.write(0, 0, 'DCP FROM DCA ', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[81]:


costsheet_for_DCPFROMDCA['STD_Consumption'] = ansf
costsheet_for_DCPFROMDCA['month_Consumption'] = DCPFROMDCA['COMPONENT QTY.'] / DCPFROMDCA['PRO QUANITY']
costsheet_for_DCPFROMDCA['STD_Rate'] = ansf2
costsheet_for_DCPFROMDCA['month_Rate'] = DCPFROMDCA['COMPONENT VALUE'] /DCPFROMDCA['COMPONENT QTY.']
costsheet_for_DCPFROMDCA['STD_cost'] = costsheet_for_DCPFROMDCA['STD_Consumption'] * costsheet_for_DCPFROMDCA['STD_Rate']
costsheet_for_DCPFROMDCA['Month_cost'] = costsheet_for_DCPFROMDCA['month_Consumption'] * costsheet_for_DCPFROMDCA['month_Rate']
costsheet_for_DCPFROMDCA['Price CM'] = (costsheet_for_DCPFROMDCA['STD_Rate'] - costsheet_for_DCPFROMDCA['month_Rate'])*costsheet_for_DCPFROMDCA['month_Consumption']
costsheet_for_DCPFROMDCA['Usage CM'] = (costsheet_for_DCPFROMDCA['STD_Consumption'] - costsheet_for_DCPFROMDCA['month_Consumption'])*costsheet_for_DCPFROMDCA['STD_Rate']
costsheet_for_DCPFROMDCA['Total CM'] = costsheet_for_DCPFROMDCA['Usage CM'] + costsheet_for_DCPFROMDCA['Price CM']
costsheet_for_DCPFROMDCA.loc['Total RM']= costsheet_for_DCPFROMDCA.sum(numeric_only=True, axis=0)
costsheet_for_DCPFROMDCA


# In[82]:


import pandas as pd

dict = {'ID':[1,2,3,4,5,6,7,8,9,10,11,12,13,14],
        'Description':['Steam-DIrect',
'Steam-IndIrect',
'Power-DIrect',
'Power-IndIrect',
'Water-DIrect','Water-IndIrect'
,
'Utilities total'
,
'Stores-Direct'
,
'Stores-IndIrect'
,
'Repairs-Direct'
,
'Environment cost'
,
'Repairs-Indirect'
,
'Repairs Total'
,
'Total Variable Cost'
] }
  
df2 = pd.DataFrame(dict)
df2['STD_Consumption'] = ansf3
df2['month_Consumption'] = np.zeros(14)
df2['STD_Rate'] = ansf4
df2['month_Rate'] = np.zeros(14)
df2['STD_cost'] = df2['STD_Consumption']*df2['STD_Rate']
df2['Month_cost'] = np.zeros(14)
df2['Price CM'] = np.zeros(14)
df2['Usage CM'] = np.zeros(14)
df2['Total CM'] = np.zeros(14)

costsheet_for_DCPFROMDCA = pd.concat([costsheet_for_DCPFROMDCA, df2], ignore_index = True)
costsheet_for_DCPFROMDCA.reset_index()


# In[83]:


import pandas as pd
string = str(latest_file)
file = 'DCPFROMDCA'+ '-' + string[4:12]
writer = pd.ExcelWriter('COSTSHEET/' + file + '.' + 'xlsx')  

costsheet_for_DCPFROMDCA.to_excel(writer, sheet_name='costsheet_for_DCPFROMDCA',index=False, na_rep='NaN',startrow=2)

# Auto-adjust columns' width
for column in costsheet_for_DCPFROMDCA:
    column_width = max(costsheet_for_DCPFROMDCA[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_DCPFROMDCA.columns.get_loc(column)
    writer.sheets['costsheet_for_DCPFROMDCA'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_DCPFROMDCA']
worksheet.write(0, 0, 'DCP FROM DCA ', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[84]:


costsheet_for_MECL = pd.DataFrame()
costsheet_for_MECL['ID'] = MECL['COMPONENT']
costsheet_for_MECL['Description'] = MECL['COMPONENT DESCR.']
x = MECL['COMPONENT']
for column in MECL:
   # Select column contents by column name using [] operator
   columnSeriesObj = MECL['COMPONENT']
   

data=[]
try:
    connection = mysql.connector.connect(host='127.0.0.1',
                                         database='product_data',
                                         user='root',
                                         password='amogh28')
    cursor2 = connection.cursor(prepared=True)
    
    cursor2.execute("SELECT Component_ID FROM standard_data WHERE Product_Desc='MECL'")
    result_set = [item[0] for item in cursor2.fetchall()]
    #print(result_set)
    

    ansf = []
    ansf2 = []
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='MECL'"
        cursor2.execute(queryrun,(val,))
        ans = cursor2.fetchall()
        print(ans)
        print(len(ans))
        if len(ans)==0:
            ansf.append(0)
        else:
            ansf.append(ans[0][0])
        ansf =[round(num,4) for num in ansf]
        
    for val in columnSeriesObj.values:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='MECL'"
        cursor2.execute(queryrun,(val,))
        ans2 = cursor2.fetchall()
        print(ans2)
        if len(ans2)==0:
            ansf2.append(0)
        else:
            ansf2.append(ans2[0][0])
        ansf2 =[round(num,4) for num in ansf2]
        
        
  
        
    connection.commit()
except mysql.connector.Error as error:
    print("parameterized query failed {}".format(error))


# In[85]:


ansf3 = []
ansf4 = []
others = [1 , 2 , 3 , 4 , 5 , 6, 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14]
for val in others:
        queryrun = "SELECT STD_consumption FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='MECL'"
        cursor2.execute(queryrun,(val,))
        ans3 = cursor2.fetchall()
        #print(ans3)
        #print(len(ans3))
        if len(ans3)==0:
            ansf3.append(0)
        else:
            ansf3.append(ans3[0][0])
        ansf3 =[round(num,4) for num in ansf3]
for val in others:
        queryrun = "SELECT STD_Rate FROM standard_data WHERE Component_ID=(%s) AND Product_Desc='MECL'"
        cursor2.execute(queryrun,(val,))
        ans4 = cursor2.fetchall()
        #print(ans4)
        if len(ans4)==0:
            ansf4.append(0)
        else:
            ansf4.append(ans4[0][0])
        ansf4 =[round(num,4) for num in ansf4]
        
print(ansf3)
print(ansf4)


# In[86]:


costsheet_for_MECL['STD_Consumption'] = ansf
costsheet_for_MECL['month_Consumption'] = MECL['COMPONENT QTY.'] / MECL['PRO QUANITY']
costsheet_for_MECL['STD_Rate'] = ansf2
costsheet_for_MECL['month_Rate'] = MECL['COMPONENT VALUE'] /MECL['COMPONENT QTY.']
costsheet_for_MECL['STD_cost'] = costsheet_for_MECL['STD_Consumption'] * costsheet_for_MECL['STD_Rate']
costsheet_for_MECL['Month_cost'] = costsheet_for_MECL['month_Consumption'] * costsheet_for_MECL['month_Rate']
costsheet_for_MECL['Price CM'] = (costsheet_for_MECL['STD_Rate'] - costsheet_for_MECL['month_Rate'])*costsheet_for_MECL['month_Consumption']
costsheet_for_MECL['Usage CM'] = (costsheet_for_MECL['STD_Consumption'] - costsheet_for_MECL['month_Consumption'])*costsheet_for_MECL['STD_Rate']
costsheet_for_MECL['Total CM'] = costsheet_for_MECL['Usage CM'] + costsheet_for_MECL['Price CM']


# In[87]:


import pandas as pd
string = str(latest_file)
file = 'MECL'+ '-' + string[4:12]
writer = pd.ExcelWriter('RawCostsheet/' + file + '.' + 'xlsx') 

costsheet_for_MECL.to_excel(writer, sheet_name='costsheet_for_MECL', index=False, na_rep='NaN')

# Auto-adjust columns' width
for column in costsheet_for_MECL:
    column_width = max(costsheet_for_MECL[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_MECL.columns.get_loc(column)
    writer.sheets['costsheet_for_MECL'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_MECL']
#worksheet.write(0, 0, 'MECL', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[88]:


costsheet_for_MECL['STD_Consumption'] = ansf
costsheet_for_MECL['month_Consumption'] = MECL['COMPONENT QTY.'] / MECL['PRO QUANITY']
costsheet_for_MECL['STD_Rate'] = ansf2
costsheet_for_MECL['month_Rate'] = MECL['COMPONENT VALUE'] /MECL['COMPONENT QTY.']
costsheet_for_MECL['STD_cost'] = costsheet_for_MECL['STD_Consumption'] * costsheet_for_MECL['STD_Rate']
costsheet_for_MECL['Month_cost'] = costsheet_for_MECL['month_Consumption'] * costsheet_for_MECL['month_Rate']
costsheet_for_MECL['Price CM'] = (costsheet_for_MECL['STD_Rate'] - costsheet_for_MECL['month_Rate'])*costsheet_for_MECL['month_Consumption']
costsheet_for_MECL['Usage CM'] = (costsheet_for_MECL['STD_Consumption'] - costsheet_for_MECL['month_Consumption'])*costsheet_for_MECL['STD_Rate']
costsheet_for_MECL['Total CM'] = costsheet_for_MECL['Usage CM'] + costsheet_for_MECL['Price CM']
costsheet_for_MECL.loc['Total RM']= costsheet_for_MECL.sum(numeric_only=True, axis=0)
costsheet_for_MECL


# In[89]:


import pandas as pd

dict = {'ID':[1,2,3,4,5,6,7,8,9,10,11,12,13,14],
        'Description':['Steam-DIrect',
'Steam-IndIrect',
'Power-DIrect',
'Power-IndIrect',
'Water-DIrect','Water-IndIrect'
,
'Utilities total'
,
'Stores-Direct'
,
'Stores-IndIrect'
,
'Repairs-Direct'
,
'Environment cost'
,
'Repairs-Indirect'
,
'Repairs Total'
,
'Total Variable Cost'
] }
  
df2 = pd.DataFrame(dict)
df2['STD_Consumption'] = ansf3
df2['month_Consumption'] = np.zeros(14)
df2['STD_Rate'] = ansf4
df2['month_Rate'] = np.zeros(14)
df2['STD_cost'] = df2['STD_Consumption']*df2['STD_Rate']
df2['Month_cost'] = np.zeros(14)
df2['Price CM'] = np.zeros(14)
df2['Usage CM'] = np.zeros(14)
df2['Total CM'] = np.zeros(14)

costsheet_for_MECL = pd.concat([costsheet_for_MECL, df2], ignore_index = True)
costsheet_for_MECL.reset_index()


# In[90]:


import pandas as pd
string = str(latest_file)
file = 'MECL'+ '-' + string[4:12]
writer = pd.ExcelWriter('COSTSHEET/' + file + '.' + 'xlsx') 

costsheet_for_MECL.to_excel(writer, sheet_name='costsheet_for_MECL', index=False, na_rep='NaN',startrow=2)

# Auto-adjust columns' width
for column in costsheet_for_MECL:
    column_width = max(costsheet_for_MECL[column].astype(str).map(len).max(), len(column))
    col_idx = costsheet_for_MECL.columns.get_loc(column)
    writer.sheets['costsheet_for_MECL'].set_column(col_idx, col_idx, column_width)
workbook  = writer.book
worksheet = writer.sheets['costsheet_for_MECL']
worksheet.write(0, 0, 'MECL', workbook.add_format({'bold': True, 'color': '#000000', 'size': 14}))


writer.save()


# In[ ]:




