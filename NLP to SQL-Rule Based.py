#Function that helps with speech to text. listening starts and ends when spectre is said. 
def texts():
    check=0
    querys=''
    r=sr.Recognizer()
    with sr.Microphone() as source:
        query=''
        r.adjust_for_ambient_noise(source,duration=1)
        while True:
            audio=r.listen(source,0,)
            try:
                query=r.recognize_google(audio)
            except:
                pass
            if 'spectre' in query.lower():
                break

    while True:
        with sr.Microphone() as source:
            print("\n\nListening......")
            time.sleep(0.5)
            audio=r.listen(source,0,)
        try:
            query=r.recognize_google(audio)
        except:
            pass
        if 'spectre' in query.lower():
            return querys
        querys+=' '+query

#tokeninzation of the sentence(preprocessing)
def tokens(arr,selectw):
    tokenize = word_tokenize(arr)
    for x in tokenize:
        y=x.lower()
        if y not in stop and y not in string.punctuation:
            y=ps.stem(y)
            if y not in selectw:
                selectw.append(x)
    return selectw
#identifying the tables used using a dictonary
def conf_table(table_identifier,words,all_tables):
    confirm_table=[]
    for y in words:
        for x in range (0,len(table_identifier)):
            y=y.lower()
            y=ps.stem(y)
            try:
                if y in table_identifier[x] and all_tables[x] not in confirm_table:
                    confirm_table.append(all_tables[x])
            
            except:
                pass
    return confirm_table

#generates a condition for int type queries(where clause)
def d_type_int(deets,inp):

    more = ['greater','more']
    less=['less','lower']
    between=['range','between']
    nums=[]
    condition=''
    for x,y in deets:
                if y.lower()=='cd':
                    nums.append(x)
    for x in more:
        if x in inp:
            condition = '>= '+nums[0]
            return condition

    for x in less:
        if x in inp:
            condition = '<= '+nums[0]
            return condition

    for x in between:   
        if  x in inp and len(nums) == 2:
            condition = 'between '+nums[0]+' and '+nums[1]
            return condition

    condition = '= '+nums[0]
    return condition
    
    
            

#creates the condition(where clause) for date type statements
def d_type_date(deets,inp,data):
    time=[]
    for a in range (0,len(deets)):
         x,y=deets[a][0],deets[a][1]
         if y=='CD' and ':' in x:
              try:
               for z in [deets[a+1][0],deets[a-1][0]]:
                   if z.lower() == 'p.m' or z.lower() == 'p.m.':
                        c = x.split(':')
                        c[0]=int(c[0])+12
                        x=str(c[0])+':'+str(c[1])
                        break
              except:
                   pass

                        
              time.append(x)
    print(time)
    more = ['after','more','greater']
    less=['less','lower','before']
    between=['from','between']
    for x in more:
        if x in inp:
            condition = '>= "'+str(date.today())+' '+time[0]+'"'
            return condition

    for x in less:
        if x in inp:
            condition = '<= "'+str(date.today())+' '+time[0]+'"'
            return condition
    
    for x in between:
         if x in inp:
              condition = 'between "'+str(date.today())+' '+time[0]+'"'+' and "'+str(date.today())+' '+time[1]+'"'
              return condition
              
        
    condition = ' = "'+str(date.today())+' '+time[0]+'"'
    return condition


#helps identify the proper nouns(Named entities) in the sentence
def d_type_string(deets,inp):
    noun=[]
    for col,tag in deets:
        if tag == 'NNP' or tag == 'NNPS':
            noun.append(col)
    return noun

#create the sql statement for date type problems 
def stmt_date(condition,data):
     data=data[data.Table == 'time']
     data=data[data.type == 'date']
     stmt = 'select * from time where '
     for x in data.Column:
          stmt+=x+' '+condition
     return stmt
          
     
     
#create the sql statement for string type problems using the proper nouns
def stmt_varchar(nouns,data,confirm_table):
    stmt='select * from '
    data=data[data.type == 'varchar']
    data=data[data.Table.isin(confirm_table)]
    stmt+=list(data['Table'])[0]+' where '
    val=''
    for x in data.Column:
        for y in nouns:
            val+=x+' like "%'+y+'%"'
            val+=' or '

    stmt+=val
    return stmt

#create the sql statement for int type problems
def stmt_intss(condition,data,inp,confirm_table):
    stmt='select * from '
    data=data[data.type == 'int']
    data=data[data.Table.isin(confirm_table)]
    stmt+=list(data['Table'])[0]+' where '
    checker = stmt
    for a in data.Column:
        for b in inp:
            if ps.stem(b.lower()) in a.lower():
                stmt+=a+' '+condition+' or '
    if checker == stmt:
         for a in data.Column:
              stmt+=a+' '+condition+' or '
    
    return stmt[:-4]


#when multiple tables are accessed, this devides the sentence into array of sentences where each sentence accesses 0 or 1 table only
def divide(confirm_table,inp,table_identifier):
        st=''
        arr_stmts=[]
        dd=[]
        for x in inp:
            check=0
            for y in table_identifier:
                for z in y:
                    if ps.stem(x) in z:
                        check=1
            if check ==1:
                    dd.append(1)
                    arr_stmts.append(st)
                    st=x
            else:
                    dd.append(0)
                    st+=' '+x
        arr_stmts.append(st)
        return arr_stmts
        

import nltk                                             
import numpy as np 
import re                                  
import string                              

from nltk.corpus import stopwords
stop=set(stopwords.words('english'))         
from nltk.stem import PorterStemmer        
from nltk.tokenize import word_tokenize
ps = PorterStemmer()
import pandas as pd 
from datetime import date
import mysql.connector  
import pyaudio
import time
import speech_recognition as sr
global query
r=sr.Recognizer()

myconn = mysql.connector.connect(host = "localhost", user = "root" ,password = "Akshat@111")  
#starting the connection
exe=myconn.cursor()
db=exe.execute('use nlp;')
#using the database
db=exe.execute('show tables;')
all_tables=[]
for x in exe:
    all_tables.append(x[0])
#getting all tables in the database
all_tables.remove('keywords')
all_tables.remove('keyword')
#removing extra tables not needed
data=pd.DataFrame({'Column':[],'type':[],'Key':[],'Table':[]})
info=[]
#creating a dataframe that contains column name, data type, key, and table name
for x in all_tables:
    txt='desc '+x
    db=exe.execute(txt)
    for y in exe:
        try:
            df2=pd.DataFrame({'Column':[y[0]],'type':[y[1][:7]],'Key':[y[3]],'Table':[x]})
        except:
            df2=pd.DataFrame({'Column':[y[0]],'type':[y[1]],'Key':[y[3]],'Table':[x]})
        
        data=data.append(pd.DataFrame(df2),ignore_index=True)
df=data

#extracting the keywords(dictionary) from keywords table
txt='select keyword from keywords where corr_table = "'
iden=[]
for x in all_tables:
    lst=txt+x+'";'
    db=exe.execute(lst)
    ls=[]
    for y in exe:
        ls.append(y[0])
    iden.append(ls)
table_identifier=[]
for y in iden:
    temp=[]
    for x in y:

        x=x.lower()
        x=ps.stem(x)
        if x not in temp:
                temp.append(x)
    table_identifier.append(temp)

#input speach
inp=texts()
print(inp)
from nltk.corpus import stopwords
stop=set(stopwords.words('english'))
words=[]
#preprocessing the input text
words = tokens(inp,words)
inp=nltk.word_tokenize(inp)
try:
    inp.remove('is')
except:
    pass
#positional tagging the input text
deets=nltk.pos_tag(words)
#getting all tables involved
confirm_table = conf_table(table_identifier,words,all_tables)
ct=0
for col,tag in deets:
    if tag.lower()=='cd':
        ct+=1

dtype=''
stmt='select * from '
data=df
if len(confirm_table) == 0:
    #if no table is involved
    stmt = 'no table identified'
    
elif len(confirm_table) == 1:
    #identifying the type of statements we need to create, date, int or varchar.
    if ct != 0:
    
        if 'time' not in confirm_table:
            dtype='int'
            condition = d_type_int(deets,inp)
            stmt = stmt_intss(condition,data,inp,confirm_table)
            
            


        else:
            dtype='date'
            
            condition = d_type_date(deets,inp,data)
            
            stmt=stmt_date(condition,data)
            

    else:
        dtype='varchar'
        nouns = d_type_string(deets,inp)
        
        stmt=stmt_varchar(nouns,data,confirm_table)
        stmt=stmt[:-4]
        

else:
    join=''
    for x in confirm_table:
        #ncube contains all foreign keys so hardcoding that
        df1=data[data.Table == 'ncube']
        df2=data[data.Table == x]
        #identifying which columns is the primary and foreign key among the 2 tables
        for a in df1.Column:
            for b in df2.Column:
                if ps.stem(a.lower()) == ps.stem(b.lower()):
                    com_col='.'+a
                    #creating the join clause of the statement
                    join += ' join ' +x+' on ncube'+com_col+' = '+x+com_col 
    stmt+='ncube'+join
    array= divide(confirm_table,inp,table_identifier)
    data=data[data.Table.isin(confirm_table)]
    stmts=''
    for sent in array:
        table = conf_table(table_identifier,tokens(sent,[]),all_tables)
        if len(table) == 1:
            tags=nltk.pos_tag(nltk.word_tokenize(sent))
            stmtchar=''
            stmtint=''
            stmtdate=''
            for x,y in tags:
                if y == 'NNP' or y == 'NNPS':
                    
                    nouns = d_type_string(tags,nltk.word_tokenize(sent))
                    stmtchar=stmt_varchar(nouns,data,table)
                    stmtchar=stmtchar.split('where')[1]
                    stmtchar=stmtchar[:-4]
                    stmtchar+=' and'
                
                    
                    
                    
                elif y == 'CD' and 'time' not in table:
                    condition = d_type_int(deets,inp)
                    stmtint = stmt_intss(condition,data,inp,confirm_table)
                    stmtint=stmtint.split('where')[1]
                    stmtint+=' and'
                
                else:
                    condition = d_type_date(deets,inp,data)
                    
                    stmtdate = stmt_date(condition,data)
                    smtmdate=stmtdate.split('where')[1]
                    stmtdate+=' and'


            stmts+=stmtchar+stmtint+stmtdate



    stmt+=stmts
    stmt=stmt[:-4]
print(stmt)