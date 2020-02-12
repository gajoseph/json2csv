import json
import pandas as pd
import numpy as np
import math
import traceback
from pandas.io.json import json_normalize
from pandas.api.types import is_string_dtype
import os
import datetime
import inspect
import numpy

from inspect import currentframe, getframeinfo

cf = currentframe()
filename = getframeinfo(cf).filename

bigDf = pd.DataFrame({'A' : [1]});
fileDir = os.path.dirname(__file__)
filename = os.path.join(fileDir, '/PycharmProjects/jsonrows/json2csv/sample/JsonsampleALL.txt')
#with  open("c:/users/tgaj2/aws/qwe.txt") as f:
with  open(filename) as f:
    json_data = json.load(f )
    #json_data = json.loads(json.dumps(json_data["payLoad"] ))
    #json_data = pd.read_json(f)
#print (json_data )

def drop_dups(bigDf2):
    columnlist_2_add = list = ['0FK', '1PK']
    if '0FK' not in bigDf2.columns:
        columnlist_2_add.remove('0FK')
    if '1PK' not in bigDf2.columns:
        columnlist_2_add.remove('1PK')
    return bigDf2.drop_duplicates(subset=columnlist_2_add )

def dropDataframeCol(df, colname):
    if colname in df.columns:
        df.drop([colname], axis=1, inplace=True)

def merge_and_remove_columns(bigDf2, child):
#    if bigDf2['0FK'']==bigDf2['0FK'']
    ## GEt list of columns common!!! in both; rename them with pk values aka parent name
    com_cols = bigDf2.columns & child.columns
    print(bigDf2.columns, " <><><>, ", child.columns)
    print(bigDf2)
    print(child, com_cols)
    # for col in com_cols:
    #     print ("Col", col)
    #     if col not in ('0FK', '1PK'):
    #         print("Renaming bigDf2  ", col, " To ", str(bigDf2['1PK'].unique()[-1]) + col)
    #         bigDf2.rename(columns={col: str(bigDf2['1PK'].unique()[-1]) + col}, inplace=True)    ### rename column name to {PK_colu,name}_colname
    #         print("Renaming " , col , " To " , str(child['1PK'].unique()[-1])+"_" + col)
    #         child.rename(columns={col: str(child['1PK'].unique()[-1]) +"_" + col}, inplace=True)      ### rename column name to {PK_colm}_colname
    #

    print(bigDf2.columns, " AFTER <><><>, ", child.columns)


    print(bigDf2)
    print(child)

    bigDf2 = pd.merge(left=bigDf2, right=child, left_on='1PK', right_on='0FK', how='inner')
    print(bigDf2, "SDASSSS")
    bigDf2.rename(columns={'0FK_x': '0FK', '0FK_y': '1PK'}, inplace=True)  ### was PK_y


    if not '1PK' in bigDf2.columns:
        bigDf2.rename(columns={'1PK_x': '1PK'}, inplace=True)  ### was PK_y

    dropDataframeCol(bigDf2, '1PK_x')
    dropDataframeCol(bigDf2, '1PK_y')

    return bigDf2;




###################################################################################################
def pad_lefSideparent(bigDfCp, bigDf2):
    ### copy columns from bigf2 from PK to the end
    ### then
    bigDfChldCp =  bigDf2.copy()

    if '1PK' in bigDfChldCp.columns:
        columnlist_2_add = list(set(bigDfChldCp.columns) - set(['0FK']))
    else:
        columnlist_2_add = list(bigDfChldCp.columns)

    bigPar_padChldCols = pd.concat([bigDfCp, pd.DataFrame(columns=columnlist_2_add)], axis=0)

    return bigPar_padChldCols
####################################################################################################
def arrayNameAndRenameColumns(df):
    # if there are 2 array to be mereged this can be used
    #
    #
    colname = df.columns[df.columns.get_loc("1PK") + 1]
    new_colname = ""
    for col in df.columns:
        if (col != colname):
            if col.find(colname)>=0:
                new_colname=                 col.replace(colname,"")
                df.rename(columns={col: new_colname}, inplace=True)  ### was PK_y


#####################################################################################################
def assignVal(bigDf1, col, value ):
    printMoreinfo()
    print ("bigDf1, col, value ", bigDf1, "\n : " , col, " : ", value , " <<")
    if (bigDf1.empty):
        bigDf1 = pd.DataFrame({col: [value]})
    else:
        bigDf1[col] = value
        if 'A' in bigDf1.columns:
            bigDf1 = bigDf1.drop(['A'], axis = 1, inplace=True)

def build_rows(bigDf, child, run_leftpad):
    bigDfCp = bigDf.copy()
    bigDf2 = bigDfCp.copy()
    #bigDf2 = bigDf2.drop_duplicates(subset=['0FK', '1PK'])  ### remove the dups baasedon the columns
    bigDf2 = drop_dups(bigDf )

    if  ( ( (child['1PK'].dtype==int) or (child['1PK'].dtype==np.int64 ) or (type(child['1PK'].unique()[-1])==int ) ) and  (list(child['1PK'].unique())[-1] >= 1)): ## handling array
        #bigDf2 = bigDf2.iloc[:, 0:2].copy()
        bigDf2 = bigDf2.iloc[:, 0:bigDf2.columns.get_loc("1PK") + 1].copy()
    #### merging array at the same level

    if (child['0FK'].dtype == int or child['0FK'].dtype == np.int64) \
            and (bigDf['1PK'].dtype == int or bigDf['1PK'].dtype == np.int64) \
            and ((bigDf['1PK'].unique()[-1]) == (child['0FK'].unique()[-1]))\
            and (bigDf.shape[0] > 1):  ## handling array
        bigDf2 = bigDf2.iloc[:, 0:bigDf2.columns.get_loc("1PK")+1].copy()
    bigDf2 = merge_and_remove_columns(bigDf2,child)  ## merge the data frames and remove colums comun on both side and have onl

    ### 1st array element
    if (((child['1PK'].dtype == int) or (child['1PK'].dtype == np.int64) or (
        type(child['1PK'].unique()[-1]) == int)) and (list(child['1PK'].unique())[-1] ==0)):  ## handling array
        return bigDf2
    ### other array element. this is to avoid mutiple concat  for debug remove tis
    if (((child['1PK'].dtype == int) or (child['1PK'].dtype == np.int64) or (
                type(child['1PK'].unique()[-1]) == int)) and (list(child['1PK'].unique())[-1] > 0)):  ## handling array
        return pd.concat([bigDfCp, bigDf2], axis=0, sort=False )
    ### forgot???
    if (((child['1PK'].dtype == int) or (child['1PK'].dtype == np.int64) or (
        type(child['1PK'].unique()[-1]) == int)) ):  ## handling array
        return bigDf2
    if (child['0FK'].dtype == int  or child['0FK'].dtype == np.int64 ) \
            and  (bigDf['1PK'].dtype == int or bigDf['1PK'].dtype == np.int64 ) \
            and ((bigDf['1PK'].unique()[-1]) == (child['0FK'].unique()[-1]) )  :  ## handling array
        if (bigDf.shape[0] > 1):  ## handling merging of 2 sibling arrays
            #find the array name and rename col names
            #bigDfCp = arrayNameAndRenameColumns(bigDfCp)
            #bigDf2 = arrayNameAndRenameColumns(bigDf2)
            return pd.concat([bigDfCp, bigDf2], axis=0) ## for user case 1

        return bigDf2



    if (run_leftpad==1):
        bigDfCp = pad_lefSideparent(bigDfCp, bigDf2)

#0n 08/21 list(bigDfCp['0FK'].unique())[-1]
    if '0FK' in bigDfCp.columns:
       # if ( bigDfCp.columns in bigDf2.columns):
       #     print ("Same  <><><<><><<<><<><<<><><<<<<<<<<<<><><><><><<<><<<<<<><><><><><<><<><<><><<><<><><<><><><<><<><><><><><><<><<><<><<")
       ### This for concating when we process withina a parent aan array item and then later has a simple item
        if  (list(bigDfCp['0FK'].unique())[-1]==list(bigDf2['0FK'].unique())[-1] and list(bigDfCp['1PK'].unique())[-1]==list(bigDf2['1PK'].unique())[-1]):
            ### check if we actually removed some dup rows b/c of array collections; and another simple types
            if (bigDf.shape[0] != bigDf2.shape[0]):
                con_bigDf = pd.concat([bigDfCp, bigDf2], axis=0)
            else:
                con_bigDf = bigDf2

        else:
            con_bigDf = merge_and_remove_columns(bigDfCp, bigDf2)
            return bigDf2
    else:
        con_bigDf = bigDf2
    return con_bigDf

###########################################################################################
def build_leftSideParent(bigDf, child):
    if (child.shape[1] ==2 ): # if there are only 2 column -- 0FK and 1PK
        return bigDf
    return build_rows(bigDf, child, 0)
###################################################################################################


def addRowsforArrays(bigDf, child ):
    return build_rows(bigDf, child,1)

def printMoreinfo():
    nd = numpy.array(inspect.stack())
    print("sssss", nd[:, [2, 3]])  ### printing the 2nd and 3rd elements of mutli-dime array
    return

def merge_new(bigDf, child, parentcolumn , colpos):
    if (child.empty):
        return bigDf
    if 'A' in bigDf.columns:
        return child
    print("==================== bigDf========================" )
    print (bigDf)
    print("==================== Child ========================"  )
    print(child)
    printMoreinfo()

    if (type(parentcolumn)==str):
        for col in child.columns:
            if col not in ('0FK', '1PK'):
                child.rename(columns={col: str(parentcolumn) + "_" + col}, inplace=True)



    print ("child.columns ", child.columns, " parentcolumn ", parentcolumn )
    # pd.merge(left=surveySub,right=speciesSub
    if ((child['1PK'].dtype==int) or child['1PK'].dtype==np.int64 ) and ( (bigDf['1PK'].dtype==int) or bigDf['1PK'].dtype==np.int64): #if there rows are same need to mergeg
        ## create a new rows
        child['0FK'] = bigDf['0FK']
        #new_bigDf = pd.concat([bigDf, child], axis=1)
        new_bigDf = bigDf.append(child,ignore_index=True)
    else:
        if (child['0FK'].isin(bigDf['1PK']).unique()[0]==False):
            new_bigDf=  addRowsforArrays(bigDf, child)
            return
        else: ## PK and FK are matching do a EQ join
            #### for arrays
            if  ( (child['1PK'].dtype==int) or (child['1PK'].dtype==np.int64 ) ) and (list(child['1PK'].unique())[-1] >=1):
                return addRowsforArrays(bigDf, child)

            return build_leftSideParent(bigDf, child)

    return new_bigDf

def FK_NAMEWhenColumnisIntAndcolumn_nameisEmpty(column,column_name, index ):
    print(" else type(column) : is ", type(column), ' Colun: ', column, " FK_NAME ")

    FK_NAME= ""
    if (type(column) == np.int):  # or type(index)==np.int64:
        if (column_name == ""):
            FK_NAME = str(index)
        else:
            FK_NAME = str(column_name)
    else:
        if (type(index) != np.int64):
            if (column_name == ""):
                FK_NAME = str(column) + "_" + str(index)
            else:
                FK_NAME = str(column_name)
    print("type(column)", type(column), "type(index)", type(index), "  FK_NAME ", FK_NAME, " type(column)==np.int64",
          type(column) == np.int64)

    return FK_NAME



def flatten_data(data, i, column_name, parentcolumn, colpos, bigDf1 ):
    b = "";
    a= data
    print('Entering flatten_data ', i, type(a),"a=" ,a, " column_name  :", column_name)
    icnt=0
    icolcnt=0
    FK_NAME= column_name
##
    coltype= True
    icnt = i ##### talking the level from previous call
    icolcnt= colpos

    for index, row in a.iterrows():
        print ("Index: ", index , " Row: ", row )
        for column in a.columns:
            coltype = False
            if type(row[column]) == dict or type(row[column]) == list:
                print (" CHEKED D D D D ", "column =", column , list(a)[a.columns.get_loc(column)],"\n row:>>" ,row, "<< index ", index, " \n column type "
                       , type(column)                      , "\n column_name :",column_name                       , "\n level::", i , " ParentColumn", parentcolumn )

                if (type(column)==int) or type(index)==np.int64:
                    FK_NAME = str(column_name)
                else:
                    FK_NAME = str(column_name) #+ "_"  + str(index)

                if (bigDf1.empty):
                    dd = flatten_data(pd.DataFrame({index: json.loads(json.dumps(row[column]))}), icnt, FK_NAME,
                                  0, icolcnt, pd.DataFrame({'1PK' : index , '0FK' : [column]} ))  # pass the
                else:
                    dd = flatten_data(pd.DataFrame({index: json.loads(json.dumps(row[column]))}), icnt, FK_NAME,
                             bigDf1.columns.get_loc(bigDf1.columns[-1]), icolcnt
                                  ,pd.DataFrame({'1PK' : index, '0FK' : [column]} ))  # pass the
                #here we need to slice and dice to get  the correct rows

                print (column,  " ASSSS ", dd.columns)

                bigDf1 = merge_new(bigDf1, dd, column, colpos)
                print("==================== new ========================")
                print(bigDf1)

                icolcnt = icolcnt + 1

            else:

                FK_NAME = FK_NAMEWhenColumnisIntAndcolumn_nameisEmpty(column, column_name, index)

                if type(index)==np.int64 :#or type(column)==np.int64:
                    if (index==0): ### checking for array
                        if (index == 0):
                            FK_NAME =  str(column_name)
                        if (column == 0):
                            FK_NAME = str(column_name) #+ "_"+  index  ## add colunm_name

                        assignVal(bigDf1, FK_NAME, row[column])

                        print (" comibiend ", bigDf)
                        ### reset the level bigDf = pd.DataFrame({'A' : [1]});

                    else:
                        bigDf11 = pd.DataFrame(bigDf1[-1:] ) ## copy the last row
                        print ("bigDf11 ::", bigDf11 )
                  #      print("get_loc::", column, " Column_name = ", column_name,  index , "\n list of column bigDf:: " , bigDf.columns)
                        bigDf11.iloc[-1, bigDf1.columns.get_loc(FK_NAME)] = row[column] #### assign the new value to last column
                        bigDf1 = bigDf1.append(bigDf11)

                else:
                    FK_NAME = FK_NAMEWhenColumnisIntAndcolumn_nameisEmpty(column, column_name, index)

                    assignVal(bigDf1, FK_NAME, row[column])

            #icolcnt = icolcnt + 1
        icnt= icnt +icolcnt

        #icolcnt= 0

    if (bigDf1.empty):
        return pd.DataFrame({})
    else:
        return bigDf1



index="ALL"
bigDf = pd.DataFrame({'1PK' : [index]});
#b = flatten_data({index: json_data}, 0)
column_name = ""
start = datetime.datetime.now()

bigDf = flatten_data(pd.DataFrame({index: json.loads(json.dumps(json_data))}), 0, "", "", 0,bigDf )

end = datetime.datetime.now()



print ("dfdd",bigDf )
a = bigDf
#print ( "a = ", a.columns)
clear = lambda: os.system('cls') #on Windows System
clear()
print("\033[H\033[J")
a.to_csv('c:/users/tgaj2/aws/out.csv')  # read from paramter file
print (" start:" ,  start ,  " end " ,   end )
exit()


for index, row in a.iterrows():
    for column in a.columns:
        print(column ," : ", row[column] )
    print ("=========================================================================")

