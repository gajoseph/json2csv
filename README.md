## json Data to rdbms rows 
This program convert a json data into a panda dataframe and writes to a csv file;  With little modifications; the program should be able to reurn as dataframe and then will have options to write what fields into a csv file

1) use case when there are mutiple array elements in a json then it converts into mutiple rows; for example if there is a manager and employees and this data is stored in an json data the program will convert this is mutiple rows and column see example json file and out putfile 
<table>
  <tr><TD> JSON file </TD>
    <TD> Output file  </TD>
    </tr>
  <tr><TD>
<p align="left">
   <a href = "https://github.com/gajoseph/json2csv/blob/master/sample/Jsonsample1.txt">JSON with nested arrays</a>
</p>
  </TD>
<TD>
<p align="left">
  <img src="https://github.com/gajoseph/json2csv/blob/master/example1.j.jpg" width="700"/>
</p>
</TD>
  </TR>
  <tr><TD> <a href = "https://github.com/gajoseph/json2csv/blob/master/sample/Jsonsample2.txt">JSON data with nested array </a> </TD>
    <TD><img src="https://github.com/gajoseph/json2csv/blob/master/sample/jsonsample2.jpg" width="700"/> </TD>
    </tr>
 
 <tr><TD> <a href = "https://github.com/gajoseph/json2csv/blob/master/sample/Jsonsample3.txt">JSON 2 Simple objects  </a> </TD>
    <TD><img src="https://github.com/gajoseph/json2csv/blob/master/sample/jsonsample3.jpg" /> </TD>
    </tr>
 
  <tr><TD> <a href = "https://github.com/gajoseph/json2csv/blob/master/sample/Jsonsample4.txt">JSON data with nested array </a> </TD>
    <TD><img src="https://github.com/gajoseph/json2csv/blob/master/sample/jsonsample4.jpg" width="700"/> </TD>
    </tr>
  <tr><TD> <a href = "https://github.com/gajoseph/json2csv/blob/master/sample/Jsonsample5.txt">JSON data with nested array </a> </TD>
    <TD><img src="https://github.com/gajoseph/json2csv/blob/master/sample/jsonsample5.jpg" /> </TD>
    </tr>
 
 <Table>

###Description
Program uses pandas to parse the json data; then flattens data into dataframes; each json object flattened and stored into another pandas dataframe and then merged(using pandas merge and rmeoving common columns )/ concatenated(via using pandas concat) -- if this object is an  jason array object; this is done recursively from bottom level to the top level. Finally there will be one pandas dataframe and is writen to cvs file.  
