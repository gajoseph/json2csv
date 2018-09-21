## json Data to rdbms rows 
This program convert a json data into a panda dataframe and writes to a csv file;  With little modifications; the program should be able to reurn as dataframe and then will have options to write what fields into a csv file

1) Listed below are mutiple usecases on how the program converts from a simple json to complex mutiple json files 
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
 
  <tr><TD> <a href = "https://github.com/gajoseph/json2csv/blob/master/sample/Jsonsample4.txt">JSON data with mutiple complex nested array </a> </TD>
    <TD><img src="https://github.com/gajoseph/json2csv/blob/master/sample/jsonsample4.jpg" width="700"/> </TD>
    </tr>
    

  <tr><TD> <a href = "https://github.com/gajoseph/json2csv/blob/master/sample/Jsonsample5.txt">JSON data with mutiple simple nested array </a> </TD>
    <TD><img src="https://github.com/gajoseph/json2csv/blob/master/sample/jsonsample5.jpg" width="700"/> </TD>
    </tr>


   <tr><TD> <a href = "https://github.com/gajoseph/json2csv/blob/master/sample/Jsonsample6.txt">JSON data with mutiple simple nested types </a> </TD>
    <TD><img src="https://github.com/gajoseph/json2csv/blob/master/sample/jsonsample6.jpg" /> </TD>
    </tr>
   
    
 
 
 <Table>

###Description
Program uses pandas to parse the json data; then flattens data into dataframes; each json object flattened and stored into another pandas dataframe and then merged(using pandas merge and rmeoving common columns )/ concatenated(via using pandas concat) -- if this object is an  jason array object; this is done recursively from bottom level to the top level. Finally there will be one pandas dataframe and is writen to cvs file.  
