# Gaia TAP+ Potential Server-side bugs

1. Uploading user table requires 'TASKID' parameter although its value means nothing and this is not noted in the Gaia help page otherwise it throws 500 internal server error.

```sh
❯ curl -k -c cookies.txt -X POST -d username="user" -d password="password" https://gea.esac.esa.int/tap-server/login
OK

❯ curl -k -b cookies.txt -X POST -F FILE=@mytable.xml -F TABLE_NAME="test-foo-bar123" -F FORMAT="votable" "https://gea.esac.esa.int/tap-server/Upload"

<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<style type="text/css">
body { background-color: white; color: black; }
h2 { font-weight: bold; font-variant: small-caps; text-decoration: underline; font-size: 1.5em; color: #4A4A4A; }
ul, ol { margin-left: 2em; margin-top: 0.2em; text-align: justify; }
li { margin-bottom: 0.2em; margin-top: 0; }
p, p.listheader { text-align: justify; text-indent: 2%; margin-top: 0; }
table { border-collapse: collapse; }
table, th, td { border: 1px solid #909090; }
th { background-color: #909090; color: white; font-size: 1.1em; padding: 3px 5px 3px 5px; }
tr.alt1 { background-color: white; }
tr.alt2 { background-color: #F0F0F0; }
td { padding: 2px 5px 2px 5px; }
</style>
<title>SERVICE ERROR</title>
</head>
<body>
<h1 style="text-align: center; background-color:red; color: white; font-weight: bold;">SERVICE ERROR - 500</h1>
<h2>Description</h2>
<ul>
<li><b>Action: </b>Upload service problem</li>
<li><b>Context: </b>Upload</li>
<li><b>Exception: </b>esavo.tap.TAPException</li>
<li><b>Message: </b>java.lang.NullPointerException</li>
</ul>
...

❯ curl -k -b cookies.txt -X POST -F FILE=@mytable.xml -F TASKID="-1" -F TABLE_NAME="table_name" "https://gea.esac.esa.int/tap-server/Upload"

Uploaded file (size: 0) into 'table_name'

❯ cat mytable.xml
<?xml version="1.0" encoding="utf-8"?>
<!-- Produced with astropy.io.votable version 3.0.4
     http://www.astropy.org/ -->
<VOTABLE version="1.3" xmlns="http://www.ivoa.net/xml/VOTable/v1.3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.ivoa.net/xml/VOTable/v1.3">
 <RESOURCE type="results">
  <TABLE>
   <FIELD ID="a" datatype="long" name="a"/>
   <FIELD ID="b" datatype="long" name="b"/>
   <DATA>
    <TABLEDATA>
     <TR>
      <TD>1</TD>
      <TD>4</TD>
     </TR>
     <TR>
      <TD>2</TD>
      <TD>5</TD>
     </TR>
     <TR>
      <TD>3</TD>
      <TD>6</TD>
     </TR>
    </TABLEDATA>
   </DATA>
  </TABLE>
 </RESOURCE>
</VOTABLE>
```

2. Requesting FITS output from queries returns nothing
```sh
❯ curl "http://gea.esac.esa.int/tap-server/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=fits&QUERY=SELECT+TOP+5+source_id,ra,dec+FROM+gaiadr1.gaia_source"

❯ curl "http://gea.esac.esa.int/tap-server/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY=SELECT+TOP+5+source_id,ra,dec+FROM+gaiadr1.gaia_source"
source_id,ra,dec
4149502805337861888,265.79135417010923,-13.319638698403157
4149383439569766912,265.66682848055933,-14.213040254971228
4149324306478966784,264.6318294715637,-14.336423786340458
4149406975996196736,266.165059957401,-13.898858025195493
4149548435035876608,267.6775504751368,-13.97193049777155
```

2019-02-12 It looks like that both issues are ninja patched.

3. Synchronous queries can silently timeout and return nothing. I see this as a bug but oh well.