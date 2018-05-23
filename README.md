# PKChunkGenerator
Generates chunks of a Salesforce primary key range for exporting large data volumes in smaller pieces, by doing arithmetic on the base-62 numbers of the Salesforce IDs.

usage: java -jar PKChunkGenerator.jar [-h] [-v] [-pf <prefix>] [-st] [-on <objectname>] [-fn <fieldnamelist>] [-si <SalesforceID>] [-ei <SalesforceID>] [-of
       <filename>] [-cs <chunksize>] [-gt] [-b10 <base10number> | -a <base62ID,base10NumberToAdd> | -d <ID1,ID2> | -g | -b62 <base62id>]
Do interesting stuff with Salesforce base62 IDs

 -h,--help                               print help message
 -v,--verbose                            spit out more detail in output
 -pf,--prefix <prefix>                   add a prefix to converted base 10 number
 -st,--strict                            strict calculations, i.e. throw error e.g. when prefixes don't match or distance would be negative
 -on,--objectname <objectname>           Required for -g operation. Name of the object to generate chunk queries for, e.g. Asset or Product2, etc.
 -fn,--fieldnames <fieldnamelist>        Optional for -g operation. Commaseparated names of the fields to extract from the object to generate chunk queries for,
                                         e.g. Id,Name,LastModifiedBy,My_Custom_Field__c, etc. No spaces please. Will default to just the ID field if not
                                         provided.
 -si,--startid <SalesforceID>            Required for -g operation. Starting ID for the range to generate chunk queries for,
 -ei,--endid <SalesforceID>              Required for -g operation. Ending ID for the range to generate chunk queries for,
 -of,--outputfile <filename>             Optional for -g operation. Name of the file to generate. Defaults to output.txt if not provided.
 -cs,--chunksize <chunksize>             Optional for -g operation. Size of chunks to generate. Defaults to 10 chunks for less than 100M records or the required
                                         number of 10M record chunks if not provided.
 -gt,--greaterthan                       Optional for -g operation.
                                         If provided the generated queries will always be ID > startID AND ID =< endID
                                         If not provided, queries will be ID >= startID AND ID =< endID\n
 -b10,--convertbase10 <base10number>     convert a base10 id to a base62 number
 -a,--add <base62ID,base10NumberToAdd>   add a base10 number to a base62 ID, get the resulting base62 ID
 -d,--distance <ID1,ID2>                 calculate a base10 distance between two Salesforce IDs
 -g,--generatechunks                     generate queries for chunks of a primary key range. Requires object name (-on), start and end IDs (-si, -ei), and
                                         optional query range start (-gt) and field names (fn) parameters.
                                         If chunksize parameter (-cs) is not provided, will generate 10 chunks for the entire range, as long as the chunks are
                                         less than 10M records each, or the required number of 10M record chunks.
                                         If field names parameter is not provided, will default to the ID field
 -b62,--convertbase62 <base62id>         convert a base62 id to a base10 number

Repo at https://github.com/kgalant/PKChunkGenerator

###Example usage: 

####What is the base10 number corresponding to the ID?

kgalant$ java -jar PKChunkGenerator.jar -b62 02i0k00000GTheO -v
Discarded prefix: 02i0k0 Base10 number: 243500684

####Another example, this time not verbose

kgalant$ java -jar PKChunkGenerator.jar -b62 02i0k00000GWDjn
244100683

####So if we want the record number 244.000.000, what is its ID?

Kims-MacBook-Pro:PKChunkGenerator kgalant$ java -jar PKChunkGenerator.jar -b10 244000000
0000GVnXs

####Wait, that's not an ID, I need the Salesforce ID, so we'll just give it the prefix

Kims-MacBook-Pro:PKChunkGenerator kgalant$ java -jar PKChunkGenerator.jar -b10 244000000 -prefix 02i0k0
02i0k00000GVnXs

####So what's the distance between those two records (i.e. how many records are there between the two, assuming nothing has been deleted)

kgalant$ java -jar PKChunkGenerator.jar -d 02i0k00000GWDjn,02i0k00000GTheO
599999

####OK, want more than 600K. Give me the ID 2M records after the starting one

kgalant$ java -jar PKChunkGenerator.jar -a 02i0k00000GTheO,2000000 -v
Starting point: 02i0k00000GTheO Distance requested: 2000000 End ID: 02i0k00000Gc5wS 

####OK, now give me the queries to extract 60M Asset records in 5M chunks from salesforce. I need the ID, Name and Someotherfield__c fields

#####What is the ID of the record 60M records after the starting one?

kgalant$ java -jar PKChunkGenerator.jar -a 02i0k00000GTheO,60000000
02i0k00000KXSOK

#####Now use that to generate queries to fire into Salesforce

kgalant$ java -jar PKChunkGenerator.jar -g -si 02i0k00000GTheO -ei 02i0k00000KXSOK -on Asset -of myfile.txt -fn Id,Name,SomeOtherField__c -cs 5000000
SELECT Id,Name,SomeOtherField__c FROM Asset WHERE Id >= '02i0k00000GTheO' AND Id <= '02i0k00000GogNX' ORDER BY Id asc limit 5000000
SELECT Id,Name,SomeOtherField__c FROM Asset WHERE Id >= '02i0k00000GogNY' AND Id <= '02i0k00000H9f6h' ORDER BY Id asc limit 5000000
SELECT Id,Name,SomeOtherField__c FROM Asset WHERE Id >= '02i0k00000H9f6i' AND Id <= '02i0k00000HUdpr' ORDER BY Id asc limit 5000000
SELECT Id,Name,SomeOtherField__c FROM Asset WHERE Id >= '02i0k00000HUdps' AND Id <= '02i0k00000HpcZ1' ORDER BY Id asc limit 5000000
SELECT Id,Name,SomeOtherField__c FROM Asset WHERE Id >= '02i0k00000HpcZ2' AND Id <= '02i0k00000IAbIB' ORDER BY Id asc limit 5000000
SELECT Id,Name,SomeOtherField__c FROM Asset WHERE Id >= '02i0k00000IAbIC' AND Id <= '02i0k00000IVa1L' ORDER BY Id asc limit 5000000
SELECT Id,Name,SomeOtherField__c FROM Asset WHERE Id >= '02i0k00000IVa1M' AND Id <= '02i0k00000IqYkV' ORDER BY Id asc limit 5000000
SELECT Id,Name,SomeOtherField__c FROM Asset WHERE Id >= '02i0k00000IqYkW' AND Id <= '02i0k00000JBXTf' ORDER BY Id asc limit 5000000
SELECT Id,Name,SomeOtherField__c FROM Asset WHERE Id >= '02i0k00000JBXTg' AND Id <= '02i0k00000JWWCp' ORDER BY Id asc limit 5000000
SELECT Id,Name,SomeOtherField__c FROM Asset WHERE Id >= '02i0k00000JWWCq' AND Id <= '02i0k00000JrUvz' ORDER BY Id asc limit 5000000
SELECT Id,Name,SomeOtherField__c FROM Asset WHERE Id >= '02i0k00000JrUw0' AND Id <= '02i0k00000KCTf9' ORDER BY Id asc limit 5000000
SELECT Id,Name,SomeOtherField__c FROM Asset WHERE Id >= '02i0k00000KCTfA' AND Id <= '02i0k00000KXSOJ' ORDER BY Id asc limit 5000000
