/*

1    Year   1987-2008
2    Month  1-12
3    DayofMonth 1-31
4    DayOfWeek  1 (Monday) - 7 (Sunday)

5    DepTime    actual departure time (local, hhmm)
6    CRSDepTime scheduled departure time (local, hhmm)
7    ArrTime    actual arrival time (local, hhmm)
8    CRSArrTime scheduled arrival time (local, hhmm)

9    UniqueCarrier  unique carrier code
10   FlightNum  flight number
11   TailNum    plane tail number

12   ActualElapsedTime  in minutes
13   CRSElapsedTime in minutes
14   AirTime    in minutes

15   ArrDelay   arrival delay, in minutes
16   DepDelay   departure delay, in minutes

17   Origin origin IATA airport code
18   Dest   destination IATA airport code
19   Distance   in miles

20   TaxiIn taxi in time, in minutes
21   TaxiOut    taxi out time in minutes

22   Cancelled  was the flight cancelled?
23   CancellationCode   reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
24   Diverted   1 = yes, 0 = no

25   CarrierDelay   in minutes
26   WeatherDelay   in minutes
27   NASDelay   in minutes
28   SecurityDelay  in minutes
29   LateAircraftDelay  in minutes

*/

A = LOAD '/laboratory/airlines/2005.csv' USING PigStorage(',') AS 
	(year: int, month: int, day: int, dow: int, 
	deptime: int, sdeptime: int, arrtime: int, sarrtime: int, 
	carrier: chararray, fn: int, tn: chararray, 
	aetime: int, setime: int, airtime: int, 
	arrdelay: int, depdelay: int, 
	ocode: chararray, dcode: chararray, dist: int, 
	tintime: int, touttime: int, 
	cancelled: chararray, ccode: chararray, diverted: int, 
	cdelay: int, wdelay: int, ndelay: int, secdelay: int, latedelay: int);

/* airports by output traffic  */
B = FOREACH A GENERATE ocode as o, month as m;
C = GROUP B BY (o,m);
D = FOREACH C GENERATE FLATTEN(group), COUNT(B);
E = ORDER D by $2 desc;
F = LIMIT E 20;
STORE F INTO './AIRLINE1MOUT';
/* airports by input traffic */
filtI = FOREACH A GENERATE dcode as d, month as m2;
groupI = GROUP filtI by (d,m2);
countI = FOREACH groupI GENERATE FLATTEN(group), COUNT(filtI);
orderI = ORDER countI by $2 desc;
limitI = LIMIT orderI 20;
STORE limitI INTO './AIRLINE1MIN';
/* total traffic */
joinT = JOIN D by $0, countI by $0; 
filtT = FILTER joinT BY $1==$4;
filt2T = FOREACH filtT GENERATE $0, $1, $2, $5;
--groupT = GROUP joinT by $0, $1;
countT = FOREACH filt2T generate $0, $1, $2+$3;
orderT = ORDER countT by $2 desc;
limitT = LIMIT orderT 20;
STORE limitT INTO './AIRLINE1MTOT';

