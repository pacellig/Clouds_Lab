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

A = LOAD '/laboratory/airlines/{2005,2006,2007,2008}.csv' USING PigStorage(',') AS 
	(year: int, month: int, day: int, dow: int, 
	deptime: int, sdeptime: int, arrtime: int, sarrtime: int, 
	carrier: chararray, fn: int, tn: chararray, 
	aetime: int, setime: int, airtime: int, 
	arrdelay: int, depdelay: int, 
	ocode: chararray, dcode: chararray, dist: int, 
	tintime: int, touttime: int, 
	cancelled: chararray, ccode: chararray, diverted: int, 
	cdelay: int, wdelay: int, ndelay: int, secdelay: int, latedelay: int);

B = FOREACH A GENERATE year, carrier;
C = GROUP B BY (year, carrier);
D = FOREACH C GENERATE FLATTEN(group), LOG10(COUNT(B));
STORE D INTO './AIRLINE2YEAR';

