dbase(clientView,[client,odetails,orders,parts,viewing]).

table(client,[cno,fname,lname,"telno",preftype,maxrent]).
client(CR76,John,Kay,'0207-774-5632',Flat,425).
client(CR56,Aline,Stewart,'0141-848-1825',Flat,350).
client(CR74,Mike,Richie,'01475-392178',House,750).
client(CR62,Mary,Tregear,'01224-196720',Flat,600).

table(odetails,[ono,pno,qty]).
odetails(1020,10506,1).
odetails(1020,10507,1).
odetails(1020,10508,2).
odetails(1020,10509,3).
odetails(1021,10601,4).
odetails(1022,10601,1).
odetails(1022,10701,1).
odetails(1023,10800,1).
odetails(1023,10900,1).

table(orders,[ono,cno,eno,"received","shipped"]).
orders(1020,1111,1000,'10-DEC-94','12-DEC-94').
orders(1021,1111,1000,'12-JAN-95','15-JAN-95').
orders(1022,2222,1001,'13-FEB-95','20-FEB-95').
orders(1023,3333,1000,'20-JUN-97','').

table(parts,[pno,"pname",qoh,price,olevel]).
parts(10506,'Land Before Time I',200,19.99,20).
parts(10507,'Land Before Time II',156,19.99,20).
parts(10508,'Land Before Time III',190,19.99,20).
parts(10509,'Land Before Time IV',60,19.99,20).
parts(10601,'Sleeping Beauty',300,24.99,20).
parts(10701,'When Harry Met Sally',120,19.99,30).
parts(10800,'Dirty Harry',140,14.99,30).
parts(10900,'Dr. Zhivago',100,24.99,30).

table(viewing,[cno,propertyno,"viewdate","comment"]).
viewing(CR56,PA14,'24-MAY-04','too small').
viewing(CR76,PG4,'20-APR-04','too remote').
viewing(CR56,PG4,'26-MAY-04','').
viewing(CR62,PA14,'14-MAY-04','no dining room').
viewing(CR56,PG36,'28-APR-04','').
