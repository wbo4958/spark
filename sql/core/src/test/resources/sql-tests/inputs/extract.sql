CREATE TEMPORARY VIEW t AS select '2011-05-06 07:08:09.1234567' as c, interval 10 year 20 month 30 day 40 hour 50 minute 6.7890 second as i;

select extract(millennium from c) from t;
select extract(millennia from c) from t;
select extract(mil from c) from t;
select extract(mils from c) from t;

select extract(century from c) from t;
select extract(centuries from c) from t;
select extract(c from c) from t;
select extract(cent from c)from t;

select extract(decade from c) from t;
select extract(decades from c) from t;
select extract(dec from c) from t;
select extract(decs from c) from t;

select extract(year from c), extract(year from i) from t;
select extract(y from c), extract(y from i) from t;
select extract(years from c), extract(years from i) from t;
select extract(yr from c), extract(yr from i) from t;
select extract(yrs from c), extract(yrs from i) from t;

select extract(isoyear from c) from t;

select extract(quarter from c) from t;
select extract(qtr from c) from t;

select extract(month from c), extract(month from i) from t;
select extract(mon from c), extract(mon from i) from t;
select extract(mons from c), extract(mons from i) from t;
select extract(months from c), extract(months from i) from t;

select extract(week from c) from t;
select extract(w from c) from t;
select extract(weeks from c) from t;

select extract(day from c), extract(day from i) from t;
select extract(d from c), extract(d from i) from t;
select extract(days from c), extract(days from i) from t;

select extract(dayofweek from c) from t;

select extract(dow from c) from t;

select extract(isodow from c) from t;

select extract(doy from c) from t;

select extract(hour from c), extract(hour from i) from t;
select extract(h from c), extract(h from i) from t;
select extract(hours from c), extract(hours from i) from t;
select extract(hr from c), extract(hr from i) from t;
select extract(hrs from c), extract(hrs from i) from t;

select extract(minute from c), extract(minute from i) from t;
select extract(m from c), extract(m from i) from t;
select extract(min from c), extract(min from i) from t;
select extract(mins from c), extract(mins from i) from t;
select extract(minutes from c), extract(minutes from i) from t;

select extract(second from c), extract(second from i) from t;
select extract(s from c), extract(s from i) from t;
select extract(sec from c), extract(sec from i) from t;
select extract(seconds from c), extract(seconds from i) from t;
select extract(secs from c), extract(secs from i) from t;

select extract(milliseconds from c) from t;
select extract(msec from c) from t;
select extract(msecs from c) from t;
select extract(millisecon from c) from t;
select extract(mseconds from c) from t;
select extract(ms from c) from t;

select extract(microseconds from c) from t;
select extract(usec from c) from t;
select extract(usecs from c) from t;
select extract(useconds from c) from t;
select extract(microsecon from c) from t;
select extract(us from c) from t;

select extract(epoch from c) from t;

select extract(not_supported from c) from t;
select extract(not_supported from i) from t;
