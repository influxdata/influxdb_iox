-- Test for predicate push down explains
-- IOX_SETUP: OneMeasurementThreeChunksWithDuplicates

-- Plan with order by
explain verbose select time, state, city, min_temp, max_temp, area from h2o order by time, state, city;


-- plan without order by
explain verbose select time, state, city, min_temp, max_temp, area from h2o;

-- Union plan
EXPLAIN VERBOSE select state as name from h2o UNION ALL select city as name from h2o;
