-- others like select version();
select JSON_UNQUOTE(JSON_EXTRACT(stats, '$[0]')) * 0 ver, (JSON_UNQUOTE(JSON_EXTRACT(stats, '$[1]')) + JSON_UNQUOTE(JSON_EXTRACT(stats, '$[2]')) + JSON_UNQUOTE(JSON_EXTRACT(stats, '$[3]')) + JSON_UNQUOTE(JSON_EXTRACT(stats, '$[4]'))) * 0 as val from system.statement_info order by request_at desc limit 1;
