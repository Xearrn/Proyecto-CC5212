data = LOAD 'hdfs://cm:9000/uhadoop2024/projects/group-27/rdata.csv' USING PigStorage(',') AS (user_id:chararray, artist:chararray);
--data = limit data 10000;
artist_data = FOREACH data GENERATE artist;

--Permite que no haya repeticiones de artista dentro de un mismo user_id
user_artists = DISTINCT data;
artist_group = GROUP user_artists BY artist;
user_group = GROUP user_artists BY user_id;

--Esto calcula el top 10 de artistas más escuchados (Que mas gente los tiene en sus playlist).
artist_count = FOREACH artist_group GENERATE group AS artist, COUNT(user_artists) AS count;
ordered_artist_count = ORDER artist_count BY count DESC;
top_100_artists = LIMIT ordered_artist_count 100;
--dump top_10_artists;

--Pares
user_artists2 = DISTINCT user_artists;
paired_artists = JOIN user_artists BY user_id, user_artists2 BY user_id;
filtered_pairs = FILTER paired_artists BY user_artists::artist < user_artists2::artist;
grouped_pairs = GROUP filtered_pairs BY (user_artists::artist, user_artists2::artist);
pair_counts = FOREACH grouped_pairs GENERATE FLATTEN(group) AS (artist1, artist2), COUNT(filtered_pairs) AS count;
ordered_pairs= ORDER pair_counts BY count DESC;
top_100_pairs = limit ordered_pairs 100;
---



x = join top_100_pairs BY artist1, ordered_artist_count BY artist;
y = join x BY artist2, ordered_artist_count BY artist;
useful = FOREACH y GENERATE x::top_100_pairs::artist1 as artist1, 
                            x::top_100_pairs::artist2 as artist2,
                            x::top_100_pairs::count as pcount,
                            x::ordered_artist_count::count as artist1count,
                            ordered_artist_count::count as artist2count;
                            
prob =  FOREACH useful { 
    a_given_b = ((double)pcount / (double)artist2count)*100;
    b_given_a = ((double)pcount / (double)artist1count)*100;
    GENERATE artist1, artist2, pcount,
    a_given_b AS prob_artist1_given_artist2, 
    b_given_a AS prob_artist2_given_artist1;}

order1 = ORDER prob BY pcount DESC;

order2 = ORDER prob BY prob_artist1_given_artist2 DESC;

STORE order1 INTO 'hdfs://cm:9000/uhadoop2024/projects/group-27/top100probsbycount' USING PigStorage(',');
STORE top_100_artists INTO 'hdfs://cm:9000/uhadoop2024/projects/group-27/top100artists' USING PigStorage(',');
--STORE top_100_pairs INTO 'hdfs://cm:9000/uhadoop2024/projects/group-27/top100pairs' USING PigStorage(',');
STORE order2 INTO 'hdfs://cm:9000/uhadoop2024/projects/group-27/top100probbypercentage' USING PigStorage(',');

