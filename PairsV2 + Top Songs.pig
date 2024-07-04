-- Cargar el dataset (cambiar por data.csv cuando esté listo).
spotify_data = LOAD 'hdfs://cm:9000/uhadoop2024/projects/group-27/rdata.csv' USING PigStorage(',') AS (user_id:chararray, artistname:chararray, trackname:chararray, playlistname:chararray);

-- Generar pares de artistas por cada playlist de cada usuario.
artist_pairs = FOREACH spotify_data GENERATE user_id, artistname, playlistname;

-- Filtrar duplicados de artistas en la misma playlist del mismo usuario.
unique_artist_pairs = DISTINCT artist_pairs;

-- Generar combinaciones de pares de artistas.
paired_artists = FOREACH (GROUP unique_artist_pairs BY (user_id, playlistname)) {
    pairs = CROSS unique_artist_pairs.artistname, unique_artist_pairs.artistname;
    paired_artists = FILTER pairs BY $0 < $1;  -- Filtrar pares con el mismo artista y evitar duplicados (a, b) y (b, a)
    GENERATE FLATTEN(paired_artists) AS (artist1, artist2);
}

-- Filtrar los pares que contienen el artista "Starred".
filtered_paired_artists = FILTER paired_artists BY artist1 != 'Starred' AND artist2 != 'Starred';

-- Agrupar por pares de artistas y contar las ocurrencias.
artist_pair_counts = FOREACH (GROUP filtered_paired_artists BY (artist1, artist2)) {
    GENERATE
        FLATTEN(group) AS (artist1, artist2),
        COUNT(filtered_paired_artists) AS pair_count;
}

-- Filtrar los pares que se repiten más de una vez.
filtered_artist_pairs = FILTER artist_pair_counts BY pair_count > 1;

-- Ordenar los resultados en forma descendente.
sorted_artist_pairs = ORDER filtered_artist_pairs BY pair_count DESC;

-- Concatenar artistname y trackname.
song_counts = FOREACH spotify_data GENERATE user_id, CONCAT(artistname, '##', trackname) AS song;

-- Filtrar duplicados de canciones en la misma playlist del mismo usuario.
unique_songs = DISTINCT song_counts;

-- Agrupar por canciones y contar las ocurrencias.
song_play_counts = FOREACH (GROUP unique_songs BY song) {
    GENERATE
        group AS song,
        COUNT(unique_songs) AS play_count;
}

-- Ordenar los resultados por cantidad de reproducciones.
sorted_song_play_counts = ORDER song_play_counts BY play_count DESC;

-- Guardar resultados.
STORE sorted_artist_pairs INTO 'hdfs://cm:9000/uhadoop2024/projects/group-27/xd';
STORE sorted_song_play_counts INTO 'hdfs://cm:9000/uhadoop2024/projects/group-27/song_count';