# 380,000+ Lyrics from MetroLyrics

*[Link](https://www.kaggle.com/gyani95/380000-lyrics-from-metrolyrics)

*Description:  
This dataset contains information for over 380,000 songs. Information includes song name, year of release, artist name, genre and most importantly, lyrics. The dataset is organized through the  artist/year/song structure. Genre information is indicated by a text file inside each musician folder. Therefore the implied drawback for this dataset is that when an artist performs different music in different genres, such variety will not be shown. 

*Questions of Interest:  
	1. What are the most frequent words in each genre?
	2. What is the repetitiveness within songs for each genre? 
	3. Are there any strong genre indicating words/phrases?
	4. Is it possible to predict genre based on the lyrics? If so, how good is the accuracy?

*Reasons for the Interest:  
I work on music genre classification for senior thesis. However we only take the audio files instead of the lyrics into account when analyzing songs. Part of the reason is that I believe when humans determine the genre of a song, we mainly rely on the melody, rhythm, timbre and other musical features rather than lyrics. Lyrics, although has its own limitations, can also aid the genre identification. I hope to find out lyrics' potential in this task.

---

# Anime Recommendations Database

.*[Link](https://www.kaggle.com/CooperUnion/anime-recommendations-database)

.*Description:
This database contains information about animes from [myanimelist](myanimelist.net). The dataset is devided into two parts: animes and ratings. The anime section contains the myanimelist id, name, genre, type(movie, TV, OVA, etc.), episodes, rating and members(number of community members that are in this anime's group). The rating section contains user id, anime id and a rating(on a scale of 10).

.*Questions of Interest:
..1. What are the polarities for the top rated animes?
..2. Can we develop an algorithem to recommend animes for users based on this database, especially the rating section?

.*Reasons for the Interest:
So far music softwares have made a head start in the user recommendation field and I have always been amazed by their progress. However there is not enough research on the movie/tv show/anime criteria, which is an equaly, if not more, important part of people's entertainment life. I assume that the recommendation algorithms for music are done by linking or grouping users based on their listening history. For animes, the equvalent of listening history could be the ratings. With a large dataset of user ratings along with other information on animes, I hope the goal of recommendation could be achieved (to some extent). 

---

# GTZAN Genre Collection

.*[Link](http://marsyasweb.appspot.com/download/data_sets/)

.* This is a dataset of 1,000 music clips from 10 genres, each containing 100 tracks. The dataset is named after its author George Tzanetakis. It is one of the most commonly used dataset for music genre classification researches. The genres include blue, classical, country, disco, hiphop, jazz, metal, pop, reggae and rock. Each clip is of 30 seconds long and is usually the middle of a song. The clips are from a variety of sources: CDs, radio, microphone recordings, in order to provide a variety of recording conditions.

.*Questions of Interest: 
..1. How can we predict genre using machine learning based on the audio file alone (actually my thesis topic)? 

.*Reasons for the Interest:
Currently it seems like humans have no needs for auto genre classification because the genre information usually comes with the track, and is defined by the artist. However there are many limitations to the current system. First of all, not all music comes with a genre tag, especially not the indie ones. There is also a problem with inaccurate tagging, which could be cause by blurred genre distinctions or over-generalizing tag based on the artist alone. More often than not, machine learning researches are targeted at making programs achieve near-human level intelligence and I believe in that sense, being able to tell a genre based on the audio is just as important as to recognize objects in a picture. 