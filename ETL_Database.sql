-- Create movies table
CREATE TABLE IF NOT EXISTS dbo.movies
(
    id int NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 )
    ,name varchar(40) NOT NULL
    ,description varchar(500) NOT NULL
    ,category varchar(40) NOT NULL
    ,CONSTRAINT movies_pkey PRIMARY KEY (id)
)
;

-- Insert data into movies table
INSERT INTO dbo.movies
VALUES
('Avatar','Avatar is a 2009 American epic science fiction film.','Sci-Fi')
,('Avengeres: Infinity War','Avengers: Infinity War is a 2018 American superhero film based on MCU','Sci-Fi')
,('Holidate','Holidate is a 2020 American romantic comedy holiday film','Romcom')
,('Extraction','Extraction is a 2020 American action-thriller film starring Chris Hemsworth','Action')	
,('Johm Wick','John Wick is a 2014 American neo-noir action film','Action')
;

-- Create users table
CREATE TABLE IF NOT EXISTS dbo.users
(
    id int NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 )
    ,movie_id int NOT NULL
    ,rating int NOT NULL
    ,CONSTRAINT users_pkey PRIMARY KEY (id)
    ,CONSTRAINT fk_movie 
		FOREIGN KEY (movie_id)
        	REFERENCES dbo.movies (id) MATCH SIMPLE
       			ON UPDATE NO ACTION
        		ON DELETE NO ACTION
)
;

-- Inser data into users table
INSERT INTO dbo.users
VALUES
(1,4)
,(2,5)
,(1,4)
,(3,3)
,(4,5)






