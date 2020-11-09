--Setup database
DROP DATABASE IF EXISTS demo;
CREATE DATABASE demo;
\c demo;


DROP TABLE IF EXISTS public.result;

CREATE TABLE IF NOT EXISTS public.result (
     id                    BIGINT NOT NULL,
     calculated_value      DOUBLE PRECISION NOT NULL,
     write_side_offset     BIGINT NOT NULL,
     PRIMARY KEY(id)
);

INSERT INTO public.result VALUES(1, 0, 1);